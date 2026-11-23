# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run Q9/Q3 with ordinary-broadcast cleanup controls and separate GC diagnostics."""
import argparse
from collections import Counter
import json
import math
import os
from pathlib import Path
import signal
import sys
import threading
import time

from pyspark.sql import SparkSession

from gateway_barrier import release_proxies
sys.path.insert(0, os.environ['SCALEUP_REFERENCE_SUITE'] + '/scripts')
from catalog_statistics import register_tables
from common import SUITE, TABLES, read, sha, write
from validation import Validator


def mechanism(sc, boundary, app, config, targets):
    """Explicit GC is confined to this diagnostic; natural comparisons never call it."""
    executors = int(config['spark_conf']['spark.executor.instances'])
    slots = min(int(config['spark_conf']['spark.executor.cores']),
                round(1 / float(config['spark_conf']['spark.task.resource.gpu.amount'])))
    counts = {r['executor']: {o['object_id'] for o in r['objects']} for r in targets}
    if not any(counts.values()):
        write(app / 'mechanism.json', {'status': 'no-ordinary-broadcast-residual',
                                      'targets': {k: sorted(v) for k, v in counts.items()}})
        return
    started = time.perf_counter()
    records = []

    def probe(label, action):
        rows = [json.loads(str(r)) for r in boundary.probe(
            sc._jsc.sc(), label, executors, slots, action)]
        write(app / (label + '.observation.json'), rows)
        records.append({'phase': label, 'time_ms': int(time.time() * 1000)})
        return rows

    rounds = config['experiment'].get('driver_gc_rounds', 1)
    driver_stages = []
    probe_index = 0
    for round_index in range(rounds):
        round_started = time.perf_counter()
        boundary.driverGc('driver-gc-after-q9-round-' + str(round_index + 1))
        deadline = time.monotonic() + 30
        while not boundary.driverGcCompleted():
            if time.monotonic() > deadline:
                raise TimeoutError('Driver GC completion event was not observed')
            time.sleep(.25)
        driver_cycle_seconds = time.perf_counter() - round_started
        deadline = time.monotonic() + 20
        while True:
            rows = probe('driver-blocks-' + str(probe_index), 'observe')
            probe_index += 1
            remaining = sum(o['host_block_present'] for r in rows for o in r['objects']
                            if o['object_id'] in counts[r['executor']])
            if remaining == 0 or time.monotonic() >= deadline:
                break
            time.sleep(1)
        driver_stages.append({'round': round_index + 1, 'host_blocks_remaining': remaining,
                              'gc_cycle_seconds': driver_cycle_seconds,
                              'through_block_observation_seconds': time.perf_counter() - round_started})
    probe('executor-gc', 'gc')
    deadline = time.monotonic() + 30
    index = 0
    while True:
        rows = probe('executor-gc-wait-' + str(index), 'wait')
        if all(r['gc_cycle_completed'] for r in rows):
            break
        if time.monotonic() >= deadline:
            raise TimeoutError('Executor GC completion event was not observed everywhere')
        index += 1
        time.sleep(.5)
    deadline = time.monotonic() + 15
    index = 0
    while True:
        rows = probe('executor-finalized-' + str(index), 'observe')
        unfinalized = sum(len(counts[r['executor']] - set(r['finalized_object_ids'])) for r in rows)
        if not unfinalized or time.monotonic() >= deadline:
            break
        index += 1
        time.sleep(1)
    write(app / 'mechanism.json', {'status': 'observed', 'targets': {k: sorted(v) for k, v in counts.items()},
          'driver_stage': driver_stages[-1], 'driver_stages': driver_stages,
          'driver_gc_rounds': rounds, 'unfinalized_target_objects': unfinalized,
          'seconds': time.perf_counter() - started, 'phases': records,
          'scope': 'Deliberate Driver then Executor System.gc requests, outside timed SQL'})


def metrics(plan):
    pending, seen, result = [plan], set(), []
    while pending:
        node = pending.pop()
        if int(node.id()) in seen:
            continue
        seen.add(int(node.id()))
        values, iterator = {}, node.metrics().iterator()
        while iterator.hasNext():
            pair = iterator.next()
            values[str(pair._1())] = int(pair._2().value())
        result.append({'node': str(node.nodeName()), 'metrics': values})
        children = node.children()
        pending.extend(children.apply(i) for i in range(children.size()))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', required=True)
    parser.add_argument('--app', required=True)
    parser.add_argument('--queries', required=True)
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    run, app = Path(args.run), Path(args.app)
    config = read(run / 'config.json')
    experiment = config['experiment']
    profiles = read(run / 'profiles.json')
    profile = read(app / 'profile.json')
    queries = [int(v) for v in args.queries.split(',')]
    state = {'status': 'starting', 'samples': [], 'validation_enabled': config['validation'],
             'reference_only': args.reference_only, 'independent_cpu_reference': False,
             'timing_scope': 'SQL planning through complete JVM collect; validation and cleanup excluded'}
    spark = None
    startup = threading.Timer(420, lambda: os.kill(os.getpid(), signal.SIGTERM))
    startup.daemon = True
    startup.start()
    try:
        spark = SparkSession.builder.appName('scaleup-reference-' + app.name).getOrCreate()
        sc = spark.sparkContext
        if spark.version != config['spark_version']:
            raise AssertionError('Spark version differs')
        effective = dict(sc.getConf().getAll())
        for key, value in profile.items():
            if key.endswith('extraJavaOptions'):
                if not all(t in effective.get(key, '').split() for t in value.split()):
                    raise AssertionError('JVM options differ: ' + key)
            elif effective.get(key) != str(value):
                raise AssertionError('Effective configuration differs: ' + key)
        state.update(application_id=sc.applicationId, effective_conf=effective)
        if sha(config['tools']) != config['tools_sha256']:
            raise AssertionError('Diagnostic tools hash differs')
        executors = int(profile['spark.executor.instances'])
        slots = min(int(profile['spark.executor.cores']),
                    math.floor(1 / float(profile['spark.task.resource.gpu.amount']) + 1e-9))
        deadline = time.monotonic() + 180
        while sc._jsc.sc().getExecutorMemoryStatus().size() != executors + 1:
            if time.monotonic() >= deadline:
                raise RuntimeError('Expected GPU executors did not register')
            time.sleep(1)
        def probe(iterator):
            from pyspark import BarrierTaskContext
            import socket
            context = BarrierTaskContext.get()
            yield {'host': socket.gethostname(), 'gpu': context.resources()['gpu'].addresses,
                   'uuid': os.environ.get('CUDA_VISIBLE_DEVICES')}
        assignments = sc.parallelize(range(executors * slots), executors * slots).barrier().mapPartitions(probe).collect()
        counts = Counter(row['uuid'] for row in assignments)
        if len(counts) != executors or set(counts.values()) != {slots}:
            raise AssertionError('GPU resource assignment differs')
        state['gpu_assignments'] = assignments
        state['catalog_statistics'] = register_tables(spark, config['dataset'],
                SUITE / 'inputs' / config['platform'] / 'catalog-statistics.json')
        native = state['catalog_statistics']['tables']['lineitem']['schema']
        required_type = 'double' if config['platform'] == 'nvl72' else 'decimal(15,2)'
        if any(f['type'] != required_type for f in native['fields']
               if f['name'] in ('l_extendedprice', 'l_discount')):
            raise AssertionError('Dataset numeric schema differs')
        engine = Path(config['engine'])
        loader = spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
        source = str(loader.loadClass('com.nvidia.spark.SQLPlugin').getProtectionDomain().getCodeSource().getLocation())
        if not source.endswith('/' + engine.name) or sha(engine) != config['engine_sha256']:
            raise AssertionError('Loaded engine identity differs')
        state['plugin_code_source'] = source
        boundary_class = spark._jvm.com.nvidia.spark.rapids.ShimReflectionUtils.loadClass(
            'org.apache.spark.sql.rapids.BroadcastGcBoundary$')
        boundary = boundary_class.getField('MODULE$').get(None)
        validator = Validator(config['platform']) if config['validation'] else None
        startup.cancel()
        state['status'] = 'running'
        state['experiment'] = experiment
        state['reference_score_eligible'] = False
        state['periodic_gc_interval'] = effective.get('spark.cleaner.periodicGC.interval',
                                                     '30min (Spark 3.5.3 default)')
        write(app / 'result.json', state)
        pair_started = time.perf_counter()
        for query in queries:
            query_profile = profiles[str(query)]
            if not args.reference_only:
                for key in ('spark.sql.files.maxPartitionBytes', 'spark.sql.files.maxPartitionNum'):
                    if key in query_profile:
                        spark.conf.set(key, query_profile[key])
                    else:
                        spark.conf.unset(key)
            for table, casts in config['view_overrides'].get(str(query), {}).items():
                base = spark.table(table)
                base.select(*[base[f.name].cast(casts[f.name]).alias(f.name) if f.name in casts else
                              base[f.name] for f in base.schema.fields]).createOrReplaceTempView(table)
            sql_path = SUITE / 'inputs' / config['platform'] / 'sql' / f'q{query}.sql'
            for iteration in range(1, (1 if args.reference_only else config['iterations']) + 1):
                token = f'q{query}-iteration{iteration}'
                sc.setJobGroup(token, token, True)
                if not args.reference_only:
                    boundary.drain(sc._jsc.sc())
                    boundary.mark('sql-start-' + token)
                expired = threading.Event()
                def cancel():
                    expired.set()
                    sc.cancelJobGroup(token)
                timer = threading.Timer(config['query_timeout_seconds'], cancel)
                hard = threading.Timer(config['query_timeout_seconds'] + 60,
                                       lambda: os.kill(os.getpid(), signal.SIGTERM))
                timer.daemon = hard.daemon = True
                timer.start()
                hard.start()
                try:
                    started = time.perf_counter()
                    frame = spark.sql(sql_path.read_text())
                    execution = frame._jdf.queryExecution()
                    rows = frame._jdf.collect()
                    elapsed = time.perf_counter() - started
                    if expired.is_set():
                        raise TimeoutError('Query exceeded its limit')
                finally:
                    timer.cancel()
                    hard.cancel()
                plan = execution.executedPlan()
                plan_text = str(plan.toString())
                if 'Gpu' not in plan_text:
                    raise AssertionError('No GPU operator in collected plan')
                prefix = app / token
                Path(str(prefix) + '.logical.txt').write_text(str(execution.optimizedPlan().toString()))
                Path(str(prefix) + '.executed.txt').write_text(plan_text)
                node_metrics = metrics(plan)
                reader = {key: sum(n['metrics'].get(key, 0) for n in node_metrics) for key in (
                    'hybridScanSplits', 'hybridScanDeviceBytesRead', 'hybridScanNativeHostBytesRead',
                    'hybridScanGdsFallbackFiles', 'hybridScanGpuBusyHostBytesRead')}
                if not args.reference_only:
                    if reader['hybridScanSplits'] <= 0 or reader['hybridScanGdsFallbackFiles'] != 0:
                        raise AssertionError('Expected Hybrid reader path did not execute')
                    if config['platform'] == 'nvl72' and reader['hybridScanDeviceBytesRead'] <= 0:
                        raise AssertionError('Expected GDS device reads did not execute')
                    if config['platform'] == 'aws48x' and (reader['hybridScanDeviceBytesRead'] != 0 or
                                                        reader['hybridScanNativeHostBytesRead'] <= 0):
                        raise AssertionError('Expected native host reads did not execute')
                schema = frame.schema.jsonValue()
                reference = str(Path(config['deployment']['scratch_root']) / run.name / 'q11-reference')
                validation_started = time.perf_counter()
                validation = {'status': 'not-validated'}
                if args.reference_only:
                    digest = str(spark._jvm.com.nvidia.spark.rapids.benchmark.FullResultReference.write(
                        rows, reference + '.bin'))
                    write(reference + '.json', {'sha256': digest, 'schema': schema,
                                               'sql_sha256': sha(sql_path), 'independent_cpu_reference': False})
                    validation = {'status': 'reference-generated'}
                elif validator:
                    digest = spark._jvm.com.nvidia.spark.rapids.benchmark.ResultDigest.compute(rows)
                    validation = validator.validate(query, rows, schema, sha(sql_path), digest,
                                                     spark._jvm, reference)
                validation_seconds = time.perf_counter() - validation_started
                sample = {'query': query, 'iteration': iteration, 'sql_seconds': elapsed,
                          'rows': len(rows), 'schema': schema, 'sql_sha256': sha(sql_path),
                          'validation': validation, 'validation_seconds': validation_seconds,
                          'reader_metrics': reader, 'plan_metrics': node_metrics,
                          'scan_configuration': {k: query_profile.get(k) for k in (
                              'spark.sql.files.maxPartitionBytes', 'spark.sql.files.maxPartitionNum')}}
                rows = frame = execution = plan = None
                if not args.reference_only:
                    before = time.perf_counter()
                    # Drop unreachable Python/Py4J proxy cycles equally in every comparison arm.
                    sample['python_proxy_cycles_collected'] = release_proxies(sc)
                    receipts = [json.loads(str(row)) for row in boundary.finish(
                        sc._jsc.sc(), token, executors, slots, experiment['manual_ordinary_cleanup'])]
                    sample['cleanup_seconds'] = time.perf_counter() - before
                    sample['sql_plus_cleanup_seconds'] = elapsed + sample['cleanup_seconds']
                    write(str(prefix) + '.cleanup.json', receipts)
                    if query == 9 and experiment['scenario'] == 'mechanism':
                        mechanism(sc, boundary, app, config, receipts)
                state['samples'].append(sample)
                write(app / 'result.json', state)
                print(json.dumps({k: sample[k] for k in ('query', 'iteration', 'sql_seconds', 'validation')}), flush=True)
            for table in config['view_overrides'].get(str(query), {}):
                if not spark.catalog.dropTempView(table):
                    raise AssertionError('Temporary numeric view was not removed')
        state['status'] = 'completed'
        state['pair_wall_seconds'] = time.perf_counter() - pair_started
        state['pair_wall_scope'] = 'Q9 SQL start through Q3 boundary end; includes validation, observations and deliberate GC if configured'
    except BaseException as error:
        state.update(status='failed', error=repr(error))
        raise
    finally:
        startup.cancel()
        write(app / 'result.json', state)
        if spark:
            spark.stop()


if __name__ == '__main__':
    main()
