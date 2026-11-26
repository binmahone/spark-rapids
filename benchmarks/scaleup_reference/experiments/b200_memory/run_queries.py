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

"""Run fresh independent SQL actions with required statistics and optional result validation."""
import argparse
import hashlib
import re
import sys
from collections import Counter
import json
import math
import os
from pathlib import Path
import signal
import threading
import time

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from catalog_statistics import register_tables
from common import SUITE, TABLES, apply_query_settings, read, sha, write
from b200_validation import Validator
from suite_validation import SuiteValidator
from suite_variants import transform, check_semantics, selected_arm
from next_variants import planner_overrides


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
        result.append({'node': str(node.nodeName()), 'node_id': int(node.id()),
                       'output': str(node.output().toString()), 'metrics': values})
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
    profiles = read(run / 'profiles.json')
    profile = read(app / 'profile.json')
    queries = [int(v) for v in args.queries.split(',')]
    state = {'status': 'starting', 'samples': [], 'validation_enabled': config['validation'],
             'reference_only': args.reference_only, 'independent_cpu_reference': False,
             'operator_metric_scope': 'Main physical plan; event-plan SQL accumulator IDs include scalar subqueries',
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
        session_identity = int(spark._jvm.java.lang.System.identityHashCode(spark._jsparkSession))
        state.update(application_id=sc.applicationId, spark_session_identity=session_identity,
                     effective_conf=effective)
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
                Path(config['statistics_snapshot']))
        native = state['catalog_statistics']['tables']['lineitem']['schema']
        required_type = 'double'
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
            'org.apache.spark.sql.rapids.IndependentQueryBoundary$')
        boundary = boundary_class.getField('MODULE$').get(None)
        validator_class = SuiteValidator if config.get('suite_audit') else Validator
        validator = validator_class(config['work']) if config['validation'] else None
        for planned_query in ([] if config.get('suite_audit') else [18, 20]):
            sql_path = SUITE / 'inputs/aws48x/sql' / f'q{planned_query}.sql'
            original = sql_path.read_text()
            column = 'l_orderkey' if planned_query == 18 else 'p_partkey'
            hinted, count = re.subn(r'(\bselect\s+)(' + column + r'\b)',
                r'\1/*+ BROADCAST */ \2', original, count=1, flags=re.I)
            assert count == 1 and hinted.replace('/*+ BROADCAST */ ', '') == original
            probe = spark.sql(hinted)
            planned = str(probe._jdf.queryExecution().executedPlan().toString())
            (app / f'q{planned_query}-hint-preflight.txt').write_text(planned)
            if not any('BroadcastHashJoin' in line and 'LeftSemi' in line
                       for line in planned.splitlines()):
                raise AssertionError(f'Q{planned_query}: hint did not reach a broadcast semi join')
        if any(selected_arm(q, config['probe_arm']) in ('compact-supplier','compact-counted-state') for q in queries):
            from integer_domains import require_unchanged
            proof = require_unchanged(config['work'], config['dataset'], {'lineitem':['l_suppkey']})
            write(app/'integer-domain-proof.json', {'sha256':sha(Path(config['work'])/'evidence/integer-domains.json'),
                'lineitem':proof['tables']['lineitem']['columns']['l_suppkey']})
        if config.get('suite_audit') and any(selected_arm(q, config['probe_arm']) not in
                                            ('baseline','prefilter') for q in queries):
            checks = check_semantics(spark, SUITE, queries, config['probe_arm'])
            write(app / 'rewrite-counterexamples.json', checks)
        test = spark.range(10000).selectExpr('sum(id * id + 3) AS total')
        if test.collect()[0][0] != sum(i*i+3 for i in range(10000)):
            raise AssertionError('GPU smoke result differs')
        if 'Gpu' not in str(test._jdf.queryExecution().executedPlan().toString()):
            raise AssertionError('GPU smoke has no GPU operator')
        startup.cancel()
        state['status'] = 'running'
        write(app / 'result.json', state)
        for query in queries:
            query_arm = selected_arm(query, config['probe_arm'])
            mechanism_arm = selected_arm(query, 'combined') if query_arm == 'no-runtime-bloom' else query_arm
            query_profile = profiles[str(query)]
            prefilter_key = 'spark.rapids.sql.optimizer.aggregatePrefilter.enabled'
            planner_configuration = {prefilter_key: 'false' if query_arm ==
                'preaggregate-prefilter-reuse' else query_profile[prefilter_key]}
            for key in ('spark.rapids.sql.optimizer.joinReorder.enabled',
                        'spark.rapids.sql.optimizer.joinGraphReorder.enabled',
                        'spark.rapids.sql.driverlessBroadcast.enabled',
                        'spark.sql.optimizer.runtime.bloomFilter.enabled'):
                planner_configuration[key] = query_profile[key]
            planner_configuration.update(planner_overrides(query_arm))
            for key, value in planner_configuration.items():
                spark.conf.set(key, value)
                if spark.conf.get(key) != value:
                    raise AssertionError('Per-query planner setting differs')
            write(app/f'q{query}-planner-configuration.json',planner_configuration)
            scan_configuration = {}
            if not args.reference_only:
                scan_configuration = apply_query_settings(spark, query_profile)
            for table, casts in config['view_overrides'].get(str(query), {}).items():
                base = spark.table(table)
                base.select(*[base[f.name].cast(casts[f.name]).alias(f.name) if f.name in casts else
                              base[f.name] for f in base.schema.fields]).createOrReplaceTempView(table)
            sql_path = SUITE / 'inputs' / 'aws48x' / 'sql' / f'q{query}.sql'
            for iteration in range(1, (1 if args.reference_only else config['iterations']) + 1):
                token = f'q{query}-iteration{iteration}'
                sc.setJobGroup(token, token, True)
                if not args.reference_only:
                    boundary.begin(sc._jsc.sc(), token)
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
                    started_unix_ns = time.time_ns()
                    started = time.perf_counter()
                    original_sql = sql_path.read_text()
                    query_sql = original_sql
                    if args.reference_only:
                        query_sql, count = re.subn(r'limit\s+100\s*$', 'limit 10000', query_sql, flags=re.I)
                        assert count == 1 and query == 18
                    elif config.get('prefix_reference'):
                        query_sql,count = re.subn(r'limit\s+\d+\s*$', 'limit 1000', query_sql,flags=re.I)
                        assert count == 1 and query in (3,10)
                    elif config.get('suite_audit'):
                        query_sql = transform(query, original_sql, config['probe_arm'])
                    elif config['probe_arm'] == 'hint':
                        column = 'l_orderkey' if query == 18 else 'p_partkey'
                        query_sql, count = re.subn(r'(\bselect\s+)(' + column + r'\b)',
                            r'\1/*+ BROADCAST */ \2', original_sql, count=1, flags=re.I)
                        assert count == 1 and query_sql.replace('/*+ BROADCAST */ ', '') == original_sql
                    (app / (token + '.sql')).write_text(query_sql)
                    frame = spark.sql(query_sql)
                    execution = frame._jdf.queryExecution()
                    preflight = str(execution.executedPlan().toString())
                    (app / (token + '.preflight.txt')).write_text(preflight)
                    if query_arm == 'no-runtime-bloom' and 'bloom_filter_agg' in preflight:
                        raise AssertionError('Runtime Bloom aggregation is still present')
                    Path(str(app / token) + '.cost.txt').write_text(str(
                        spark._jvm.PythonSQLUtils.explainString(execution, 'cost')))
                    semi = [line for line in preflight.splitlines() if 'LeftSemi' in line]
                    if config['probe_arm'] == 'hint' and not any('BroadcastHashJoin' in line for line in semi):
                        raise AssertionError('Hint did not reach a broadcast semi join')
                    if config.get('suite_audit') and mechanism_arm in ('row-propagation','row-propagation-reuse'):
                        if not any('BroadcastHashJoin' in line and 'LeftSemi' in line for line in preflight.splitlines()):
                            raise AssertionError('Propagated semi join is absent or not broadcast')
                    if config.get('suite_audit') and mechanism_arm == 'row-propagation-reuse':
                        if 'ReusedExchange' not in preflight or preflight.count('GpuFileGpuScan parquet spark_catalog.default.part[')!=1:
                            raise AssertionError('Expected shared part broadcast with a single part scan')
                    if config.get('suite_audit') and mechanism_arm in ('dimension-broadcast','preaggregate-broadcast'):
                        if not any('BroadcastHashJoin' in line and 'Inner' in line for line in preflight.splitlines()):
                            raise AssertionError('Dimension broadcast hint was not executed')
                    if config.get('suite_audit') and mechanism_arm in ('preaggregate','preaggregate-broadcast','preaggregate-prefilter','preaggregate-prefilter-reuse') and not config.get('prefix_reference'):
                        group_key = 'l_orderkey' if query in (3,4,18) else 'o_custkey'
                        if not re.search(r'GpuHashAggregate \(keys=\[' + group_key + r'#\d+L\],', preflight):
                            raise AssertionError('Requested pre-join aggregate is absent')
                    if config.get('suite_audit') and mechanism_arm in ('preaggregate-prefilter','preaggregate-prefilter-reuse'):
                        if 'ReusedExchange' not in preflight or preflight.count('GpuFileGpuScan parquet spark_catalog.default.lineitem[')!=1:
                            raise AssertionError('Expected shared Q18 aggregate and a single lineitem scan')
                        if not any('LeftSemi' in line and 'BroadcastHashJoin' in line for line in preflight.splitlines()):
                            raise AssertionError('Expected early order-key broadcast filter')
                    if config.get('suite_audit') and mechanism_arm in ('aggregate-reuse','window-average'):
                        if not re.search(r'Gpu\w*Window', preflight) or preflight.count('GpuFileGpuScan parquet spark_catalog.default.lineitem[')!=1:
                            raise AssertionError('Expected one lineitem scan and a GPU window')
                    if config.get('suite_audit') and mechanism_arm == 'aggregate-total-reuse':
                        if not re.search(r'Gpu\w*Window', preflight) or preflight.count('GpuFileGpuScan parquet spark_catalog.default.partsupp[')!=1:
                            raise AssertionError('Expected one partsupp scan and a GPU window')
                    if query_arm in ('revenue-first','profit-first','country-flags','eligible-customer-join',
                                     'compact-supplier','counted-wide','compact-counted-state',
                                     'priority-flag','promo-flag-revenue','supplier-prefilter',
                                     'eligible-orders-broadcast','country-flags-ordered',
                                     'country-flags-ordered-driver','eligible-orders-distinct-dimension'):
                        pending, nodes = [execution.executedPlan()], []
                        while pending:
                            node = pending.pop()
                            nodes.append(node)
                            children = node.children()
                            pending.extend(children.apply(i) for i in range(children.size()))
                        outputs = [str(n.output().toString()) for n in nodes
                                   if str(n.nodeName())=='GpuColumnarExchange']
                        if query_arm in ('country-flags-ordered','country-flags-ordered-driver'):
                            if not any('BroadcastHashJoin' in x and 'o_custkey' in x and
                                       'c_custkey' in x for x in preflight.splitlines()):
                                raise AssertionError('Customer filter is absent before the order-key shuffle')
                            if not any('o_orderkey' in x and 'c_fr' in x and
                                       'o_custkey' not in x for x in outputs):
                                raise AssertionError('Filtered-order flags did not reach the shuffle')
                            native = any('GpuDriverlessBroadcastHashJoin' in x and
                                         'o_custkey' in x and 'c_custkey' in x
                                         for x in preflight.splitlines())
                            if native == query_arm.endswith('-driver') or (
                                    query_arm.endswith('-driver') and
                                    'GpuDriverlessBroadcastHashJoin' in preflight):
                                raise AssertionError('Declared broadcast implementation was not selected')
                        if query_arm in ('eligible-orders-broadcast','eligible-orders-distinct-dimension'):
                            if not any('BroadcastHashJoin' in x and 'l_orderkey' in x and
                                       'o_orderkey' in x for x in preflight.splitlines()):
                                raise AssertionError('Eligible orders did not reach a broadcast fact join')
                            if any('hashpartitioning' in str(n.toString()).splitlines()[0].lower() and
                                   'l_orderkey' in str(n.output().toString()) for n in nodes
                                   if str(n.nodeName())=='GpuColumnarExchange'):
                                raise AssertionError('Lineitem order keys are still shuffled')
                            if query_arm == 'eligible-orders-distinct-dimension' and not all(
                                    any('GpuDriverlessBroadcastHashJoin' in x and left in x and right in x
                                        for x in preflight.splitlines()) for left,right in
                                    (('l_orderkey','o_orderkey'),('o_custkey','c_custkey'))):
                                raise AssertionError('Distinct-dimension estimates still reject native customer/order builds')
                        if query_arm == 'priority-flag' and not any('__high' in x and
                                'o_orderkey' in x and 'o_orderpriority' not in x for x in outputs):
                            raise AssertionError('Priority flag did not reach the order-key exchange')
                        if query_arm == 'promo-flag-revenue':
                            if not any('__promo' in x and 'p_type' not in x for x in outputs):
                                raise AssertionError('Promo flag did not narrow the part exchange')
                            if not any('__revenue' in x and 'l_extendedprice' not in x
                                       and 'l_discount' not in x for x in outputs):
                                raise AssertionError('Revenue did not narrow the lineitem exchange')
                        if query_arm == 'supplier-prefilter':
                            if preflight.count('GpuFileGpuScan parquet spark_catalog.default.lineitem[')!=1:
                                raise AssertionError('Expected one lineitem scan')
                            if not any(str(n.nodeName())=='GpuHashAggregate' and
                                       re.search(r'BroadcastHashJoin \[l_suppkey#',str(n.toString()))
                                       for n in nodes):
                                raise AssertionError('Supplier filter did not reach the pre-aggregate path')
                        if query_arm == 'revenue-first' and not any('__line_revenue' in x and
                                'l_extendedprice' not in x and 'l_discount' not in x for x in outputs):
                            raise AssertionError('Revenue projection did not narrow shuffle payload')
                        if query_arm == 'profit-first':
                            if preflight.count('GpuFileGpuScan parquet spark_catalog.default.part[')!=1 or 'ReusedExchange' not in preflight:
                                raise AssertionError('Profit candidate lost the shared green-part build')
                            if not any('amount' in x and 'l_orderkey' in x and 'l_quantity' not in x
                                       and 'l_extendedprice' not in x for x in outputs):
                                raise AssertionError('Profit did not reach the order-key shuffle')
                        if query_arm == 'country-flags' and not any(('s_fr' in x or 'c_fr' in x)
                                and ('l_orderkey' in x or 'o_orderkey' in x) and 'n_name' not in x for x in outputs):
                            raise AssertionError('Country flags did not narrow the order-key shuffle')
                        if query_arm == 'eligible-customer-join':
                            if preflight.count('GpuFileGpuScan parquet spark_catalog.default.customer[')!=1:
                                raise AssertionError('Expected one customer scan')
                            if not any('BroadcastHashJoin' in x and 'Inner' in x and 'c_custkey' in x
                                       and 'o_custkey' in x for x in preflight.splitlines()):
                                raise AssertionError('Eligible-customer broadcast join is absent')
                        if query_arm in ('compact-supplier','counted-wide','compact-counted-state'):
                            compact = False
                            for node in nodes:
                                if str(node.nodeName())!='GpuHashAggregate':continue
                                for i in range(node.output().size()):
                                    attr=node.output().apply(i)
                                    if str(attr.name()).startswith(('__rapids_corragg_exists_min','__state_all_min')):
                                        expected = 'bigint' if query_arm=='counted-wide' else 'int'
                                        compact = str(attr.dataType().simpleString())==expected
                            if not compact or preflight.count('GpuFileGpuScan parquet spark_catalog.default.lineitem[')!=1:
                                raise AssertionError('Expected one lineitem scan and the declared integer state type')
                    rows = frame._jdf.collect()
                    elapsed = time.perf_counter() - started
                    ended_unix_ns = time.time_ns()
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
                    if config['platform'] in ('aws48x', 'b200') and (reader['hybridScanDeviceBytesRead'] != 0 or
                                                        reader['hybridScanNativeHostBytesRead'] <= 0):
                        raise AssertionError('Expected native host reads did not execute')
                write(str(prefix) + '.measurement.json', {'sql_seconds':elapsed,
                    'started_unix_ns':started_unix_ns,'ended_unix_ns':ended_unix_ns,
                    'reader_metrics':reader,'plan_metrics':node_metrics,
                    'schema':frame.schema.jsonValue()})
                schema = frame.schema.jsonValue()
                reference = str(Path(config['deployment']['scratch_root']) / run.name / 'q11-reference')
                validation_started = time.perf_counter()
                validation = {'status': 'not-validated'}
                if args.reference_only:
                    values = [[None if row.isNullAt(i) else str(row.get(i))
                               for i in range(len(schema['fields']))] for row in rows]
                    Validator.write_q18_reference(config['work'], schema, values)
                    validation = {'status': 'reference-generated', 'method': 'GPU baseline cutoff-complete prefix'}
                elif config.get('prefix_reference'):
                    validation = validator.write_prefix(query, rows, schema, sha(sql_path), str(prefix))
                elif validator:
                    digest = spark._jvm.com.nvidia.spark.rapids.benchmark.ResultDigest.compute(rows)
                    validation = validator.validate(query, rows, schema, sha(sql_path), digest,
                                                     spark._jvm, str(prefix))
                validation_seconds = time.perf_counter() - validation_started
                sample = {'query': query, 'iteration': iteration, 'sql_seconds': elapsed,
                          'started_unix_ns':started_unix_ns,'ended_unix_ns':ended_unix_ns,
                          'application_id': sc.applicationId,
                          'spark_session_identity': int(spark._jvm.java.lang.System.identityHashCode(
                              frame._jdf.sparkSession())),
                          'rows': len(rows), 'schema': schema,
                          'sql_sha256': hashlib.sha256(query_sql.encode()).hexdigest(),
                          'reference_sql_sha256': sha(sql_path), 'probe_arm': config['probe_arm'],
                          'query_arm': query_arm,
                          'transformation_arm': mechanism_arm,
                          'semi_join_plan_lines': semi,
                          'validation': validation, 'validation_seconds': validation_seconds,
                          'reader_metrics': reader, 'plan_metrics': node_metrics,
                          'scan_configuration': scan_configuration,
                          'planner_configuration': planner_configuration}
                rows = frame = execution = plan = None
                if not args.reference_only:
                    before = time.perf_counter()
                    receipts = [json.loads(str(row)) for row in boundary.finish(
                        sc._jsc.sc(), token, executors, slots)]
                    sample['cleanup_seconds'] = time.perf_counter() - before
                    sample['sql_plus_cleanup_seconds'] = elapsed + sample['cleanup_seconds']
                    write(str(prefix) + '.cleanup.json', receipts)
                state['samples'].append(sample)
                write(app / 'result.json', state)
                print(json.dumps({k: sample[k] for k in ('query', 'iteration', 'sql_seconds', 'validation')}), flush=True)
            for table in config['view_overrides'].get(str(query), {}):
                if not spark.catalog.dropTempView(table):
                    raise AssertionError('Temporary numeric view was not removed')
        state['status'] = 'completed'
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
