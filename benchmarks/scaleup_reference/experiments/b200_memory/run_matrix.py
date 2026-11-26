#!/usr/bin/env python3
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

"""Measure broadcast semi-join placement with one fixed engine in an ABBA matrix."""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
SUITE = HERE.parents[1]
sys.path.insert(0, str(SUITE / 'scripts'))
from common import load_config, read, sha, write
import b200_services as services


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True)
    parser.add_argument('--budget', default='default')
    parser.add_argument('--order', default='baseline,hint,hint,baseline')
    parser.add_argument('--reference-only', action='store_true')
    parser.add_argument('--queries', default='18,20')
    parser.add_argument('--label')
    parser.add_argument('--suite-audit', action='store_true')
    parser.add_argument('--prefix-reference', action='store_true')
    parser.add_argument('--query-timeout', type=int, default=200)
    parser.add_argument('--iterations', type=int, default=2)
    parser.add_argument('--gpus', default='0,1,2,3')
    parser.add_argument('--execution-profile', default='reference',
                        choices=['reference', 'partitions128'])
    args = parser.parse_args()
    work = Path(args.work).resolve()
    gpu_indices = [int(i) for i in args.gpus.split(',')]
    if len(set(gpu_indices)) != 4 or not set(gpu_indices) <= set(range(8)):
        raise ValueError('Exactly four distinct physical GPUs are required')
    queries = [int(q) for q in args.queries.split(',')]
    if not queries or len(set(queries)) != len(queries) or not set(queries) <= set(range(1,23)):
        raise ValueError('Expected distinct TPC-H query numbers')
    if args.label and (not args.label.replace('-', '').replace('_', '').isalnum()):
        raise ValueError('Label must be a simple directory name')
    state = {'status': 'running', 'score_eligible': False, 'arms': [],
             'classification': ('all-query opportunity audit' if args.suite_audit else 'SQL hint mechanism diagnostic'),
             'order': ['reference'] if args.reference_only else args.order.split(','), 'rmm_budget': args.budget, 'gpu_indices': gpu_indices,
             'cache_policy': f'No cache flush; {args.iterations} actions per query per fresh application',
             'source_revision': subprocess.check_output(
                 ['git', '-C', str(SUITE), 'rev-parse', 'HEAD'], text=True).strip()}
    state['execution_profile'] = args.execution_profile
    matrix_root = work / 'results' / (args.label or ('reference' if args.reference_only else args.budget))
    output = matrix_root / 'matrix.json'
    if output.exists():
        raise RuntimeError('Matrix output exists; inspect it before starting another run')
    write(output, state)
    os.environ['SCALEUP_HEAD'] = socket.gethostname()
    host = '10.34.1.4'
    if host not in subprocess.check_output(['hostname', '-I'], text=True).split():
        raise RuntimeError('Expected B200 data address is unavailable')
    os.environ['SPARK_LOCAL_IP'] = host
    os.environ['SPARK_LOCAL_HOSTNAME'] = host
    config = load_config('aws48x', work / 'evidence/deployment.json')
    artifacts = work / 'artifacts'
    engine = artifacts / 'cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar'
    if sha(engine) != 'c267dafd65ae85b558d8c8df03a94c0947cbe42b79a1e29ddeaa63bfcadb8cce':
        raise AssertionError('Frozen engine hash differs')
    config.update(engine=str(engine), engine_sha256=sha(engine),
                  tools=str(artifacts / 'benchmark-tools.jar'), queries=[18] if args.reference_only else queries,
                  work=str(work), platform='b200', gpu_indices=gpu_indices,
                  dataset='/raid/knataraj/datasets/tpch/sf10k_v2_float',
                  statistics_snapshot=str(work/'evidence/catalog-statistics.json'))
    for table in read(config['statistics_snapshot'])['tables']:
        if not any((Path(config['dataset']) / table).glob('*.parquet')):
            raise RuntimeError('Dataset table contains no Parquet files: ' + table)
    dataset_files = [p for table in read(config['statistics_snapshot'])['tables']
                     for p in (Path(config['dataset']) / table).glob('*.parquet')]
    config['dataset_contract'] = {'parquet_files': len(dataset_files),
                                  'parquet_bytes': sum(p.stat().st_size for p in dataset_files)}
    config['spark_conf']['spark.executor.instances']='4'
    if args.execution_profile == 'partitions128':
        config['spark_conf']['spark.sql.shuffle.partitions'] = '128'
    config['execution_profile'] = args.execution_profile
    config.update(suite_audit=args.suite_audit, prefix_reference=args.prefix_reference, iterations=args.iterations,
                  query_timeout_seconds=args.query_timeout)
    if args.suite_audit:
        config['view_overrides'] = {'15': {'lineitem': {
            'l_extendedprice': 'decimal(15,2)', 'l_discount': 'decimal(15,2)'}}}
    config['classification'] = state['classification']
    if args.budget != 'default':
        raise ValueError('This experiment uses the default RMM budget')
    deployment = config['deployment']
    native = str(work/'runtime/native') + ':' + str(Path(deployment['ucx_home'])/'lib')
    native += ':/usr/local/cuda-13.0/lib64:/usr/local/cuda-13.0/targets/x86_64-linux/lib'
    env = dict(os.environ, JAVA_HOME=deployment['java_home'],
               SPARK_HOME=deployment['spark_home'], PYSPARK_PYTHON='/usr/bin/python3',
               CUDA_VISIBLE_DEVICES='', LD_LIBRARY_PATH=native,
               UCX_MODULE_DIR=str(Path(deployment['ucx_home']) / 'lib/ucx'))
    cache = work / 'scratch/cuda-cache'
    cache.mkdir(parents=True, exist_ok=True)
    env['CUDA_CACHE_PATH'] = str(cache)
    env['PATH'] = deployment['java_home'] + '/bin:' + env['PATH']
    try:
        for index, arm in enumerate(state['order']):
            run = matrix_root / f'{args.label or args.budget}-{index}-{arm}'
            app = run / 'applications/suite'
            app.mkdir(parents=True)
            current = dict(config, probe_arm=arm)
            write(run / 'config.json', current)
            profile = dict(config['spark_conf'])
            scratch = Path(deployment['scratch_root']) / run.name
            for name in ['events', 'driver', 'java-tmp']:
                (scratch / name).mkdir(parents=True, exist_ok=True)
            classpath = str(engine) + ':' + config['tools']
            profile.update({'spark.master': f'spark://{host}:19077',
                'spark.cores.max': '32', 'spark.driver.host': host,
                'spark.driver.bindAddress': host, 'spark.ui.port': '0',
                'spark.eventLog.enabled': 'true', 'spark.eventLog.compress': 'false',
                'spark.eventLog.dir': 'file://' + str(scratch / 'events'),
                'spark.local.dir': str(scratch / 'driver'),
                'spark.driver.extraClassPath': classpath,
                'spark.executor.extraClassPath': classpath,
                'spark.executorEnv.PYSPARK_PYTHON': '/usr/bin/python3',
                'spark.executorEnv.SPARK_LOCAL_IP': host,
                'spark.executorEnv.SPARK_LOCAL_HOSTNAME': host,
                'spark.executorEnv.CUDA_CACHE_PATH': str(cache),
                'spark.executorEnv.LD_LIBRARY_PATH': native,
                'spark.executorEnv.UCX_MODULE_DIR': env['UCX_MODULE_DIR']})
            for component in ['driver', 'executor']:
                profile[f'spark.{component}.extraJavaOptions'] = '-Djava.io.tmpdir=' + str(scratch / 'java-tmp')
            write(app / 'profile.json', profile)
            write(run / 'profiles.json', {str(q): dict(profile,
                **config['query_overrides'].get(str(q), {})) for q in config['queries']})
            properties = app / 'spark-defaults.conf'
            properties.write_text(''.join(f'{k}={v}\n' for k, v in sorted(profile.items())))
            record = {'arm': arm, 'run': str(run), 'status': 'starting'}
            state['arms'].append(record)
            write(output, state)
            service_state = services.start(run, current, host, 19077, 19080, 'suite')
            driver_state = app / 'driver-service.json'
            command = [str(Path(deployment['spark_home']) / 'bin/spark-submit'),
                '--properties-file', str(properties), '--jars',
                'local:' + str(engine) + ',local:' + config['tools'],
                str(HERE / 'run_queries.py'), '--run', str(run), '--app', str(app),
                '--queries', ','.join(map(str,config['queries']))]
            if args.reference_only:
                command.append('--reference-only')
            write(app / 'command.json', {'argv': command})
            try:
                process, receipt = services.launch(command, app / 'driver.log', env)
                write(driver_state, [receipt])
                started = time.monotonic()
                next_size_check = started
                while process.poll() is None:
                    if time.monotonic() - started > 420 + len(config['queries']) * config['iterations'] * (args.query_timeout + 90):
                        raise TimeoutError('Focused arm exceeded its deadline')
                    if os.statvfs(scratch).f_bavail * os.statvfs(scratch).f_frsize < 256 * 2**30:
                        raise RuntimeError('NVMe free-space guard reached')
                    if time.monotonic() >= next_size_check:
                        usage = int(subprocess.check_output(
                            ['du', '-sx', '--block-size=1', str(work / 'scratch')], text=True).split()[0])
                        if usage > 32 * 2**30:
                            raise RuntimeError('Owned scratch directory exceeded 32 GiB')
                        next_size_check = time.monotonic() + 5
                    time.sleep(2)
                result = read(app / 'result.json')
                if process.returncode or result['status'] != 'completed':
                    raise RuntimeError('Arm failed: ' + str(result.get('error')))
                if len(result['samples']) != (1 if args.reference_only else len(config['queries']) * config['iterations']) or any(
                        s['validation']['status'] not in ['passed','reference-generated'] for s in result['samples']):
                    raise AssertionError('Arm lacks complete validated outputs')
                record.update(status='completed', samples=[{k: s[k] for k in
                    ['query', 'iteration', 'sql_seconds', 'reader_metrics',
                     'validation', 'semi_join_plan_lines']} for s in result['samples']])
                write(output, state)
            finally:
                services.stop(driver_state)
                services.stop(service_state)
        state['status'] = 'completed'
    except BaseException as error:
        state.update(status='failed', error=repr(error))
        raise
    finally:
        write(output, state)


if __name__ == '__main__':
    main()
