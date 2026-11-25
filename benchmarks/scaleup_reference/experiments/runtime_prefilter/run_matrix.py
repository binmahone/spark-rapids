#!/usr/bin/env python3
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
import services


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True)
    parser.add_argument('--name', choices=['main', 'q20-isolated'], default='main')
    parser.add_argument('--queries', default='18,20')
    parser.add_argument('--order', default='baseline,hint,hint,baseline')
    args = parser.parse_args()
    work = Path(args.work).resolve()
    queries = [int(q) for q in args.queries.split(',')]
    order = args.order.split(',')
    if not queries or not set(queries) <= {18, 20} or not set(order) <= {'baseline', 'hint'}:
        raise ValueError('Unsupported focused matrix')
    result_root = work / ('results' if args.name == 'main' else 'results-q20-isolated')
    state = {'status': 'running', 'score_eligible': False, 'arms': [],
             'classification': 'SQL hint mechanism diagnostic; no engine or aggregate-reuse change',
             'order': order, 'queries': queries, 'name': args.name,
             'cache_policy': 'No cache flush; two actions per query per fresh application',
             'source_revision': subprocess.check_output(
                 ['git', '-C', str(SUITE), 'rev-parse', 'HEAD'], text=True).strip()}
    output = result_root / 'matrix.json'
    if output.exists():
        raise RuntimeError('Matrix output exists; inspect it before starting another run')
    write(output, state)
    os.environ['SCALEUP_HEAD'] = socket.gethostname()
    host = socket.gethostbyname(socket.gethostname())
    config = load_config('aws48x', work / 'evidence/deployment.json')
    artifacts = work / 'artifacts/main'
    engine = artifacts / 'cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar'
    if sha(engine) != 'c267dafd65ae85b558d8c8df03a94c0947cbe42b79a1e29ddeaa63bfcadb8cce':
        raise AssertionError('Frozen engine hash differs')
    config.update(engine=str(engine), engine_sha256=sha(engine),
                  tools=str(artifacts / 'benchmark-tools.jar'), queries=queries)
    deployment = config['deployment']
    native = str(Path(deployment['ucx_home']) / 'lib') + ':/usr/local/cuda-13.1/lib64'
    native += ':/usr/local/cuda-13.1/targets/x86_64-linux/lib'
    env = dict(os.environ, JAVA_HOME=deployment['java_home'],
               SPARK_HOME=deployment['spark_home'], PYSPARK_PYTHON='/usr/bin/python3',
               CUDA_VISIBLE_DEVICES='', LD_LIBRARY_PATH=native,
               UCX_MODULE_DIR=str(Path(deployment['ucx_home']) / 'lib/ucx'))
    env['PATH'] = deployment['java_home'] + '/bin:' + env['PATH']
    try:
        for index, arm in enumerate(state['order']):
            prefix = '' if args.name == 'main' else args.name + '-'
            run = result_root / f'{prefix}{index}-{arm}'
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
                'spark.cores.max': '64', 'spark.driver.host': host,
                'spark.driver.bindAddress': host, 'spark.ui.port': '0',
                'spark.eventLog.enabled': 'true', 'spark.eventLog.compress': 'false',
                'spark.eventLog.dir': 'file://' + str(scratch / 'events'),
                'spark.local.dir': str(scratch / 'driver'),
                'spark.driver.extraClassPath': classpath,
                'spark.executor.extraClassPath': classpath,
                'spark.executorEnv.PYSPARK_PYTHON': '/usr/bin/python3',
                'spark.executorEnv.LD_LIBRARY_PATH': native,
                'spark.executorEnv.UCX_MODULE_DIR': env['UCX_MODULE_DIR']})
            for component in ['driver', 'executor']:
                profile[f'spark.{component}.extraJavaOptions'] = '-Djava.io.tmpdir=' + str(scratch / 'java-tmp')
            write(app / 'profile.json', profile)
            write(run / 'profiles.json', {str(q): profile for q in queries})
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
                '--queries', ','.join(map(str, queries))]
            write(app / 'command.json', {'argv': command})
            try:
                process, receipt = services.launch(command, app / 'driver.log', env)
                write(driver_state, [receipt])
                started = time.monotonic()
                while process.poll() is None:
                    if time.monotonic() - started > 1500:
                        raise TimeoutError('Focused arm exceeded its deadline')
                    if os.statvfs(scratch).f_bavail * os.statvfs(scratch).f_frsize < 100 * 2**30:
                        raise RuntimeError('NVMe free-space guard reached')
                    time.sleep(2)
                result = read(app / 'result.json')
                if process.returncode or result['status'] != 'completed':
                    raise RuntimeError('Arm failed: ' + str(result.get('error')))
                if len(result['samples']) != 2 * len(queries) or any(
                        s['validation']['status'] != 'passed' for s in result['samples']):
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
