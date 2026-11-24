#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""Run Paul's GDS pytest selection with a strict GDS filesystem and owned JVM lifecycle."""
import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import xml.etree.ElementTree as ET

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
SUITE = HERE.parents[1]
sys.path.insert(0, str(SUITE / 'scripts'))
from common import read, sha, write
import services


def prepare(args):
    run = args.run.resolve()
    if subprocess.check_output(['git', '-C', str(REPO), 'status', '--porcelain'], text=True).strip():
        raise ValueError('Commit the test runner before submission')
    deployment = read(args.deployment)
    artifacts = REPO / '.build/artifacts/arm64'
    manifest = read(artifacts / 'manifest.json')
    for name, item in manifest['files'].items():
        if sha(artifacts / name) != item['sha256']:
            raise ValueError('Artifact changed: ' + name)
    run.mkdir(parents=True, exist_ok=False)
    write(run / 'config.json', {'deployment': deployment, 'artifacts': str(artifacts),
        'manifest': manifest, 'thread_id': os.environ.get('CODEX_THREAD_ID'),
        'paul_head': 'b3e85674bad145871c4a23da0c932fb28e4d80c4',
        'harness_revision': subprocess.check_output(['git', '-C', str(REPO), 'rev-parse', 'HEAD'], text=True).strip(),
        'selection': 'hybrid_scan_test.py -k gds'})
    write(run / 'status.json', {'status': 'prepared'})
    if args.submit:
        command = ['sbatch', '--parsable', '--export=ALL', '--job-name=paul-gds-tests',
            '--nodes=1', '--ntasks=1', '--cpus-per-task=16', '--gres=gpu:b200:1',
            '--mem=128G', '--time=00:20:00', '--no-requeue',
            '--partition=' + deployment.get('slurm_partition', 'nvidia-part'),
            '--nodelist=' + args.node,
            '--comment=codex-thread-' + os.environ.get('CODEX_THREAD_ID', 'manual'),
            '--output=' + str(run / 'slurm-%j.log'), str(HERE / 'scripts/gds_tests.slurm')]
        output = subprocess.check_output(command, env=dict(os.environ, PAUL_GDS_RUN=str(run),
            PAUL_GDS_SUITE=str(HERE)), text=True).strip()
        write(run / 'submission.json', {'job_id': int(output.split(';')[0]), 'argv': command})
        print('Submitted', output, run, flush=True)


def node(args):
    run = args.run.resolve()
    config = read(run / 'config.json')
    d = config['deployment']
    local = Path('/var/tmp') / ('paul-gds-tests-' + os.environ['SLURM_JOB_ID'])
    local.mkdir(parents=True, exist_ok=True)
    tmp = Path('/gds-tests') / run.name
    tmp.mkdir(mode=0o700, parents=True, exist_ok=False)
    mount = subprocess.check_output(['findmnt', '-T', str(tmp), '-o', 'TARGET,SOURCE,FSTYPE,OPTIONS'], text=True)
    if 'gpfs' not in mount:
        raise ValueError('The GDS test directory is not the qualified GPFS mount')
    artifacts = Path(config['artifacts'])
    engine = next(artifacts.glob('cudf-spark_*.jar'))
    env = dict(os.environ, SPARK_HOME=d['spark_home'], JAVA_HOME=d['java_home'],
        LOCAL_JAR_PATH=str(artifacts), TESTS='hybrid_scan_test.py', TEST_PARALLEL='1',
        INCLUDE_SPARK_AVRO_JAR='false', INCLUDE_SPARK_PROTOBUF_JAR='false', TZ='UTC',
        PYSP_TEST_spark_rapids_sql_format_parquet_hybridScan_enabled='true',
        PYSP_TEST_spark_rapids_sql_reader_gpuDirectStorage_mode='ON',
        PYSP_TEST_spark_rapids_sql_reader_kvikio_numThreads='32',
        PYSP_TEST_spark_executorEnv_CUFILE_ALLOW_COMPAT_MODE='false',
        CUFILE_ALLOW_COMPAT_MODE='false', CUFILE_STATS_LEVEL='0',
        SPARK_RAPIDS_TEST_DATAGEN_SEED='0', SPARK_RAPIDS_TEST_INJECT_OOM_SEED='0')
    for key in ('JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS', 'SPARK_SUBMIT_OPTS'):
        env.pop(key, None)
    env['PATH'] = d['java_home'] + '/bin:' + env['PATH']
    env['LD_LIBRARY_PATH'] = d['ucx_home'] + '/lib:/usr/local/cuda-13.1/targets/sbsa-linux/lib:/usr/local/cuda-13.0/targets/sbsa-linux/lib:/usr/local/nvidia/lib:/usr/local/nvidia/lib64'
    cufile = read(local / 'cufile.json')
    cufile['properties']['allow_compat_mode'] = False
    cufile['profile']['cufile_stats'] = 0
    cufile['logging']['dir'] = str(run / 'cufile-logs')
    (run / 'cufile-logs').mkdir()
    write(local / 'cufile.json', cufile)
    env['CUFILE_ENV_PATH_JSON'] = str(local / 'cufile.json')
    env['PYSP_TEST_spark_executorEnv_CUFILE_ENV_PATH_JSON'] = env['CUFILE_ENV_PATH_JSON']
    env['RUN_DIR'] = str(run / 'pytest')
    (run / 'pytest').mkdir()
    command = ['bash', str(REPO / 'integration_tests/run_pyspark_from_build.sh'),
        '-k', 'gds', '--tmp_path=' + str(tmp), '-rs', '--junitxml=' + str(run / 'pytest.xml')]
    write(run / 'preflight.json', {'host': socket.gethostname(), 'mount': mount,
        'cuda_visible_devices': env.get('CUDA_VISIBLE_DEVICES'), 'engine_sha256': sha(engine),
        'environment': {k: env[k] for k in env if k.startswith(('PYSP_TEST_', 'CUFILE_', 'SPARK_RAPIDS_TEST_'))}})
    write(run / 'command.json', {'argv': command})
    write(run / 'status.json', {'status': 'running', 'started_utc': datetime.now(timezone.utc).isoformat()})
    state = run / 'pytest-service.json'
    def terminate(signum, frame):
        raise SystemExit('Test run interrupted')
    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    process = None
    try:
        process, receipt = services.launch(command, run / 'pytest-console.log', env)
        write(state, [receipt])
        code = process.wait(timeout=900)
        if code:
            raise RuntimeError('GDS pytest process exited ' + str(code))
        report = ET.parse(run / 'pytest.xml').getroot()
        tests = list(report.iter('testcase'))
        failures = [t.attrib for t in tests if t.find('failure') is not None or t.find('error') is not None]
        skipped = [t.attrib for t in tests if t.find('skipped') is not None]
        if not tests or failures or skipped:
            raise AssertionError({'tests': len(tests), 'failures': failures, 'skipped': skipped})
        write(run / 'summary.json', {'status': 'completed', 'passed': len(tests),
            'cases': [t.attrib for t in tests], 'cpu_gpu_parity': True,
            'gds_required_by_launch_configuration': True})
        write(run / 'status.json', {'status': 'completed'})
    except BaseException as error:
        write(run / 'status.json', {'status': 'failed', 'error': repr(error)})
        raise
    finally:
        if state.exists():
            services.stop(state)
        if process:
            process.wait(timeout=20)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'node'))
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--deployment', type=Path)
    parser.add_argument('--node', default='presto-gb200-gcn-04')
    parser.add_argument('--submit', action='store_true')
    args = parser.parse_args()
    prepare(args) if args.action == 'prepare' else node(args)
