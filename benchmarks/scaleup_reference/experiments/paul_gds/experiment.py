#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""Paul MULTITHREADED OFF/ON comparison on Q1, Q6 and Q17."""
import argparse
from datetime import datetime, timezone
import getpass
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
SUITE = HERE.parents[1]
REPO = SUITE.parents[1]
sys.path.insert(0, str(SUITE / 'scripts'))
from common import expand, load_config, read, sha, write
from runtime_support import node

PAUL_HEAD = 'b3e85674bad145871c4a23da0c932fb28e4d80c4'
CASES = (
    ('multithreaded-off', 'arm64', 'MULTITHREADED', 'OFF', 'kvikio-host', 'gds-off-on'),
    ('multithreaded-on', 'arm64', 'MULTITHREADED', 'ON', 'gds', 'gds-off-on'),
)


def prepare(args):
    sys.path.insert(0, str(SUITE))
    import benchmark
    if subprocess.check_output(['git', '-C', str(REPO), 'status', '--porcelain'], text=True).strip():
        raise ValueError('Commit the experiment before preparing a run')
    queries = [int(q) for q in args.queries.split(',')]
    if not queries or not set(queries) <= {1, 6, 17} or len(set(queries)) != len(queries):
        raise ValueError('This experiment only supports the selected representative queries')
    deployment = load_config('nvl72', str(args.deployment.resolve()))['deployment']
    if deployment['nodes'] <= 0 or deployment['gpus_per_node'] != 4:
        raise ValueError('This runner requires four GPUs per NVL node')
    executors = deployment['nodes'] * deployment['gpus_per_node']
    root = args.run.resolve()
    root.mkdir(parents=True, exist_ok=False)
    cases, configurations = [], {}
    for label, artifact, reader, mode, expected, group in CASES:
        artifacts = REPO / '.build/artifacts' / artifact
        engine = next(artifacts.glob('cudf-spark_*.jar'))
        config, _ = benchmark.prepare(SimpleNamespace(platform='nvl72',
            deployment=None, queries=args.queries,
            no_validate=False, iterations=2, diagnostics=False, engine=str(engine),
            tools=str(artifacts / 'benchmark-tools.jar')))
        config['deployment'] = dict(deployment)
        config['spark_conf']['spark.executor.instances'] = str(executors)
        benchmark.validate_target(config)
        config['spark_conf'].update({
            'spark.rapids.sql.format.parquet.reader.type': reader,
            'spark.rapids.sql.reader.gpuDirectStorage.mode': mode,
            'spark.rapids.sql.reader.kvikio.numThreads': '32'})
        config['paul_pr_test'] = {'expected_reader': expected, 'comparison_group': group,
            'paul_head': PAUL_HEAD, 'reference_score_eligible': False,
            'public_source_revision': config['artifact_manifest']['source_revision']}
        run = root / 'runs' / (root.name + '-' + label)
        write(run / 'config.json', config)
        write(run / 'profiles.json', expand(config, queries))
        write(run / 'owner.json', {'uid': os.getuid(), 'user': getpass.getuser(),
            'thread_id': os.environ.get('CODEX_THREAD_ID'), 'run_id': run.name})
        cases.append({'label': label, 'arm': label, 'group': group, 'run': str(run)})
        configurations[label] = config
    left, right = (configurations[c[0]]['spark_conf'] for c in CASES)
    differences = {k for k in set(left) | set(right) if left.get(k) != right.get(k)}
    if differences != {'spark.rapids.sql.reader.gpuDirectStorage.mode'}:
        raise AssertionError('OFF/ON controls differ beyond GDS mode')
    inputs = [SUITE / 'benchmark.py', *[p for directory in ('scripts', 'configs', 'inputs')
              for p in (SUITE / directory).rglob('*') if p.is_file() and '__pycache__' not in p.parts],
              *[p for p in HERE.rglob('*') if p.is_file() and '__pycache__' not in p.parts]]
    plan = {'cases': cases, 'queries': queries, 'nodes': deployment['nodes'], 'gpus': executors,
        'paul_head': PAUL_HEAD, 'reference_score_eligible': False,
        'thread_id': os.environ.get('CODEX_THREAD_ID'),
        'runtime_input_sha256': {str(p): sha(p) for p in inputs},
        'created_utc': datetime.now(timezone.utc).isoformat(),
        'protocol': 'One application per configuration; each query twice, validate and clean every sample.',
        'primary_metric': 'Second execution SQL time; retain both executions and cleanup time.',
        'gds_scope': 'Latest pinned Paul hybrid_scaleup, MULTITHREADED OFF versus ON.',
        'concurrency': 'Four Spark slots, concurrentGpuTasks=5 with dynamic=true in both arms.'}
    write(root / 'plan.json', plan)
    write(root / 'config.json', {'deployment': config['deployment']})
    write(root / 'status.json', {'status': 'prepared'})
    if args.submit:
        d = config['deployment']
        command = ['sbatch', '--parsable', '--export=ALL', '--job-name=paul-gds-off-on',
            '--nodes=' + str(d['nodes']), '--ntasks-per-node=1', '--cpus-per-task=144', '--gres=gpu:b200:4',
            '--mem=0', '--exclusive', '--no-requeue', '--time=00:40:00',
            '--partition=' + d['slurm_partition'],
            '--comment=codex-thread-' + str(plan['thread_id']),
            '--output=' + str(root / 'slurm-%j.log'), str(SUITE / 'scripts/run_nvl.slurm')]
        if d.get('node_list'):
            command.insert(-1, '--nodelist=' + d['node_list'])
        env = dict(os.environ, SCALEUP_RUN=str(root), SCALEUP_SUITE=str(HERE))
        response = subprocess.check_output(command, env=env, text=True).strip()
        write(root / 'submission.json', {'job_id': int(response.split(';')[0]), 'argv': command})
        print('Submitted', response, 'results:', root, flush=True)


def evidence(case, queries):
    run = Path(case['run'])
    data, config = read(run / 'summary.json'), read(run / 'config.json')
    if data['status'] != 'completed' or len(data['samples']) != len(queries) * 2:
        raise AssertionError('Incomplete representative-query matrix')
    if any(s['validation']['status'] != 'passed' for s in data['samples']):
        raise AssertionError('Result validation failed')
    executors = config['deployment']['nodes'] * config['deployment']['gpus_per_node']
    receipts = list(run.glob('applications/suite/*.cleanup.json'))
    if len(receipts) != len(queries) * 2 or any(len(read(p)) != executors for p in receipts):
        raise AssertionError('Incomplete cleanup acknowledgement matrix')
    hosts = list((run / 'runtime').iterdir())
    if len(hosts) != config['deployment']['nodes']:
        raise AssertionError('Missing allocated-node runtime evidence')
    for node_path in hosts:
        snapshots = [read(p) for p in node_path.glob('*.json') if p.stem.isdigit()]
        if len(snapshots) != config['deployment']['gpus_per_node']:
            raise AssertionError('Missing actual executor evidence')
    rdd_evidence = list((run / 'runtime').glob('*/reader-rdd-evidence.json'))
    if len(rdd_evidence) != 1 or read(rdd_evidence[0])['rdd_names'].get('GpuDataSourceRDD', 0) <= 0:
        raise AssertionError('MULTITHREADED DataSourceRDD did not execute')
    totals = {key: sum(s['reader_metrics'][key] for s in data['samples']) for key in (
        'hybridScanDeviceBytesRead', 'hybridScanNativeHostBytesRead', 'hybridScanGpuBusyHostBytesRead')}
    result = {'label': case['label'], 'group': case['group'], 'samples': data['samples'],
        'engine_sha256': config['engine_sha256'], 'tools_sha256': config['tools_sha256'],
        'public_source_revision': config['artifact_manifest']['source_revision'],
        'reader_totals': totals,
        'nodes': config['deployment']['nodes'], 'gpus': executors,
        'reference_score_eligible': False}
    write(run / 'case-evidence.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'node'))
    parser.add_argument('--run', required=True, type=Path)
    parser.add_argument('--deployment', type=Path)
    parser.add_argument('--queries', default='1,6,17')
    parser.add_argument('--submit', action='store_true')
    args = parser.parse_args()
    prepare(args) if args.action == 'prepare' else node(args, evidence)


if __name__ == '__main__':
    main()
