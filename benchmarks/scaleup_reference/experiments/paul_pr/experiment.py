#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""Three requested Paul comparisons on NVL, restricted to Q1, Q6 and Q17."""
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
from common import expand, read, sha, write
from runtime_support import node

BASE = '9b9568673843c91f4712fb165cef43e386dba1ec'
PR_HEAD = '1e83455bf84e8b4cd409b27fed9495f804a61ea8'
CASES = (
    ('perfile-before', 'before', 'PERFILE', 'OFF', 'hadoop-host', 'host-perfile'),
    ('perfile-after', 'after', 'PERFILE', 'OFF', 'kvikio-host', 'host-perfile'),
    ('multithreaded-before', 'before', 'MULTITHREADED', 'OFF', 'hadoop-host', 'host-multithreaded'),
    ('multithreaded-after', 'after', 'MULTITHREADED', 'OFF', 'kvikio-host', 'host-multithreaded'),
    ('perfile-gds-on', 'gds', 'PERFILE', 'WAIT_IF_GPU_BUSY', 'gds', 'gds-policy'),
    ('perfile-gds-auto', 'gds', 'PERFILE', 'HOST_IF_GPU_BUSY', 'gds', 'gds-policy'),
)


def prepare(args):
    sys.path.insert(0, str(SUITE))
    import benchmark
    if subprocess.check_output(['git', '-C', str(REPO), 'status', '--porcelain'], text=True).strip():
        raise ValueError('Commit the experiment before preparing a run')
    queries = [int(q) for q in args.queries.split(',')]
    if not queries or not set(queries) <= {1, 6, 17} or len(set(queries)) != len(queries):
        raise ValueError('This experiment only supports the selected representative queries')
    root = args.run.resolve()
    root.mkdir(parents=True, exist_ok=False)
    cases, configurations = [], {}
    for label, artifact, reader, mode, expected, group in CASES:
        artifacts = REPO / '.build/artifacts' / artifact
        engine = next(artifacts.glob('cudf-spark_*.jar'))
        config, _ = benchmark.prepare(SimpleNamespace(platform='nvl72',
            deployment=str(args.deployment.resolve()), queries=args.queries,
            no_validate=False, iterations=2, diagnostics=False, engine=str(engine),
            tools=str(artifacts / 'benchmark-tools.jar')))
        benchmark.validate_target(config)
        config['spark_conf'].update({
            'spark.rapids.sql.format.parquet.reader.type': reader,
            'spark.rapids.sql.reader.gpuDirectStorage.mode': mode,
            'spark.rapids.sql.reader.kvikio.numThreads': '32'})
        if group == 'gds-policy':
            config['spark_conf'].update({'spark.rapids.sql.concurrentGpuTasks': '2',
                                         'spark.rapids.sql.concurrentGpuTasks.dynamic': 'false'})
        config['paul_pr_test'] = {'expected_reader': expected, 'comparison_group': group,
            'pr': 17023, 'pr_base': BASE, 'pr_head': PR_HEAD,
            'public_source_revision': config['artifact_manifest']['source_revision'],
            'gds_policy_alias': {'WAIT_IF_GPU_BUSY': 'ON', 'HOST_IF_GPU_BUSY': 'AUTO'}.get(mode, mode),
            'automatic_host_fallback_requires_runtime_evidence': group == 'gds-policy',
            'reference_score_eligible': False}
        run = root / 'runs' / (root.name + '-' + label)
        write(run / 'config.json', config)
        write(run / 'profiles.json', expand(config, queries))
        write(run / 'owner.json', {'uid': os.getuid(), 'user': getpass.getuser(),
            'thread_id': os.environ.get('CODEX_THREAD_ID'), 'run_id': run.name})
        cases.append({'label': label, 'arm': label, 'group': group, 'run': str(run)})
        configurations[label] = config
    for left, right in ((0, 1), (2, 3)):
        if configurations[CASES[left][0]]['spark_conf'] != configurations[CASES[right][0]]['spark_conf']:
            raise AssertionError('Host PR controls have different Spark settings')
    left, right = (configurations[CASES[i][0]]['spark_conf'] for i in (4, 5))
    differences = {k for k in set(left) | set(right) if left.get(k) != right.get(k)}
    if differences != {'spark.rapids.sql.reader.gpuDirectStorage.mode'}:
        raise AssertionError('GDS policy controls change more than their policy')
    inputs = [SUITE / 'benchmark.py', *[p for directory in ('scripts', 'configs', 'inputs')
              for p in (SUITE / directory).rglob('*') if p.is_file() and '__pycache__' not in p.parts],
              *[p for p in HERE.rglob('*') if p.is_file() and '__pycache__' not in p.parts]]
    plan = {'cases': cases, 'queries': queries, 'nodes': 8,
        'pr_base': BASE, 'pr_head': PR_HEAD, 'reference_score_eligible': False,
        'thread_id': os.environ.get('CODEX_THREAD_ID'),
        'runtime_input_sha256': {str(p): sha(p) for p in inputs},
        'created_utc': datetime.now(timezone.utc).isoformat(),
        'protocol': 'One application per configuration; each query twice, validate and clean every sample.',
        'primary_metric': 'Second execution SQL time; retain both executions and cleanup time.',
        'gds_scope': 'Paul hybrid_scaleup implementation; legacy mode names map to ON and AUTO.',
        'gds_contention': 'Four Spark task slots, two fixed GPU permits; check busy-host bytes.'}
    write(root / 'plan.json', plan)
    write(root / 'config.json', {'deployment': config['deployment']})
    write(root / 'status.json', {'status': 'prepared'})
    if args.submit:
        d = config['deployment']
        command = ['sbatch', '--parsable', '--export=ALL', '--job-name=paul-pr17023',
            '--nodes=8', '--ntasks-per-node=1', '--cpus-per-task=144', '--gres=gpu:b200:4',
            '--mem=0', '--exclusive', '--no-requeue', '--time=00:40:00',
            '--partition=' + d['slurm_partition'], '--nodelist=' + d['node_list'],
            '--comment=codex-thread-' + str(plan['thread_id']),
            '--output=' + str(root / 'slurm-%j.log'), str(SUITE / 'scripts/run_nvl.slurm')]
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
    receipts = list(run.glob('applications/suite/*.cleanup.json'))
    if len(receipts) != len(queries) * 2 or any(len(read(p)) != 32 for p in receipts):
        raise AssertionError('Incomplete cleanup acknowledgement matrix')
    for node_path in (run / 'runtime').iterdir():
        snapshots = [read(p) for p in node_path.glob('*.json') if p.stem.isdigit()]
        if len(snapshots) != 4:
            raise AssertionError('Missing actual executor evidence')
    totals = {key: sum(s['reader_metrics'][key] for s in data['samples']) for key in (
        'hybridScanDeviceBytesRead', 'hybridScanNativeHostBytesRead', 'hybridScanGpuBusyHostBytesRead')}
    result = {'label': case['label'], 'group': case['group'], 'samples': data['samples'],
        'engine_sha256': config['engine_sha256'], 'tools_sha256': config['tools_sha256'],
        'public_source_revision': config['artifact_manifest']['source_revision'],
        'reader_totals': totals,
        'auto_busy_branch_exercised': totals['hybridScanGpuBusyHostBytesRead'] > 0,
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
