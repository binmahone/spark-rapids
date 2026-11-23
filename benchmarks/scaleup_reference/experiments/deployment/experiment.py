#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""Run paired NVL placement and UCX controls with the frozen engine and SQL runner."""
import argparse
from datetime import datetime, timezone
import getpass
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import threading
import time
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
SUITE = HERE.parents[1]
REPO = SUITE.parents[1]
sys.path.insert(0, str(SUITE / 'scripts'))
from common import expand, read, sha, write

ENGINE_PIN = 'faa47fe51f998ae97385e2b218be4a261006bfa6'
STAGES = {'placement': ('baseline', 'cpu-only', 'unbound', 'auto-tls', 'auto-net'),
          'workers': ('baseline', 'per-numa'),
          'qualification': ('baseline', 'cpu-only', 'unbound', 'auto-tls', 'bundled-cufile')}
ARMS = tuple(dict.fromkeys(arm for arms in STAGES.values() for arm in arms))
CLEAR_ENV = ('JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS',
             'SPARK_SUBMIT_OPTS', 'UCX_TLS', 'UCX_NET_DEVICES')


def git(*args):
    return subprocess.check_output(['git', '-C', str(REPO), *args], text=True).strip()


def variant(base, arm, stage='placement'):
    if arm not in ARMS:
        raise ValueError(arm)
    current = json.loads(json.dumps(base))
    placement = arm if arm in ('cpu-only', 'unbound') else 'cpu-memory'
    current['deployment_experiment'] = {'arm': arm, 'placement': placement,
        'diagnostic_only': True, 'engine_source_revision': ENGINE_PIN,
        'reference_score_eligible': False, 'verify_cuda_uuid': stage == 'workers',
        'worker_layout': 'numa' if arm == 'per-numa' else 'gpu',
        'bundle_cufile': arm == 'bundled-cufile'}
    if arm in ('auto-tls', 'auto-net'):
        key = 'UCX_TLS' if arm == 'auto-tls' else 'UCX_NET_DEVICES'
        del current['spark_conf']['spark.executorEnv.' + key]
    return current


def prepare(args):
    sys.path.insert(0, str(SUITE))
    import benchmark
    frozen = args.reference_checkout.resolve()
    if subprocess.check_output(['git', '-C', str(frozen), 'rev-parse', 'HEAD'], text=True).strip() != ENGINE_PIN:
        raise ValueError('Reference checkout differs from the engine source pin')
    if git('status', '--porcelain'):
        raise ValueError('Commit the experiment before preparing a run')
    artifacts = frozen / '.build/artifacts/arm64'
    base, _ = benchmark.prepare(SimpleNamespace(platform='nvl72',
        deployment=str(args.deployment.resolve()),
        queries=args.queries or (None if args.stage == 'qualification' else '9,3,6'),
        no_validate=False, iterations=2, diagnostics=False,
        engine=str(next(artifacts.glob('cudf-spark_*.jar'))),
        tools=str(artifacts / 'benchmark-tools.jar')))
    benchmark.validate_target(base)
    if args.stage == 'qualification' and base['queries'] != read(SUITE / 'configs/nvl72.json')['queries']:
        raise ValueError('Full qualification requires the reference query set and order')
    for component in ('driver', 'executor'):
        if 'spark.' + component + '.defaultJavaOptions' in base['spark_conf']:
            raise ValueError('Unexpected diagnostic JVM options')
    root = args.run.resolve()
    root.mkdir(parents=True, exist_ok=False)
    write(root / 'config.json', {'deployment': base['deployment']})
    order = list(STAGES[args.stage])
    if args.stage != 'qualification':
        order += list(reversed(STAGES[args.stage]))
    cases = []
    for index, arm in enumerate(order, 1):
        label = f'{index:02d}-{arm}'
        run = root / 'runs' / (root.name + '-' + label)
        current = variant(base, arm, args.stage)
        write(run / 'config.json', current)
        write(run / 'profiles.json', expand(current, current['queries']))
        write(run / 'owner.json', {'uid': os.getuid(), 'user': getpass.getuser(),
            'thread_id': os.environ.get('CODEX_THREAD_ID'), 'run_id': run.name})
        cases.append({'label': label, 'arm': arm, 'run': str(run)})
    files = [SUITE / 'benchmark.py', *[p for name in ('scripts', 'configs', 'inputs')
             for p in (SUITE / name).rglob('*') if p.is_file() and '__pycache__' not in p.parts],
             *[p for p in HERE.rglob('*') if p.is_file() and '__pycache__' not in p.parts]]
    plan = {'cases': cases, 'stage': args.stage, 'nodes': base['deployment']['nodes'], 'queries': base['queries'],
        'engine_source_revision': ENGINE_PIN, 'runtime_revision': git('rev-parse', 'HEAD'),
        'runtime_input_sha256': {str(p): sha(p) for p in files},
        'thread_id': os.environ.get('CODEX_THREAD_ID'), 'reference_score_eligible': False,
        'engine_sha256': base['engine_sha256'], 'tools_sha256': base['tools_sha256'],
        'created_utc': datetime.now(timezone.utc).isoformat(),
        'primary_metric': 'Second execution SQL time per query; retain best-of-two and cleanup-inclusive totals',
        'comparison': 'Each arm against baseline; cpu-only versus unbound isolates CPU binding',
        'clear_environment': list(CLEAR_ENV)}
    write(root / 'plan.json', plan)
    write(root / 'status.json', {'status': 'prepared'})
    (root / 'command.txt').write_text(shlex.join([sys.executable, *sys.argv]) + '\n')
    if args.submit:
        d = base['deployment']
        command = ['sbatch', '--parsable', '--export=ALL', '--job-name=nvl-deployment-ab',
            '--nodes=' + str(d['nodes']), '--ntasks-per-node=1', '--cpus-per-task=144',
            '--gres=gpu:b200:4', '--mem=0', '--exclusive', '--no-requeue', '--time=00:45:00',
            '--partition=' + d['slurm_partition'], '--comment=codex-thread-' + str(plan['thread_id']),
            '--output=' + str(root / 'slurm-%j.log')]
        if d.get('node_list'):
            command.append('--nodelist=' + d['node_list'])
        if args.dependency:
            command.append('--dependency=afterok:' + str(args.dependency))
        command.append(str(SUITE / 'scripts/run_nvl.slurm'))
        env = dict(os.environ, SCALEUP_RUN=str(root), SCALEUP_SUITE=str(HERE))
        response = subprocess.check_output(command, env=env, text=True).strip()
        write(root / 'submission.json', {'job_id': int(response.split(';')[0]), 'argv': command})
        print('Submitted', response, 'results:', root, flush=True)


def barrier(root, label, node, nodes):
    write(root / 'sync' / label / (node + '.json'), {'completed': True})
    deadline = time.monotonic() + 180
    while len(list((root / 'sync' / label).glob('*.json'))) != nodes:
        if list((root / 'failures').glob('*.json')):
            raise RuntimeError('Another node reported a failure')
        if time.monotonic() >= deadline:
            raise TimeoutError('Node barrier expired')
        time.sleep(1)


def monitor(run, local, stopped):
    """Retain process placement and interface activity without inferring payload transport."""
    node = socket.gethostname()
    output = run / 'runtime' / node
    output.mkdir(parents=True, exist_ok=True)
    with (output / 'interface-samples.jsonl').open('w') as stream:
        while not stopped.is_set():
            tick = {'utc': datetime.now(timezone.utc).isoformat(), 'interfaces': {}}
            for interface in Path('/sys/class/net').iterdir():
                if interface.name.startswith(('bond', 'ib', 'en')):
                    values = {}
                    for counter in ('rx_bytes', 'tx_bytes'):
                        try:
                            values[counter] = int((interface / 'statistics' / counter).read_text())
                        except OSError:
                            pass
                    tick['interfaces'][interface.name] = values
            stream.write(json.dumps(tick) + '\n')
            stream.flush()
            for path in Path('/proc').iterdir():
                if not path.name.isdigit():
                    continue
                try:
                    argv = (path / 'cmdline').read_bytes().replace(b'\0', b' ').decode(errors='replace')
                    if 'CoarseGrainedExecutorBackend' not in argv or str(local) not in argv:
                        continue
                    record = {'pid': int(path.name), 'argv': argv,
                        'status': (path / 'status').read_text(),
                        'numa_maps': (path / 'numa_maps').read_text(),
                        'maps': (path / 'maps').read_text(),
                        'environment': [x.decode(errors='replace') for x in
                            (path / 'environ').read_bytes().split(b'\0')
                            if x.startswith((b'UCX_', b'CUDA_', b'LD_LIBRARY_PATH=', b'CUFILE_'))]}
                    arguments = argv.split()
                    if '--resourcesFile' in arguments:
                        resources_file = Path(arguments[arguments.index('--resourcesFile') + 1])
                        record['spark_assigned_resources'] = read(resources_file)
                    write(output / (path.name + '.json'), record)
                except (FileNotFoundError, ProcessLookupError, PermissionError):
                    pass
            stopped.wait(5)


def collect(run, local):
    output = run / 'runtime' / socket.gethostname()
    with tarfile.open(output / 'executor-logs.tar.gz', 'w:gz') as archive:
        for path in local.glob('suite/worker-*/work/*/*/*'):
            if path.name in ('stdout', 'stderr') and path.is_file():
                archive.add(path, arcname=str(path.relative_to(local)))
    for name in ('cufile.json', 'gdscheck.txt'):
        if (local / name).exists():
            shutil.copyfile(local / name, output / name)
    write(output / 'local-storage.json', {'findmnt': subprocess.check_output(
        ['findmnt', '-T', str(local)], text=True), 'shm': subprocess.check_output(
        ['df', '-B1', '/dev/shm'], text=True)})


def evidence(case, queries):
    run = Path(case['run'])
    data = read(run / 'summary.json')
    if data['status'] != 'completed' or len(data['samples']) != len(queries) * 2:
        raise AssertionError('Incomplete query matrix')
    if any(s['validation']['status'] != 'passed' for s in data['samples']):
        raise AssertionError('Result validation failed')
    for receipt in run.glob('applications/suite/*.cleanup.json'):
        if len(read(receipt)) != 32:
            raise AssertionError('Missing executor cleanup acknowledgement')
    suite_id = read(run / 'applications/suite/result.json')['application_id']
    runtime_nodes = list((run / 'runtime').iterdir())
    if len(runtime_nodes) != 8:
        raise AssertionError('Missing runtime evidence nodes')
    for node in runtime_nodes:
        records = [read(p) for p in node.glob('*.json') if p.stem.isdigit()]
        records = [r for r in records if '--app-id ' + suite_id + ' ' in r['argv']]
        if len(records) != 4:
            raise AssertionError('Missing executor placement evidence: ' + str(node))
        for record in records:
            environment = dict(item.split('=', 1) for item in record['environment'])
            expected = {'UCX_TLS': None if case['arm'] == 'auto-tls' else '^posix',
                        'UCX_NET_DEVICES': None if case['arm'] == 'auto-net' else 'bond0'}
            for key, value in expected.items():
                if environment.get(key) != value:
                    raise AssertionError('Executor environment differs: ' + key)
            if case['arm'] == 'bundled-cufile':
                libraries = [line.split()[-1] for line in record['maps'].splitlines()
                             if '/libcufile.so' in line or '/libcufile_rdma.so' in line]
                if len(libraries) != 2 or any('/cufile-runtime/' not in name for name in libraries):
                    raise AssertionError('Application did not load the bundled cuFile libraries')
    result = {'label': case['label'], 'arm': case['arm'], 'samples': data['samples'],
        'second_sql': {str(s['query']): s['sql_seconds'] for s in data['samples'] if s['iteration'] == 2},
        'best_sql_sum_seconds': data['best_sql_sum_seconds'],
        'selected_sql_plus_cleanup_sum_seconds': data['selected_sql_plus_cleanup_sum_seconds'],
        'second_sql_sum_seconds': sum(s['sql_seconds'] for s in data['samples'] if s['iteration'] == 2),
        'reference_score_eligible': False}
    write(run / 'case-evidence.json', result)
    return result


def node(args):
    root, hostname = args.run.resolve(), socket.gethostname()
    plan = read(root / 'plan.json')
    head = hostname == os.environ['SCALEUP_HEAD']
    active = None
    def interrupted(signum, unused_frame):
        if active and active.poll() is None:
            active.terminate()
        raise SystemExit('Experiment interrupted')
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        for name, digest in plan['runtime_input_sha256'].items():
            if sha(name) != digest:
                raise ValueError('Source changed: ' + name)
        live = subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid,process_name',
                                        '--format=csv,noheader'], text=True).strip()
        write(root / 'preflight' / (hostname + '.json'), {'existing_compute_processes': live})
        if live:
            raise RuntimeError('Allocated GPUs have existing compute processes: ' + live)
        completed = []
        for case in plan['cases']:
            run = Path(case['run'])
            config = read(run / 'config.json')
            local = Path(config['deployment']['scratch_root']) / run.name
            barrier(root, case['label'] + '-start', hostname, plan['nodes'])
            env = dict(os.environ, SCALEUP_RUN=str(run), SCALEUP_SUITE=str(SUITE))
            for key in CLEAR_ENV:
                env.pop(key, None)
            stopped = threading.Event()
            watcher = threading.Thread(target=monitor, args=(run, local, stopped), daemon=True)
            watcher.start()
            if head:
                write(root / 'status.json', {'status': 'running', 'current_case': case['label']})
            logs = run / 'node-controller' / hostname
            logs.mkdir(parents=True, exist_ok=True)
            try:
                with (logs / 'console.log').open('w') as output:
                    active = subprocess.Popen([sys.executable, str(SUITE / 'scripts/node.py'),
                        '--run', str(run)], env=env, stdout=output, stderr=subprocess.STDOUT)
                    code = active.wait(timeout=600)
            finally:
                if active and active.poll() is None:
                    active.terminate()
                    active.wait(timeout=50)
                stopped.set()
                watcher.join(timeout=10)
                collect(run, local)
            if code:
                raise RuntimeError('Node exited ' + str(code))
            owned = [run / 'services/suite' / (hostname + '.json')]
            if head:
                owned.append(run / 'applications/suite/driver-service.json')
            for state in owned:
                if read(str(state) + '.stopped.json')['remaining_recorded_processes']:
                    raise AssertionError('A service group survived teardown')
            write(logs / 'cleanup-audit.json', {'remaining_recorded_processes': []})
            barrier(root, case['label'] + '-done', hostname, plan['nodes'])
            if head:
                completed.append(evidence(case, plan['queries']))
                write(root / 'progress.json', completed)
            barrier(root, case['label'] + '-analyzed', hostname, plan['nodes'])
        if head:
            write(root / 'summary.json', {'status': 'completed', 'cases': completed,
                'reference_score_eligible': False})
            write(root / 'status.json', {'status': 'completed'})
    except BaseException as error:
        write(root / 'failures' / (hostname + '.json'), {'error': repr(error)})
        if head:
            write(root / 'status.json', {'status': 'failed', 'error': repr(error)})
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'node'))
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--reference-checkout', type=Path)
    parser.add_argument('--deployment', type=Path)
    parser.add_argument('--queries')
    parser.add_argument('--stage', choices=tuple(STAGES), default='placement')
    parser.add_argument('--submit', action='store_true')
    parser.add_argument('--dependency', type=int, help='Require successful completion of this Slurm job')
    args = parser.parse_args()
    prepare(args) if args.action == 'prepare' else node(args)


if __name__ == '__main__':
    main()
