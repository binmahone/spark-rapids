#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""Ordinary-broadcast GC mechanism and Q9-to-Q3 natural/manual cleanup controls."""
import argparse
from datetime import datetime, timezone
import getpass
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import socket
import subprocess
import sys
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'explicit_gc'))
spec = importlib.util.spec_from_file_location('gc_shared', HERE.parent / 'explicit_gc/experiment.py')
shared = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shared)
from gc_evidence import gc_file, startup_flags
read, write, git = shared.read, shared.write, shared.git
PIN = shared.PIN


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def prepare(args):
    source, root = args.source_checkout.resolve(), args.run.resolve()
    assert git(source, 'rev-parse', 'HEAD') == PIN
    assert not git(source, 'status', '--porcelain')
    if args.submit:
        assert not git(HERE, 'status', '--porcelain'), 'Commit the diagnostic before submission'
    suite = source / 'benchmarks/scaleup_reference'
    sys.path.insert(0, str(suite))
    import benchmark
    base, unused = benchmark.prepare(SimpleNamespace(
        platform='nvl72', deployment=str(args.deployment.resolve()), queries='9,3',
        no_validate=False, iterations=1, diagnostics=False, engine=None, tools=None))
    benchmark.validate_target(base)
    tools = args.tools.resolve()
    assert tools.name == 'broadcast-gc-tools.jar' and tools.is_file()
    receipt = read(tools.parent / 'build-receipt.json')
    assert sha(tools) == receipt['sha256'] and receipt['engine_sha256'] == base['engine_sha256']
    if args.submit:
        assert not receipt['source_dirty'], 'Build the tools from committed sources'
    for path in [*HERE.glob('*.java'), *HERE.glob('*.scala')]:
        assert sha(path) == receipt['source_sha256'][path.name], path
    root.mkdir(parents=True, exist_ok=False)
    write(root / 'config.json', {'deployment': base['deployment']})
    cases = []
    matrix = [('mechanism-off', False, False, 'mechanism'),
              ('mechanism-on', True, False, 'mechanism'),
              ('natural-off-1', False, False, 'natural'),
              ('natural-on-1', True, False, 'natural'),
              ('manual-off-1', False, True, 'natural'),
              ('manual-off-2', False, True, 'natural'),
              ('natural-on-2', True, False, 'natural'),
              ('natural-off-2', False, False, 'natural')]
    if args.stage == 'mechanism':
        matrix = matrix[:2]
    elif args.stage == 'natural':
        matrix = matrix[2:]
    for index, (label, enabled, manual, scenario) in enumerate(matrix, 1):
        run = root / 'runs' / (root.name + '-' + f'{index:02d}-' + label)
        current = json.loads(json.dumps(base))
        local = Path(current['deployment']['scratch_root']) / run.name
        options = ('-XX:+UseG1GC -XX:-DisableExplicitGC -XX:' +
                   ('+' if enabled else '-') + 'ExplicitGCInvokesConcurrent '
                   '-XX:+PrintFlagsFinal -javaagent:' + str(tools) + '=' + str(local / 'jvm-agent') +
                   ' -Xlog:gc*,safepoint:file=' + str(local / 'jvm-gc/gc-%p.log') +
                   ':utctime,uptimemillis,level,tags:filecount=0 ' +
                   '-Dlog4j.configurationFile=' + str(HERE / 'log4j2.properties'))
        for component in ['driver', 'executor']:
            current['spark_conf'][f'spark.{component}.defaultJavaOptions'] = options
        current['spark_conf']['spark.rapids.sql.benchmark.independentQueryCleanup.enabled'] = str(manual).lower()
        current.update(tools=str(tools), tools_sha256=sha(tools))
        current['experiment'] = {'name': 'ordinary-broadcast-gc', 'enabled': enabled,
                                 'manual_ordinary_cleanup': manual, 'scenario': scenario,
                                 'diagnostic_only': True, 'engine_unchanged': True,
                                 'python_proxy_gc_in_all_arms': True,
                                 'synthetic_gc_in_sql': False}
        write(run / 'config.json', current)
        write(run / 'profiles.json', benchmark.expand(current, current['queries']))
        write(run / 'owner.json', {'uid': os.getuid(), 'user': getpass.getuser(),
              'platform': 'nvl72', 'thread_id': os.environ.get('CODEX_THREAD_ID'),
              'run_id': run.name, 'created_utc': datetime.now(timezone.utc).isoformat()})
        cases.append({'label': label, 'enabled': enabled, 'manual': manual,
                      'scenario': scenario, 'run': str(run)})
    inputs = [suite / 'benchmark.py', *[p for folder in ['scripts', 'configs', 'inputs']
              for p in (suite / folder).rglob('*') if p.is_file() and '__pycache__' not in p.parts]]
    experiment_inputs = [p for p in HERE.rglob('*') if p.is_file() and '__pycache__' not in p.parts]
    plan = {'source_checkout': str(source), 'engine_source_revision': PIN,
            'experiment_source_revision': git(HERE, 'rev-parse', 'HEAD'),
            'created_utc': datetime.now(timezone.utc).isoformat(), 'cases': cases,
            'nodes': base['deployment']['nodes'], 'executors': 32,
            'thread_id': os.environ.get('CODEX_THREAD_ID'),
            'tools': str(tools), 'tools_sha256': sha(tools),
            'reference_score_eligible': False,
            'runtime_input_sha256': {str(p): sha(p) for p in [*inputs, *experiment_inputs]},
            'stage': args.stage,
            'cleared_process_environment_keys': ['JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS',
                                                 '_JAVA_OPTIONS', 'SPARK_SUBMIT_OPTS']}
    write(root / 'plan.json', plan)
    (root / 'command.txt').write_text(shlex.join([sys.executable, *sys.argv]) + '\n')
    write(root / 'status.json', {'status': 'prepared'})
    if args.submit:
        deployment = base['deployment']
        command = ['sbatch', '--parsable', '--export=ALL', '--job-name=broadcast-gc',
                   '--nodes=8', '--ntasks-per-node=1', '--cpus-per-task=144',
                   '--gres=gpu:b200:4', '--mem=0', '--exclusive', '--no-requeue',
                   '--time=00:45:00', '--partition=' + deployment['slurm_partition'],
                   '--comment=codex-thread-' + str(plan['thread_id']),
                   '--output=' + str(root / 'slurm-%j.log')]
        if deployment.get('node_list'):
            command.append('--nodelist=' + deployment['node_list'])
        command.append(str(suite / 'scripts/run_nvl.slurm'))
        env = dict(os.environ, SCALEUP_RUN=str(root), SCALEUP_SUITE=str(HERE))
        response = subprocess.check_output(command, env=env, text=True).strip()
        write(root / 'submission.json', {'job_id': int(response.split(';')[0]), 'argv': command})
        print('Submitted', response, 'results:', root)
    else:
        print('Prepared:', root)


def case_node(args):
    config = read(args.run / 'config.json')
    suite = Path(os.environ['SCALEUP_REFERENCE_SUITE'])
    sys.path.insert(0, str(suite / 'scripts'))
    import cohort
    import node as reference_node
    # The original lifecycle launches this experiment's SQL script; inputs remain pinned.
    cohort.SUITE = HERE
    sys.argv = [str(suite / 'scripts/node.py'), '--run', str(args.run)]
    reference_node.main()


def evidence(case):
    run = Path(case['run'])
    summary = read(run / 'summary.json')
    assert summary['status'] == 'completed' and len(summary['samples']) == 2
    assert all(s['validation']['status'] == 'passed' for s in summary['samples'])
    startup = []
    for node in run.glob('gc/*/startup-flags.json'):
        startup.extend(read(node))
    for driver in run.glob('applications/*/driver.log'):
        startup.append({'flags': startup_flags(driver.read_text(errors='replace'))})
    assert len(startup) == 33, len(startup)
    assert all(r['flags'] == {'UseG1GC': True, 'DisableExplicitGC': False,
                              'ExplicitGCInvokesConcurrent': case['enabled']} for r in startup)
    logs = [gc_file(p) for p in run.glob('gc/*/gc-*.log')]
    assert len(logs) == 33, len(logs)
    agent_files = list(run.glob('gc/*/agent/*.jsonl'))
    assert len(agent_files) == 33, len(agent_files)
    events = [json.loads(line) for p in agent_files for line in p.read_text().splitlines()]
    assert sum(e['event'] == 'class_transformed' for e in events) == 33
    retirement = sum(e['event'] == 'retireAfterIndependentQuery_enter' for e in events)
    requests = sum(e['event'] == 'gc_requested' for e in events)
    mechanism_path = run / 'applications/suite/mechanism.json'
    mechanism_result = read(mechanism_path) if mechanism_path.exists() else None
    assert (retirement > 0) == case['manual'], retirement
    expected_requests = 33 if mechanism_result and mechanism_result['status'] == 'observed' else 0
    assert requests == expected_requests, requests
    for receipt in run.glob('applications/*/*.cleanup.json'):
        assert len(read(receipt)) == 32
    result = read(run / 'applications/suite/result.json')
    output = {'label': case['label'], 'scenario': case['scenario'], 'enabled': case['enabled'],
              'manual_ordinary_cleanup': case['manual'], 'validated_samples': 2,
              'verified_application_jvms': 33, 'lifecycle_traced_jvms': 33,
              'retirement_calls': retirement, 'deliberate_gc_requests': requests,
              'system_gc_events': sum(x['system_gc_requests'] for x in logs),
              'full_system_gc_events': sum(x['full_system_gc_events'] for x in logs),
              'sum_jvm_pause_ms': sum(x['sum_jvm_pause_ms'] for x in logs),
              'max_jvm_pause_ms': max(x['max_jvm_pause_ms'] for x in logs),
              'pair_wall_seconds': result['pair_wall_seconds'],
              'sql_seconds': {s['query']: s['sql_seconds'] for s in summary['samples']},
              'reference_score_eligible': False}
    if mechanism_result:
        output['mechanism'] = mechanism_result
    return output


def node(args):
    root = args.run.resolve()
    plan = read(root / 'plan.json')
    suite = Path(plan['source_checkout']) / 'benchmarks/scaleup_reference'
    hostname = socket.gethostname()
    head = hostname == os.environ['SCALEUP_HEAD']
    active = None

    def interrupted(signum, unused_frame):
        if active and active.poll() is None:
            active.send_signal(signal.SIGTERM)
        raise SystemExit('Experiment interrupted: ' + str(signum))

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        assert sha(plan['tools']) == plan['tools_sha256']
        for name, digest in plan['runtime_input_sha256'].items():
            assert sha(name) == digest, name
        for case in plan['cases']:
            run = Path(case['run'])
            config = read(run / 'config.json')
            local = Path(config['deployment']['scratch_root']) / run.name
            (local / 'jvm-gc').mkdir(parents=True, exist_ok=True)
            (local / 'jvm-agent').mkdir(exist_ok=True)
            live = subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid,process_name',
                                            '--format=csv,noheader'], text=True).strip()
            if live:
                raise RuntimeError('Allocated GPUs have existing compute processes: ' + live)
            env = dict(os.environ, SCALEUP_RUN=str(run), SCALEUP_SUITE=str(suite),
                       SCALEUP_REFERENCE_SUITE=str(suite))
            for key in plan['cleared_process_environment_keys']:
                env.pop(key, None)
            if head:
                write(root / 'status.json', {'status': 'running', 'current_case': case['label']})
            logs = run / 'node-controller' / hostname
            logs.mkdir(parents=True, exist_ok=True)
            try:
                with (logs / 'console.log').open('w') as output:
                    active = subprocess.Popen([sys.executable, str(HERE / 'experiment.py'),
                                               'case-node', '--run', str(run)], env=env,
                                              stdout=output, stderr=subprocess.STDOUT)
                    code = active.wait(timeout=700)
            finally:
                if active and active.poll() is None:
                    active.terminate()
                    active.wait(timeout=60)
                shared.collect_node(run, local, hostname)
                destination = run / 'gc' / hostname / 'agent'
                destination.mkdir(parents=True, exist_ok=True)
                for path in (local / 'jvm-agent').glob('*.jsonl'):
                    shutil.copyfile(path, destination / path.name)
            if code:
                raise RuntimeError('Reference node exited ' + str(code))
            owned = [run / 'services/suite' / (hostname + '.json')]
            if head:
                owned.append(run / 'applications/suite/driver-service.json')
            stopped = [read(str(path) + '.stopped.json') for path in owned]
            assert all(r['remaining_recorded_processes'] == [] for r in stopped)
            write(logs / 'cleanup-audit.json', {'stopped_service_groups': len(stopped),
                                               'remaining_recorded_processes': []})
            shared.barrier(root, case['label'], hostname, plan['nodes'])
            if head:
                current = evidence(case)
                write(run / 'case-evidence.json', current)
                completed = [read(Path(c['run']) / 'case-evidence.json') for c in plan['cases']
                             if (Path(c['run']) / 'case-evidence.json').exists()]
                write(root / 'progress.json', completed)
            shared.barrier(root, case['label'] + '-analyzed', hostname, plan['nodes'])
        if head:
            write(root / 'summary.json', {'status': 'completed', 'cases': read(root / 'progress.json'),
                                         'reference_score_eligible': False})
            write(root / 'status.json', {'status': 'completed'})
    except BaseException as error:
        write(root / 'failures' / (hostname + '.json'), {'error': repr(error)})
        if head:
            write(root / 'status.json', {'status': 'failed', 'error': repr(error)})
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'node', 'case-node'])
    parser.add_argument('--run', required=True, type=Path)
    parser.add_argument('--source-checkout', type=Path)
    parser.add_argument('--deployment', type=Path)
    parser.add_argument('--tools', type=Path)
    parser.add_argument('--stage', choices=['mechanism', 'natural', 'all'], default='mechanism')
    parser.add_argument('--submit', action='store_true')
    args = parser.parse_args()
    if args.action == 'prepare':
        prepare(args)
    elif args.action == 'case-node':
        case_node(args)
    else:
        node(args)


if __name__ == '__main__':
    main()
