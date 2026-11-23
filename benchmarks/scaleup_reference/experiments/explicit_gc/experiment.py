#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""NVL full-suite GC OFF/ON; repeat ON/OFF only if natural explicit GC executes."""
import argparse
from datetime import datetime, timezone
import getpass
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import time
from types import SimpleNamespace

from gc_evidence import case_evidence, gc_file, startup_flags

HERE = Path(__file__).resolve().parent
PIN = 'faa47fe51f998ae97385e2b218be4a261006bfa6'


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.staging')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def git(root, *args):
    return subprocess.check_output(['git', '-C', str(root), *args], text=True).strip()


def prepare(args):
    source, root = args.source_checkout.resolve(), args.run.resolve()
    assert git(source, 'rev-parse', 'HEAD') == PIN
    assert not git(source, 'status', '--porcelain')
    if args.submit:
        assert not git(HERE, 'status', '--porcelain'), 'Commit the experiment before submission'
    suite = source / 'benchmarks/scaleup_reference'
    sys.path.insert(0, str(suite))
    import benchmark
    config, unused_profiles = benchmark.prepare(SimpleNamespace(
        platform='nvl72', deployment=str(args.deployment.resolve()), queries=None,
        no_validate=False, iterations=2, diagnostics=False, engine=None, tools=None))
    benchmark.validate_target(config)
    assert not any(k in config['spark_conf'] for k in (
        'spark.driver.defaultJavaOptions', 'spark.executor.defaultJavaOptions'))
    root.mkdir(parents=True, exist_ok=False)
    write(root / 'config.json', {'deployment': config['deployment']})
    cases = []
    for index, enabled in enumerate([False, True, True, False], 1):
        label = f'{index:02d}-' + ('on' if enabled else 'off')
        run = root / 'runs' / (root.name + '-' + label)
        current = json.loads(json.dumps(config))
        local = Path(current['deployment']['scratch_root']) / run.name
        options = ('-XX:' + ('+' if enabled else '-') + 'ExplicitGCInvokesConcurrent '
                   '-XX:+PrintFlagsFinal -Xlog:gc*,safepoint:file=' +
                   str(local / 'jvm-gc/gc-%p.log') +
                   ':utctime,uptimemillis,level,tags:filecount=0')
        for component in ['driver', 'executor']:
            current['spark_conf'][f'spark.{component}.defaultJavaOptions'] = options
        current['experiment'] = {'name': 'ExplicitGCInvokesConcurrent', 'enabled': enabled,
                                 'diagnostic_only': True, 'synthetic_gc_in_sql': False}
        write(run / 'config.json', current)
        write(run / 'profiles.json', benchmark.expand(current, current['queries']))
        write(run / 'owner.json', {'uid': os.getuid(), 'user': getpass.getuser(),
              'platform': 'nvl72', 'thread_id': os.environ.get('CODEX_THREAD_ID'),
              'run_id': run.name, 'created_utc': datetime.now(timezone.utc).isoformat()})
        cases.append({'label': label, 'enabled': enabled, 'run': str(run)})
    plan = {'source_checkout': str(source), 'engine_source_revision': PIN,
            'experiment_source_revision': git(HERE, 'rev-parse', 'HEAD'),
            'created_utc': datetime.now(timezone.utc).isoformat(), 'cases': cases,
            'nodes': config['deployment']['nodes'], 'executors': 32,
            'thread_id': os.environ.get('CODEX_THREAD_ID'),
            'primary_metric': 'Sum of second-execution SQL times; also report its cleanup-inclusive total',
            'secondary_metric': 'Per-query best-of-two sum; not a published reference score',
            'reverse_order_condition': 'At least one natural System.gc() GC event in OFF/ON application logs',
            'scope': 'Driver and executors only; identical GC logging in both cases; no injected SQL GC calls',
            'cleared_process_environment_keys': ['JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS',
                                                 '_JAVA_OPTIONS', 'SPARK_SUBMIT_OPTS']}
    write(root / 'plan.json', plan)
    write(root / 'status.json', {'status': 'prepared'})
    if args.submit:
        deployment = config['deployment']
        command = ['sbatch', '--parsable', '--export=ALL', '--job-name=scaleup-explicit-gc',
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
        print('Prepared without allocating resources:', root)


def collect_node(run, local, hostname):
    output = run / 'gc' / hostname
    output.mkdir(parents=True, exist_ok=True)
    for path in (local / 'jvm-gc').glob('*.log'):
        shutil.copyfile(path, output / path.name)
    startup = []
    files = list(local.glob('*/worker-*/work/*/*/stdout'))
    with tarfile.open(output / 'executor-startup-logs.tar.gz', 'w:gz') as archive:
        for path in files:
            flags = startup_flags(path.read_text(errors='replace'))
            if flags:
                startup.append({'source': str(path.relative_to(local)), 'flags': flags})
                archive.add(path, arcname=str(path.relative_to(local)))
                stderr = path.with_name('stderr')
                if stderr.exists():
                    archive.add(stderr, arcname=str(stderr.relative_to(local)))
    write(output / 'startup-flags.json', startup)
    with (output / 'sql-events.jsonl').open('w') as compact:
        for event in local.glob('*/events/*'):
            if not event.is_file() or event.name.startswith('.'):
                continue
            with event.open(errors='replace') as stream:
                for line in stream:
                    value = json.loads(line)
                    name = value.get('Event', '')
                    if name.endswith(('SparkListenerSQLExecutionStart', 'SparkListenerSQLExecutionEnd')):
                        record = {k: value[k] for k in ('Event', 'executionId', 'rootExecutionId', 'time') if k in value}
                    elif name == 'SparkListenerJobStart':
                        props = value.get('Properties', {})
                        record = {'Event': name, 'Job ID': value['Job ID'],
                                  'Submission Time': value['Submission Time'],
                                  'execution_id': props.get('spark.sql.execution.id'),
                                  'job_group': props.get('spark.jobGroup.id')}
                    elif name == 'SparkListenerJobEnd':
                        record = {k: value[k] for k in ('Event', 'Job ID', 'Completion Time')}
                    else:
                        continue
                    compact.write(json.dumps(record) + '\n')


def barrier(root, label, hostname, nodes):
    write(root / 'sync' / label / (hostname + '.json'), {'completed': True})
    deadline = time.monotonic() + 600
    while len(list((root / 'sync' / label).glob('*.json'))) != nodes:
        if list((root / 'failures').glob('*.json')):
            raise RuntimeError('Another node reported a failure')
        if time.monotonic() > deadline:
            raise TimeoutError('Node completion barrier expired')
        time.sleep(1)


def analyze(root):
    plan = read(root / 'plan.json')
    results = []
    for case in plan['cases']:
        run = Path(case['run'])
        if (run / 'case-evidence.json').exists():
            results.append(read(run / 'case-evidence.json'))
    decision = root / 'reverse-order-decision.json'
    expected = (4 if read(decision)['run_reverse_order'] else 2) if decision.exists() else None
    return {'status': 'completed' if len(results) == expected else 'running',
            'experiment': 'ExplicitGCInvokesConcurrent',
            'engine_source_revision': PIN, 'cases': results,
            'natural_explicit_gc_seen': any(r['system_gc_requests'] for r in results),
            'interpretation': ('Explicit GC executed; compare both run orders and align GC events to SQL windows before causal attribution'
                               if any(r['system_gc_requests'] for r in results) else
                               'No System.gc() GC events in the measured application lifecycles; timing differences do not demonstrate a benefit from this flag'),
            'reference_score_eligible': False}


def node(args):
    root = args.run.resolve()
    plan = read(root / 'plan.json')
    source = Path(plan['source_checkout'])
    suite = source / 'benchmarks/scaleup_reference'
    assert git(source, 'rev-parse', 'HEAD') == PIN and not git(source, 'status', '--porcelain')
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
        for index, case in enumerate(plan['cases']):
            if index >= 2:
                deadline = time.monotonic() + 600
                while not (root / 'reverse-order-decision.json').exists():
                    if time.monotonic() > deadline:
                        raise TimeoutError('Reverse-order decision expired')
                    time.sleep(1)
                if not read(root / 'reverse-order-decision.json')['run_reverse_order']:
                    break
            run = Path(case['run'])
            config = read(run / 'config.json')
            local = Path(config['deployment']['scratch_root']) / run.name
            (local / 'jvm-gc').mkdir(parents=True, exist_ok=True)
            live = subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid,process_name',
                                            '--format=csv,noheader'], text=True).strip()
            if live:
                raise RuntimeError('Allocated GPUs have existing compute processes: ' + live)
            env = dict(os.environ, SCALEUP_RUN=str(run), SCALEUP_SUITE=str(suite))
            for key in plan['cleared_process_environment_keys']:
                env.pop(key, None)
            if head:
                write(root / 'status.json', {'status': 'running', 'current_case': case['label']})
            logs = run / 'node-controller' / hostname
            logs.mkdir(parents=True, exist_ok=True)
            started = time.monotonic()
            try:
                with (logs / 'console.log').open('w') as output:
                    active = subprocess.Popen([sys.executable, str(suite / 'scripts/node.py'),
                                               '--run', str(run)], env=env,
                                              stdout=output, stderr=subprocess.STDOUT)
                    code = active.wait(timeout=2100)
            finally:
                if active and active.poll() is None:
                    active.terminate()
                    active.wait(timeout=60)
                collect_node(run, local, hostname)
            if code:
                raise RuntimeError('Reference node exited ' + str(code))
            write(logs / 'completed.json', {'wall_seconds': time.monotonic() - started, 'exit_code': code})
            barrier(root, case['label'], hostname, plan['nodes'])
            if head:
                evidence = case_evidence(run, case['enabled'], plan['executors'])
                write(run / 'case-evidence.json', evidence)
                summary = read(run / 'summary.json')
                summary.update(score_eligible=False, diagnostic_experiment='ExplicitGCInvokesConcurrent')
                write(run / 'summary.json', summary)
                if index == 1:
                    first_two = [read(Path(c['run']) / 'case-evidence.json') for c in plan['cases'][:2]]
                    write(root / 'reverse-order-decision.json', {
                        'run_reverse_order': any(r['system_gc_requests'] for r in first_two),
                        'natural_system_gc_requests': sum(r['system_gc_requests'] for r in first_two)})
                write(root / 'progress.json', analyze(root))
            barrier(root, case['label'] + '-analyzed', hostname, plan['nodes'])
        if head:
            write(root / 'summary.json', analyze(root))
            write(root / 'status.json', {'status': 'completed'})
    except BaseException as error:
        write(root / 'failures' / (hostname + '.json'), {'error': repr(error)})
        if head:
            write(root / 'status.json', {'status': 'failed', 'error': repr(error)})
        raise


def probe(args):
    root = args.run.resolve()
    root.mkdir(parents=True, exist_ok=False)
    records = []
    for enabled in [False, True]:
        label = 'on' if enabled else 'off'
        log = root / (label + '.gc.log')
        command = [str(args.java), '-Xms256m', '-Xmx256m', '-XX:+UseG1GC',
                   '-XX:-DisableExplicitGC', '-XX:' + ('+' if enabled else '-') + 'ExplicitGCInvokesConcurrent',
                   '-Xlog:gc*:file=' + str(log) + ':utctime,uptimemillis,level,tags:filecount=0',
                   str(HERE / 'ExplicitGcProbe.java')]
        result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=90)
        (root / (label + '.stdout.log')).write_text(result.stdout + result.stderr)
        assert 'ExplicitGCInvokesConcurrent=' + str(enabled).lower() in result.stdout
        record = gc_file(log)
        assert record['system_gc_requests'] == 1, record
        assert record['full_system_gc_events'] == (0 if enabled else 1), record
        records.append({'enabled': enabled, 'argv': command, 'gc': record})
    write(root / 'positive-control.json', {'scope': 'Standalone CPU Java probe; excluded from Spark timings',
                                         'results': records})
    print(json.dumps(records, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'node', 'probe'])
    parser.add_argument('--run', required=True, type=Path)
    parser.add_argument('--source-checkout', type=Path)
    parser.add_argument('--deployment', type=Path)
    parser.add_argument('--submit', action='store_true')
    parser.add_argument('--java', type=Path)
    args = parser.parse_args()
    if args.action == 'prepare':
        if not args.source_checkout or not args.deployment:
            parser.error('prepare requires --source-checkout and --deployment')
        prepare(args)
    elif args.action == 'node':
        node(args)
    else:
        if not args.java:
            parser.error('probe requires a Java 17 executable')
        probe(args)


if __name__ == '__main__':
    main()
