#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.


"""Owned service lifecycle and process evidence for isolated NVL PR experiments."""
import json
from collections import Counter
from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import threading
import time

SUITE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SUITE / 'scripts'))
from common import read, sha, write

CLEAR_ENV = ('JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS',
             'SPARK_SUBMIT_OPTS', 'UCX_TLS', 'UCX_NET_DEVICES')

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
    events = local / 'suite/events'
    if events.exists():
        names, stages = Counter(), 0
        for path in events.iterdir():
            if not path.is_file() or path.name.startswith('.'):
                continue
            with path.open() as stream:
                for line in stream:
                    event = json.loads(line)
                    if event.get('Event') == 'SparkListenerStageSubmitted':
                        stages += 1
                        names.update(rdd['Name'] for rdd in event['Stage Info']['RDD Info'])
        if stages:
            write(output / 'reader-rdd-evidence.json',
                  {'submitted_stages': stages, 'rdd_names': dict(names)})
    for name in ('cufile.json', 'gdscheck.txt'):
        if (local / name).exists():
            shutil.copyfile(local / name, output / name)
    write(output / 'local-storage.json', {'findmnt': subprocess.check_output(
        ['findmnt', '-T', str(local)], text=True), 'shm': subprocess.check_output(
        ['df', '-B1', '/dev/shm'], text=True)})


def node(args, evidence):
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

