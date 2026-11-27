# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Run bounded critical-path probes on the retained instance with matched observers."""
import argparse
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write
import services
from critical_path_variants import cases, controls
from run_screen import launch_group


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', required=True, type=Path)
    p.add_argument('--thread-id', required=True)
    p.add_argument('--phase', required=True, choices=['oracle', 'A0', 'B1', 'B2', 'A3'])
    a = p.parse_args()
    work = a.work.resolve()
    owner = read(work / 'evidence/owner.json')
    assert owner['thread_id'] == a.thread_id
    assert Path('/proc/sys/vm/watermark_scale_factor').read_text().strip() == '10'
    assert int(Path('/sys/kernel/mm/lru_gen/enabled').read_text().strip(), 0) == 7
    assert subprocess.run(['systemctl', 'is-active', '--quiet', 'scaleup-reference-release.timer']).returncode != 0
    assert not subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader'], text=True).strip()
    for port in (19077, 19080):
        with socket.socket() as s:
            assert s.connect_ex((socket.gethostbyname(socket.gethostname()), port)) != 0
    root = work / 'critical-path'
    directory = root / a.phase
    directory.mkdir(parents=True, exist_ok=False)
    state = {'status': 'running', 'phase': a.phase, 'thread_id': a.thread_id,
             'instance_id': owner['instance_id'], 'groups': [], 'profilers': [],
             'source_revision': subprocess.check_output(['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip(),
             'source_sha256': {f: hashlib.sha256((HERE / f).read_bytes()).hexdigest() for f in
                               ['critical_path_variants.py', 'run_critical_path.py', 'run_cases.py', 'run_screen.py']},
             'classification': 'Focused Q9 and Q7 probes; unchanged Q8 temporal control; no full-suite score',
             'cache_policy': 'No cache flush; same query order and two actions per query; residency is not controlled',
             'instance_policy': 'Retain instance and data; stop only recorded experiment processes'}
    write(directory / 'controller.json', {'pid': os.getpid(), 'owner_uid': os.getuid(),
                                         'start_time': services.start_time(os.getpid())})
    write(directory / 'state.json', state)
    def interrupt(signum, frame):
        raise InterruptedError('Signal ' + str(signum))
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGALRM):
        signal.signal(sig, interrupt)
    signal.alarm(1800)
    children, logs = [], []
    def start(name, command):
        log = (directory / (name + '.log')).open('x')
        logs.append(log)
        child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        receipt = {'name': name, 'pid': child.pid, 'start_time': services.start_time(child.pid), 'command': command}
        children.append((child, receipt))
        state['profilers'].append(receipt)
        write(directory / 'state.json', state)
    try:
        if a.phase == 'oracle':
            with (directory / 'oracle.log').open('x') as log:
                subprocess.run([str(work / 'spark-venv/bin/python'), str(HERE / 'critical_path_oracle.py'),
                    '--output', str(root / 'oracle.json')], stdout=log, stderr=subprocess.STDOUT,
                    check=True, timeout=600)
            assert read(root / 'oracle.json')['status'] == 'passed'
        else:
            assert read(root / 'oracle.json')['status'] == 'passed'
            start('host', [sys.executable, str(HERE.parent / 'runtime_prefilter/monitor_host.py'),
                           '--work', str(work), '--output', str(directory / 'host.jsonl')])
            start('vm', [sys.executable, str(HERE / 'monitor_vm.py'), '--work', str(work),
                         '--thread-id', a.thread_id, '--controller', str(directory / 'controller.json'),
                         '--output', str(directory / 'vm.jsonl')])
            start('pool', [sys.executable, str(HERE / 'monitor_read_pool.py'), '--work', str(work),
                           '--output', str(directory / 'pool.jsonl')])
            selected = controls()
            queries = [9, 7, 8]
            for q in queries:
                selected[q] = replace(selected[q], overrides={**selected[q].overrides,
                    'spark.rapids.sql.join.hashTable.reuse': 'false'})
            if a.phase.startswith('B'):
                selected[9] = replace(selected[9], overrides={**selected[9].overrides,
                    'spark.rapids.sql.join.hashTable.reuse': 'true'})
                c = cases()[0]
                selected[7] = replace(c, overrides={**selected[7].overrides, **c.overrides})
            state['queries'] = queries
            state['start_unix_ns'] = time.time_ns()
            write(directory / 'state.json', state)
            path, result = launch_group(work, 'critical-path-' + a.phase, 0,
                [selected[q] for q in queries], {'spark.rapids.sql.reader.kvikio.numThreads': '32',
                                               'spark.rapids.sql.metrics.level': 'DEBUG'})
            state['groups'].append({'run': str(path), 'status': result['status'], 'samples': result['samples'],
                                    'error': result.get('error'), 'driver_exit_code': result['driver_exit_code']})
            assert result['status'] == 'completed' and result['driver_exit_code'] == 0, result.get('error')
            assert len(result['samples']) == 6 and all(s['validation']['status'] == 'passed' for s in result['samples'])
            for s in result['samples']:
                if s['query'] == 9:
                    count = sum(n['metrics'].get('hashTableReuses', 0) for n in s['plan_metrics'])
                    assert (count > 0) == a.phase.startswith('B'), (a.phase, count)
            observed = {}
            for line in (directory / 'pool.jsonl').open():
                for r in json.loads(line)['executors']:
                    observed.setdefault(str(r['pid']), set()).update(r['thread_names'])
            assert len(observed) == 8 and all(v == {'kvikio-' + str(i) for i in range(32)} for v in observed.values())
            state['pool_verified'] = True
            state['native_threads'] = {k: sorted(v) for k, v in observed.items()}
            assert all(c.poll() is None for c, _ in children)
        state['status'] = 'completed'
    except BaseException as e:
        state.update(status='failed', error=repr(e))
        raise
    finally:
        signal.alarm(0)
        for child, receipt in reversed(children):
            if child.poll() is None:
                assert services.start_time(child.pid) == receipt['start_time']
                child.send_signal(signal.SIGTERM)
        for child, receipt in children:
            try:
                receipt['exit_code'] = child.wait(timeout=25)
                if receipt['exit_code'] != 0:
                    state['status'] = 'failed'
            except subprocess.TimeoutExpired:
                state['status'] = 'failed'
                receipt['cleanup_error'] = 'Recorded observer did not exit after SIGTERM'
        for log in logs:
            log.close()
        state['end_unix_ns'] = time.time_ns()
        write(directory / 'state.json', state)


if __name__ == '__main__':
    main()
