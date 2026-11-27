#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
"""Capture Q12 allocation stalls on a dedicated AWS instance without changing its reader."""
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
from critical_variants import cases as candidates
from run_screen import launch_group
from variants import ORDER, Variant, catalog, original


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', required=True)
    p.add_argument('--thread-id', required=True)
    p.add_argument('--phase', choices=['probe', 'probe1', 'probe2', 'probe3', 'probe4', 'probe5', 'probe6', 'probe7', 'short0', 'short1',
                   'prefix0', 'prefix1', 'light0', 'locks0', 'light1', 'locks1', 'locks2', 'light2', 'lruoff0', 'lruon0', 'lruoff1', 'lruon1'], required=True)
    p.add_argument('--observer-mode', choices=['full', 'light', 'locks'], default='full')
    a = p.parse_args()
    work = Path(a.work).resolve()
    owner = read(work / 'evidence/owner.json')
    assert owner['thread_id'] == a.thread_id
    if not a.phase.startswith('probe'):
        assert (work / 'evidence/prepare.exit').read_text().strip() == '0'
        assert (work / 'evidence/prepare-observers.exit').read_text().strip() == '0'
        assert (work / 'evidence/host-prerequisites.json').is_file()
    assert Path('/proc/sys/vm/watermark_scale_factor').read_text().strip() == '10'
    expected_lru = 0 if a.phase.startswith('lruoff') else 7
    assert int(Path('/sys/kernel/mm/lru_gen/enabled').read_text().strip(), 0) == expected_lru
    assert subprocess.run(['systemctl', 'is-active', '--quiet', 'scaleup-reference-release.timer']).returncode != 0
    assert not subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader'], text=True).strip()
    for port in (19077, 19080):
        with socket.socket() as s:
            assert s.connect_ex((socket.gethostbyname(socket.gethostname()), port)) != 0
    directory = work / 'q12-diagnosis' / a.phase
    directory.mkdir(parents=True, exist_ok=False)
    cpu_event = ('cycles:k' if a.phase == 'prefix1' else 'cpu-clock') if a.observer_mode == 'full' else None
    report = {'status': 'running', 'phase': a.phase, 'groups': [], 'profilers': [],
              'thread_id': a.thread_id, 'instance_id': owner['instance_id'],
              'source_revision': subprocess.check_output(['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip(),
              'classification': 'Diagnostic with profiling overhead; not a benchmark score',
              'cpu_event': cpu_event,
              'observer_mode': a.observer_mode,
              'lock_probe_provider': 'fentry/fexit' if a.observer_mode == 'locks' else None,
              'probe_thresholds_ns': {'RECLAIM': 50000000, 'EVICT': 100000000,
                                      'SCAN': 10000000, 'LOCK_WAIT': 50000000}
                                     if a.observer_mode == 'locks' else None,
              'gpm_enabled': a.observer_mode == 'full',
              'cache_policy': 'No cache flush; two actions per query',
              'instance_policy': 'Retain across diagnostic iterations; archive and release when the investigation ends'}
    report['host_state_before'] = {path: Path(path).read_text() for path in [
        '/proc/meminfo', '/proc/sys/vm/min_free_kbytes', '/proc/sys/vm/watermark_scale_factor',
        '/proc/sys/vm/zone_reclaim_mode', '/sys/kernel/mm/lru_gen/enabled',
        '/sys/kernel/mm/transparent_hugepage/enabled', '/sys/kernel/mm/transparent_hugepage/defrag']}
    write(directory / 'controller.json', {'pid': os.getpid(), 'owner_uid': os.getuid(),
                                         'start_time': services.start_time(os.getpid())})
    write(directory / 'state.json', report)
    children, logs = [], []
    def interrupt(signum, frame):
        raise InterruptedError('Signal ' + str(signum))
    signal.signal(signal.SIGTERM, interrupt)
    signal.signal(signal.SIGINT, interrupt)
    signal.signal(signal.SIGALRM, interrupt)
    signal.alarm(1800)
    def start(name, command, privileged=False):
        log = (directory / (name + '.log')).open('x'); logs.append(log)
        if privileged:
            command = ['sudo', '-n'] + command
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        receipt = {'name': name, 'pid': process.pid, 'start_time': services.start_time(process.pid),
                   'command': command, 'privileged': privileged}
        report['profilers'].append(receipt)
        children.append((process, receipt))
        write(directory / 'state.json', report)
        return process
    try:
        start('host', [sys.executable, str(HERE.parent / 'runtime_prefilter/monitor_host.py'),
                       '--work', str(work), '--output', str(directory / 'host.jsonl')])
        start('vm', [sys.executable, str(HERE / 'monitor_vm.py'), '--work', str(work),
                     '--thread-id', a.thread_id, '--controller', str(directory / 'controller.json'),
                     '--output', str(directory / 'vm.jsonl')])
        if a.observer_mode == 'full':
            start('gpm', ['/usr/bin/python3', str(HERE / 'monitor_gpm.py'),
                          '--directory', str(directory)], privileged=True)
            deadline = time.monotonic() + 30
            while not (directory / 'gpm-ready.json').exists():
                assert all(p.poll() is None for p, _ in children), 'Monitor startup failed; inspect logs'
                assert time.monotonic() < deadline, 'GPM startup timeout'
                time.sleep(0.5)
            start('threads', ['/usr/bin/python3', str(HERE / 'monitor_q12_threads.py'),
                              '--work', str(work), '--directory', str(directory)], privileged=True)
        start('pool', [sys.executable, str(HERE / 'monitor_read_pool.py'),
                       '--work', str(work), '--output', str(directory / 'pool.jsonl')])
        if a.observer_mode != 'light':
            script = 'q12_locks.bt' if a.observer_mode == 'locks' else 'q12_reclaim.bt'
            binary = 'bpftrace'
            if a.observer_mode == 'locks':
                tool = read(work / 'evidence/bpftrace-tool.json')
                assert tool['thread_id'] == a.thread_id and tool['instance_id'] == owner['instance_id']
                binary = tool['path']
                assert hashlib.sha256(Path(binary).read_bytes()).hexdigest() == tool['sha256']
                report['bpftrace_tool'] = tool
            start('reclaim', [binary, '-q', str(HERE / script)], privileged=True)
            deadline = time.monotonic() + 60
            while 'READY monotonic_ns=' not in (directory / 'reclaim.log').read_text():
                assert all(p.poll() is None for p, _ in children), 'Profiler startup failed'
                assert time.monotonic() < deadline, 'BPF probes not ready within 60 seconds'
                time.sleep(0.5)
        if a.observer_mode == 'full':
            start('cpu', ['perf', 'record', '-a', '-e', cpu_event, '-F', '49', '-g',
                          '--call-graph', 'fp', '--clockid', 'mono', '--no-buildid', '--no-buildid-cache',
                          '-o', str(directory / 'cpu.data'), '--', 'sleep', '3600'], privileged=True)
        assert all(p.poll() is None for p, _ in children), 'Profiler startup failed'
        if a.phase.startswith('probe'):
            time.sleep(4)
        else:
            selection_path = HERE / 'results/261126-aws-speed-of-light/selected-cases-before-q11.json'
            selection = read(selection_path)
            assert selection['common_configuration_sha256'] == hashlib.sha256((HERE.parents[1] / 'configs/aws48x.json').read_bytes()).hexdigest()
            available = {(c.query, c.name): c for c in catalog(include_followups=True)}
            names = {c['query']: c['variant'] for c in selection['candidates']}
            controls = {}
            for q in ORDER:
                case = available[(q, names[q])] if q in names else Variant(q, 'baseline', original(q), 'Retained reference')
                assert hashlib.sha256(case.sql.encode()).hexdigest() == selection['sql_sha256_by_query'][str(q)]
                controls[q] = replace(case, overrides=selection['runtime_overrides_by_query'].get(str(q), {}))
            choice = read(HERE / 'results/261126-aws-critical-queries/selection.json')
            assert choice['confirmed'] and choice['queries'] == [8, 21]
            for case in candidates():
                if case.query in choice['queries'] and case.name != 'critical_witness_wide':
                    controls[case.query] = replace(case, overrides={**controls[case.query].overrides, **case.overrides})
            queries = ([9, 10, 11, 12, 13] if a.phase.startswith('short') else
                       list(ORDER[:ORDER.index(12) + 1]))
            report['queries'] = queries
            report['start_unix_ns'] = time.time_ns()
            report['start_monotonic_ns'] = time.monotonic_ns()
            write(directory / 'state.json', report)
            path, result = launch_group(work, 'q12-diagnosis-' + a.phase, 0,
                                        [controls[q] for q in queries],
                                        {'spark.rapids.sql.reader.kvikio.numThreads': '32'})
            report['groups'].append({'run': str(path), 'status': result['status'],
                                     'samples': result['samples'], 'error': result.get('error'),
                                     'driver_exit_code': result['driver_exit_code']})
            assert result['status'] == 'completed' and result['driver_exit_code'] == 0
            assert len(result['samples']) == 2 * len(queries)
            assert all(s['validation']['status'] == 'passed' for s in result['samples'])
            observed = {}
            for line in (directory / 'pool.jsonl').open():
                for record in json.loads(line)['executors']:
                    observed.setdefault(record['pid'], set()).update(record['thread_names'])
            expected = {'kvikio-' + str(i) for i in range(32)}
            assert len(observed) == 8 and all(names == expected for names in observed.values()), observed
            report['native_pool_verification'] = {
                'passed': True, 'threads': {str(pid): sorted(names) for pid, names in observed.items()}}
        assert all(p.poll() is None for p, _ in children), 'Profiler exited before experiment completion'
        report['status'] = 'completed'
    except BaseException as error:
        report.update(status='failed', error=repr(error))
        raise
    finally:
        signal.alarm(0)
        if a.observer_mode == 'locks':
            for path in ('/sys/kernel/tracing/kprobe_profile', '/sys/kernel/debug/tracing/kprobe_profile'):
                snapshot = subprocess.run(['sudo', '-n', 'cat', path], capture_output=True, text=True)
                if snapshot.returncode == 0:
                    (directory / 'kprobe-profile.txt').write_text(snapshot.stdout)
                    report['kprobe_profile_source'] = path
                    break
        for process, receipt in reversed(children):
            if process.poll() is None:
                assert services.start_time(process.pid) == receipt['start_time']
                process.send_signal(signal.SIGINT if receipt['name'] in ('cpu', 'reclaim') else signal.SIGTERM)
        for process, receipt in children:
            try:
                receipt['exit_code'] = process.wait(timeout=180 if receipt['name'] == 'cpu' else 25)
            except subprocess.TimeoutExpired:
                report['status'] = 'failed'
                receipt['cleanup_error'] = 'Process did not exit after signal; investigate recorded PID'
        for log in logs:
            log.close()
        report['end_unix_ns'] = time.time_ns()
        if not report['gpm_enabled']:
            report['gpm_restored'] = None
        elif (directory / 'gpm-receipt.json').exists():
            restoration = read(directory / 'gpm-receipt.json')['restoration']
            report['gpm_restored'] = len(restoration) == 8 and all(r.get('passed') for r in restoration)
            if not report['gpm_restored']:
                report['status'] = 'failed'
        else:
            report['status'] = 'failed'
        write(directory / 'state.json', report)


if __name__ == '__main__':
    main()
