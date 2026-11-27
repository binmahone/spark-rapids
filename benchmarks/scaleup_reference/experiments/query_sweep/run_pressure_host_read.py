#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Compare the original reader and a pressure-gated adapter on a fixed query prefix."""
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
from monitor_cold_host_read import snapshot
from run_screen import launch_group
from variants import ORDER, Variant, catalog, original


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', required=True, type=Path)
    p.add_argument('--thread-id', required=True)
    p.add_argument('--phase', required=True, choices=['A0', 'B1', 'B2', 'A3'])
    a = p.parse_args()
    work = a.work.resolve()
    owner = read(work / 'evidence/owner.json')
    assert owner['thread_id'] == a.thread_id
    assert (work / 'evidence/prepare.exit').read_text().strip() == '0'
    assert Path('/proc/sys/vm/watermark_scale_factor').read_text().strip() == '10'
    assert int(Path('/sys/kernel/mm/lru_gen/enabled').read_text().strip(), 0) == 7
    assert subprocess.run(['systemctl', 'is-active', '--quiet', 'scaleup-reference-release.timer']).returncode != 0
    assert not subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader'], text=True).strip()
    for port in (19077, 19080):
        with socket.socket() as s:
            assert s.connect_ex((socket.gethostbyname(socket.gethostname()), port)) != 0
    artifacts = work / 'artifacts/pressure-host-read-v1'
    for test in ('buffered', 'cold-direct', 'error', 'short', 'pressure', 'pressure-error', 'pressure-short'):
        assert read(artifacts / test / 'result.json')['status'] == 'passed'
    directory = work / 'pressure-host-read' / a.phase
    directory.mkdir(parents=True, exist_ok=False)
    (directory / 'native-counters').mkdir()
    report = {'status': 'running', 'phase': a.phase, 'groups': [], 'profilers': [],
              'thread_id': a.thread_id, 'instance_id': owner['instance_id'],
              'source_revision': subprocess.check_output(['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip(),
              'source_sha256': {f: hashlib.sha256((HERE / f).read_bytes()).hexdigest() for f in
                               ('cold_host_read.cpp', 'pressure_read_control.py', 'run_pressure_host_read.py')},
              'adapter_sha256': hashlib.sha256((artifacts / 'libcold_host_read.so').read_bytes()).hexdigest(),
              'adapter_source_revision': (artifacts / 'source-revision.txt').read_text().strip(),
              'read_mode': 'pressure-direct' if a.phase.startswith('B') else 'original',
              'classification': 'Diagnostic prototype screening; not a full-suite score',
              'cache_policy': 'No cache flush; same prefix, two actions per query; cache history remains a confound',
              'instance_policy': 'Retain the instance across experiment phases'}
    report['host_state_before'] = {x: Path(x).read_text() for x in (
        '/proc/meminfo', '/sys/kernel/mm/lru_gen/enabled', '/proc/sys/vm/watermark_scale_factor')}
    write(directory / 'controller.json', {'pid': os.getpid(), 'owner_uid': os.getuid(),
                                         'start_time': services.start_time(os.getpid())})
    children, logs = [], []
    def interrupt(signum, frame):
        raise InterruptedError('Signal ' + str(signum))
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGALRM):
        signal.signal(sig, interrupt)
    signal.alarm(1800)
    def start(name, command):
        log = (directory / (name + '.log')).open('x')
        logs.append(log)
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        receipt = {'name': name, 'pid': process.pid, 'start_time': services.start_time(process.pid), 'command': command}
        children.append((process, receipt))
        report['profilers'].append(receipt)
        write(directory / 'state.json', report)
    try:
        start('host', [sys.executable, str(HERE.parent / 'runtime_prefilter/monitor_host.py'),
                       '--work', str(work), '--output', str(directory / 'host.jsonl')])
        start('vm', [sys.executable, str(HERE / 'monitor_vm.py'), '--work', str(work),
                     '--thread-id', a.thread_id, '--controller', str(directory / 'controller.json'),
                     '--output', str(directory / 'vm.jsonl')])
        start('pool', [sys.executable, str(HERE / 'monitor_read_pool.py'), '--work', str(work),
                       '--output', str(directory / 'pool.jsonl')])
        start('native', [sys.executable, str(HERE / 'monitor_cold_host_read.py'), '--directory', str(directory)])
        start('pressure', [sys.executable, str(HERE / 'pressure_read_control.py'), '--directory', str(directory)])
        deadline = time.monotonic() + 15
        while not (directory / 'pressure-ready.json').exists():
            assert all(c.poll() is None for c, _ in children)
            assert time.monotonic() < deadline
            time.sleep(0.1)
        selection = read(HERE / 'results/261126-aws-speed-of-light/selected-cases-before-q11.json')
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
        queries = ORDER[:ORDER.index(13) + 1]
        report['queries'] = queries
        overrides = {'spark.rapids.sql.reader.kvikio.numThreads': '32'}
        if report['read_mode'] == 'pressure-direct':
            overrides.update({'spark.executorEnv.LD_PRELOAD': str(artifacts / 'libcold_host_read.so'),
                              'spark.executorEnv.SCALEUP_COLD_READ_MODE': 'pressure-direct',
                              'spark.executorEnv.SCALEUP_COLD_READ_ROOT': '/opt/dlami/nvme/daily-trend-hardened/dataset',
                              'spark.executorEnv.SCALEUP_COLD_READ_STATS': str(directory / 'native-counters'),
                              'spark.executorEnv.SCALEUP_COLD_READ_PRESSURE': str(directory / 'pressure-control.bin')})
        report['overrides'] = overrides
        report['start_unix_ns'] = time.time_ns()
        write(directory / 'state.json', report)
        path, result = launch_group(work, 'pressure-host-read-' + a.phase, 0, [controls[q] for q in queries], overrides)
        report['groups'].append({'run': str(path), 'status': result['status'], 'samples': result['samples'],
                                 'error': result.get('error'), 'driver_exit_code': result['driver_exit_code']})
        assert result['status'] == 'completed' and result['driver_exit_code'] == 0
        assert len(result['samples']) == len(queries) * 2
        assert all(s['validation']['status'] == 'passed' for s in result['samples'])
        observed = {}
        for line in (directory / 'pool.jsonl').open():
            for record in json.loads(line)['executors']:
                observed.setdefault(str(record['pid']), set()).update(record['thread_names'])
        assert len(observed) == 8 and all(v == {'kvikio-' + str(i) for i in range(32)} for v in observed.values())
        report['native_pool_verification'] = {'passed': True, 'threads': {k: sorted(v) for k, v in observed.items()}}
        counters = snapshot(directory / 'native-counters')
        if report['read_mode'] == 'pressure-direct':
            assert all(counters[pid]['eligible'] > 0 for pid in observed)
            assert all(v['eligible'] == 0 for pid, v in counters.items() if pid not in observed)
            totals = {k: sum(counters[pid][k] for pid in observed) for k in next(iter(counters.values()))}
            assert totals['pressure_active'] > 0 and totals['pressure_inactive'] > 0 and totals['direct'] > 0, totals
            report['native_totals'] = totals
        else:
            assert not counters
        report['native_counters'] = counters
        assert all(c.poll() is None for c, _ in children)
        report['status'] = 'completed'
    except BaseException as error:
        report.update(status='failed', error=repr(error))
        raise
    finally:
        signal.alarm(0)
        for process, receipt in reversed(children):
            if process.poll() is None:
                assert services.start_time(process.pid) == receipt['start_time']
                process.send_signal(signal.SIGTERM)
        for process, receipt in children:
            try:
                receipt['exit_code'] = process.wait(timeout=25)
                if receipt['exit_code'] != 0:
                    report['status'] = 'failed'
            except subprocess.TimeoutExpired:
                report['status'] = 'failed'
                receipt['cleanup_error'] = 'Recorded process did not exit after SIGTERM'
        for log in logs:
            log.close()
        report['end_unix_ns'] = time.time_ns()
        write(directory / 'state.json', report)


if __name__ == '__main__':
    main()
