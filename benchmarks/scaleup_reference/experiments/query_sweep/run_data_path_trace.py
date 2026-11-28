#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Capture one bounded CUDA timeline outside the scored SOL applications."""
import argparse
from pathlib import Path
import os
import shlex
import signal
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write
import services


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', type=Path, required=True)
    p.add_argument('--thread-id', required=True)
    p.add_argument('--variant', choices=('control', 'scan-pack', 'scan-split', 'scan-split-cuda', 'shuffle-buffer', 'shuffle-wait'), default='control')
    p.add_argument('--query', type=int, choices=(5, 8, 10, 18, 20), default=8)
    a = p.parse_args()
    work = a.work.resolve()
    owner = read(work / 'evidence/owner.json')
    assert owner['thread_id'] == a.thread_id
    assert (work / 'evidence/prepare.exit').read_text().strip() == '0'
    build_record = work / 'evidence/shuffle-wait-build-service.json'
    if build_record.exists():
        assert not any(services.group_members(r) for r in read(build_record)), 'Build must finish before measurement'
    assert not subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader'], text=True).strip()
    suffix = '-' + a.variant
    label = 'Q' + str(a.query)
    artifact = 'q' + str(a.query)
    directory = work / ('data-path-trace/' + label + '-process-tree' + suffix)
    directory.mkdir(parents=True, exist_ok=False)
    space = os.statvfs(directory)
    assert space.f_bavail * space.f_frsize > 15 * 2**30
    session = 'data-path-' + a.thread_id + '-' + artifact + '-tree' + suffix
    report = {'status': 'starting', 'owner': owner, 'classification': 'Diagnostic profiling; excluded from SOL scores',
              'nsys_session': session, 'source_revision': subprocess.check_output(
                  ['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip(),
              'start_unix_ns': time.time_ns()}
    write(directory / 'controller.json', {'pid': os.getpid(), 'owner_uid': os.getuid(),
                                         'start_time': services.start_time(os.getpid())})
    write(directory / 'state.json', report)
    def interrupt(signum, frame):
        raise InterruptedError('Signal ' + str(signum))
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGALRM):
        signal.signal(sig, interrupt)
    signal.alarm(900)
    ready = shlex.join([sys.executable, str(HERE / 'record_scan_trace_ready.py'), str(directory), session])
    command = ['nsys', 'profile', '--trace=cuda,nvtx', '--cuda-trace-scope=process-tree',
               '--sample=none', '--cpuctxsw=none', '--duration=240', '--stop-on-exit=true',
               '--kill=none', '--session-new=' + session, '--output=' + str(directory / artifact),
               '--after-collection-start=' + ready,
               sys.executable, str(HERE / 'run_data_path_trace_workload.py'),
               '--work', str(work), '--thread-id', a.thread_id, '--output', str(directory),
               '--variant', a.variant, '--query', str(a.query)]
    profiler, profiler_record = services.launch(command, directory / 'nsys.log', dict(os.environ))
    report['profiler'] = profiler_record
    report['profiler_command'] = command
    write(directory / 'profiler-service.json', [profiler_record])
    write(directory / 'state.json', report)
    stopped = False
    def stop_profiler():
        nonlocal stopped
        if stopped:
            return
        if profiler.poll() is not None:
            report['nsys_exit'] = profiler.returncode
            stopped = True
            return
        with (directory / 'nsys-stop.log').open('a') as log:
            stop = subprocess.run(['nsys', 'stop', '--session=' + session], stdout=log,
                                  stderr=subprocess.STDOUT, timeout=180)
            report['nsys_stop_exit'] = stop.returncode
            shutdown = subprocess.run(['nsys', 'shutdown', '--session=' + session, '--kill=none'],
                                      stdout=log, stderr=subprocess.STDOUT, timeout=60)
            report['nsys_shutdown_exit'] = shutdown.returncode
        report['nsys_exit'] = profiler.wait(timeout=60)
        stopped = True
    try:
        deadline = time.monotonic() + 45
        while not (directory / 'nsys-ready.json').exists():
            assert profiler.poll() is None, 'Profiler exited before the collection-start callback'
            assert time.monotonic() < deadline, 'Profiler collection did not start'
            time.sleep(.5)
        report['status'] = 'running'
        write(directory / 'state.json', report)
        deadline = time.monotonic() + 600
        while not (directory / 'workload.json').exists():
            assert profiler.poll() is None, 'Profiler exited before workload evidence'
            assert time.monotonic() < deadline, 'Profiled workload did not finish'
            space = os.statvfs(directory)
            assert space.f_bavail * space.f_frsize > 20 * 2**30, 'Profiler storage reserve exhausted'
            controller = directory / 'workload-controller.json'
            if controller.exists():
                record = read(controller)
                stat = Path('/proc') / str(record['pid']) / 'stat'
                assert stat.exists() and services.start_time(record['pid']) == record['start_time'], 'Workload controller exited without a receipt'
            time.sleep(.5)
        workload = read(directory / 'workload.json')
        result = workload['result']
        report['run'] = workload['run']
        report['result'] = {k: result[k] for k in ('status', 'driver_exit_code', 'samples')}
        assert result['status'] == 'completed' and result['driver_exit_code'] == 0
        assert len(result['samples']) == 2 and all(s['validation']['status'] == 'passed' for s in result['samples'])
        assert (workload['end_unix_ns'] - read(directory / 'nsys-ready.json')['time_unix_ns']) / 1e9 < 240
        stop_profiler()
        assert report['nsys_exit'] == 0
        trace = directory / (artifact + '.nsys-rep')
        assert trace.exists() and trace.stat().st_size > 0
        with (directory / 'export.log').open('x') as log:
            subprocess.run(['nsys', 'export', '--type=sqlite', '--output=' + str(directory / (artifact + '.sqlite')),
                            str(trace)], stdout=log, stderr=subprocess.STDOUT, check=True, timeout=180)
        report['status'] = 'completed'
    except BaseException as error:
        report.update(status='failed', error=repr(error))
        raise
    finally:
        signal.alarm(0)
        controller = directory / 'workload-controller.json'
        if controller.exists():
            record = read(controller)
            stat = Path('/proc') / str(record['pid']) / 'stat'
            if stat.exists() and services.start_time(record['pid']) == record['start_time']:
                assert stat.parent.stat().st_uid == record['owner_uid'] == os.getuid()
                os.kill(record['pid'], signal.SIGTERM)
        run = work / ('screen/data-path-trace-' + label + suffix + '-0')
        services.stop(run / 'applications/suite/driver-service.json')
        for path in (run / 'services').glob('*/*.json'):
            if not path.name.endswith('.stopped.json'):
                services.stop(path)
        try:
            stop_profiler()
        finally:
            services.stop(directory / 'profiler-service.json')
            report['end_unix_ns'] = time.time_ns()
            write(directory / 'state.json', report)


if __name__ == '__main__':
    main()
