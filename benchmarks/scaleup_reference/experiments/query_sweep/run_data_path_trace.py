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
    p.add_argument('--variant', choices=('control', 'scan-pack', 'scan-split', 'scan-split-cuda', 'scan-split-cuda2', 'shuffle-buffer', 'shuffle-wait', 'code-profile'), default='control')
    p.add_argument('--query', type=int, choices=(5, 8, 9, 10, 18, 20), default=8)
    p.add_argument('--attempt', type=int, default=0)
    p.add_argument('--concat-implementation', choices=('control', 'candidate'))
    p.add_argument('--scan-copy-implementation', choices=('paced-control', 'paced', 'paced-queued', 'serial-original', 'serial-unlocked', 'serial-locked', 'batch-control', 'batch-scalar', 'batch-paced'))
    p.add_argument('--timeline-only', action='store_true')
    p.add_argument('--java-ranges', action='store_true')
    a = p.parse_args()
    assert not (a.concat_implementation and a.scan_copy_implementation)
    work = a.work.resolve()
    owner = read(work / 'evidence/owner.json')
    assert owner['thread_id'] == a.thread_id
    assert (work / 'evidence/prepare.exit').read_text().strip() == '0'
    build_record = work / 'evidence/shuffle-wait-build-service.json'
    if build_record.exists():
        assert not any(services.group_members(r) for r in read(build_record)), 'Build must finish before measurement'
    native_build = work / 'evidence/native-concat-build-service.json'
    if native_build.exists():
        assert not any(services.group_members(r) for r in read(native_build))
    assert not subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader'], text=True).strip()
    suffix = '-' + a.variant
    assert a.attempt >= 0
    attempt = '-attempt' + str(a.attempt) if a.attempt else ''
    label = 'Q' + str(a.query) + attempt
    artifact = 'q' + str(a.query)
    directory = work / ('data-path-trace/' + label + '-process-tree' + suffix)
    if a.variant == 'code-profile':
        scratch = Path(read(work / 'evidence/deployment.json')['scratch_root']) / 'cuda-code-profiles' / label
        scratch.mkdir(parents=True, exist_ok=False)
        assert subprocess.check_output(['findmnt', '-no', 'TARGET', '-T', str(scratch)], text=True).strip() != '/'
        directory.parent.mkdir(exist_ok=True)
        directory.symlink_to(scratch, target_is_directory=True)
    else:
        directory.mkdir(parents=True, exist_ok=False)
    space = os.statvfs(directory)
    assert space.f_bavail * space.f_frsize > 15 * 2**30
    session = 'data-path-' + a.thread_id + '-' + artifact + '-tree' + suffix + attempt
    report = {'status': 'starting', 'owner': owner, 'classification': 'Diagnostic profiling; excluded from SOL scores',
              'nsys_session': session, 'source_revision': subprocess.check_output(
                  ['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip(),
              'start_unix_ns': time.time_ns(), 'java_ranges': a.java_ranges}
    write(directory / 'controller.json', {'pid': os.getpid(), 'owner_uid': os.getuid(),
                                         'start_time': services.start_time(os.getpid())})
    write(directory / 'state.json', report)
    def interrupt(signum, frame):
        raise InterruptedError('Signal ' + str(signum))
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGALRM):
        signal.signal(sig, interrupt)
    signal.alarm(1500)
    ready = shlex.join([sys.executable, str(HERE / 'record_scan_trace_ready.py'), str(directory), session])
    sampling = (['--sample=process-tree', '--cpuctxsw=process-tree', '--backtrace=dwarf',
                 '--sampling-period=8000000', '--discard-environment=true',
                 '--cudabacktrace=sync:100000,memory:100000'] if a.variant == 'code-profile' and not a.timeline_only
                else ['--sample=none', '--cpuctxsw=none'])
    duration = 420 if a.variant == 'code-profile' else 240
    command = ['nsys', 'profile', '--trace=cuda,nvtx', '--cuda-trace-scope=process-tree',
               *sampling, '--duration=' + str(duration), '--stop-on-exit=true',
               '--kill=none', '--wait=primary', '--session-new=' + session,
               '--output=' + str(directory / artifact),
               '--after-collection-start=' + ready,
               sys.executable, str(HERE / 'run_data_path_trace_workload.py'),
               '--work', str(work), '--thread-id', a.thread_id, '--output', str(directory),
               '--variant', a.variant, '--query', str(a.query)]
    if a.java_ranges:
        command += ['--java-ranges']
    if a.concat_implementation:
        assert a.variant == 'code-profile'
        command += ['--concat-implementation', a.concat_implementation]
    if a.scan_copy_implementation:
        assert a.variant == 'code-profile'
        command += ['--scan-copy-implementation', a.scan_copy_implementation]
    env = dict(os.environ)
    if a.variant == 'code-profile':
        temporary = directory / 'tmp'
        temporary.mkdir()
        env['TMPDIR'] = str(temporary.resolve())
        report['profiler_tmpdir'] = env['TMPDIR']
    profiler, profiler_record = services.launch(command, directory / 'nsys.log', env)
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
        if (directory / 'workload.json').exists():
            try:
                # CPU stacks can take several minutes to drain after the collection ends.
                report['nsys_exit'] = profiler.wait(timeout=600)
                stopped = True
                return
            except subprocess.TimeoutExpired:
                pass
        with (directory / 'nsys-stop.log').open('a') as log:
            stop = subprocess.run(['nsys', 'stop', '--session=' + session], stdout=log,
                                  stderr=subprocess.STDOUT, timeout=180)
            report['nsys_stop_exit'] = stop.returncode
            try:
                report['nsys_exit'] = profiler.wait(timeout=180)
            except subprocess.TimeoutExpired:
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
            root_space = os.statvfs('/')
            assert root_space.f_bavail * root_space.f_frsize > 20 * 2**30, 'Root storage reserve exhausted'
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
        assert (workload['end_unix_ns'] - read(directory / 'nsys-ready.json')['time_unix_ns']) / 1e9 < duration
        stop_profiler()
        assert report['nsys_exit'] == 0
        trace = directory / (artifact + '.nsys-rep')
        assert trace.exists() and trace.stat().st_size > 0
        with (directory / 'export.log').open('x') as log:
            subprocess.run(['nsys', 'export', '--type=sqlite', '--output=' + str(directory / (artifact + '.sqlite')),
                            str(trace)], stdout=log, stderr=subprocess.STDOUT, check=True, timeout=180)
        if a.java_ranges:
            import sqlite3
            db = sqlite3.connect('file:' + str(directory / (artifact + '.sqlite')) + '?mode=ro', uri=True)
            counts = db.execute("select (n.globalTid >> 24),count(*) from NVTX_EVENTS n "
                "left join StringIds s on n.textId=s.id where coalesce(n.text,s.value)="
                "'HybridScan materialize' and n.end is not null group by (n.globalTid >> 24)").fetchall()
            db.close()
            assert len(counts) == 8 and all(count > 0 for _, count in counts), counts
            report['java_reader_range_counts'] = counts
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
