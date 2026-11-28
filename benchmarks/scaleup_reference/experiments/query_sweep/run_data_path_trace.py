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
from common import read, sha, write
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
    p.add_argument('--pinned-copy-threshold', type=int, choices=(0, 65536))
    p.add_argument('--kernel-proof-window', action='store_true')
    p.add_argument('--wall-stacks', action='store_true')
    p.add_argument('--park-events', action='store_true')
    a = p.parse_args()
    assert not (a.concat_implementation and a.scan_copy_implementation)
    assert not a.kernel_proof_window or (a.timeline_only and a.pinned_copy_threshold is not None and not a.java_ranges)
    assert not a.wall_stacks or (a.kernel_proof_window and a.variant == 'code-profile')
    assert not a.park_events or (a.kernel_proof_window and a.variant == 'code-profile' and not a.wall_stacks)
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
              'full_sql_trace': not a.kernel_proof_window,
              'start_unix_ns': time.time_ns(), 'java_ranges': a.java_ranges,
              'wall_stacks': a.wall_stacks, 'park_events': a.park_events,
              'pinned_copy_threshold': a.pinned_copy_threshold}
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
    duration = 10 if a.kernel_proof_window else (420 if a.variant == 'code-profile' else 240)
    delay = 75 if a.kernel_proof_window else 0
    report['capture_delay_seconds'] = delay
    report['capture_duration_seconds'] = duration
    command = ['nsys', 'profile', '--trace=cuda' if a.kernel_proof_window else '--trace=cuda,nvtx', '--cuda-trace-scope=process-tree',
               *sampling, '--delay=' + str(delay), '--duration=' + str(duration), '--stop-on-exit=true',
               '--kill=none', '--wait=primary', '--session-new=' + session,
               '--output=' + str(directory / artifact),
               '--after-collection-start=' + ready,
               sys.executable, str(HERE / 'run_data_path_trace_workload.py'),
               '--work', str(work), '--thread-id', a.thread_id, '--output', str(directory),
               '--variant', a.variant, '--query', str(a.query)]
    if a.pinned_copy_threshold is not None:
        command += ['--pinned-copy-threshold', str(a.pinned_copy_threshold)]
    if a.wall_stacks:
        command += ['--wall-stacks']
    if a.park_events:
        command += ['--park-events']
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
        deadline = time.monotonic() + delay + 45
        while not (directory / 'nsys-ready.json').exists():
            assert profiler.poll() is None, 'Profiler exited before the collection-start callback'
            assert time.monotonic() < deadline, 'Profiler collection did not start'
            time.sleep(.5)
        report['status'] = 'running'
        write(directory / 'state.json', report)
        deadline = time.monotonic() + 600
        while not (directory / 'workload.json').exists():
            profiler_exit = profiler.poll()
            # A bounded collection can finish while --kill=none leaves the workload running.
            assert profiler_exit is None or (a.kernel_proof_window and profiler_exit == 0), \
                'Profiler failed before workload evidence: ' + str(profiler_exit)
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
        if a.wall_stacks:
            profiles = sorted(directory.glob('profile-*.jfr'))
            assert len(profiles) == 9 and all(path.stat().st_size > 1024 for path in profiles)
            report['profiles'] = [{'path': str(path), 'bytes': path.stat().st_size,
                                   'sha256': sha(path)} for path in profiles]
        if a.park_events:
            profiles = sorted(directory.glob('parks-*.jfr'))
            assert len(profiles) == 9 and all(path.stat().st_size > 1024 for path in profiles)
            report['park_profiles'] = [{'path': str(path), 'bytes': path.stat().st_size,
                                       'sha256': sha(path)} for path in profiles]
        if not a.kernel_proof_window:
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
        if a.pinned_copy_threshold is not None:
            import sqlite3
            db = sqlite3.connect('file:' + str(directory / (artifact + '.sqlite')) + '?mode=ro', uri=True)
            epoch = db.execute('select utcEpochNs from TARGET_INFO_SESSION_START_TIME').fetchone()[0]
            predicate = ' or '.join('(k.start >= ? and k.end <= ?)' for _ in result['samples'])
            bounds = [r[key] - epoch for r in result['samples']
                      for key in ('started_unix_ns', 'finished_unix_ns')]
            copies = db.execute("select k.globalPid,s.value,count(*) from CUPTI_ACTIVITY_KIND_KERNEL k "
                "join StringIds s on k.demangledName=s.id where s.value like '%cudf::detail::%copy_kernel%' "
                "and (" + predicate + ") group by k.globalPid,s.value", bounds).fetchall()
            if a.kernel_proof_window:
                first, last = db.execute('select min(start),max(end) from CUPTI_ACTIVITY_KIND_KERNEL').fetchone()
                assert first is not None and last > first
                windows = [(r['iteration'], max(0, min(last+epoch,r['finished_unix_ns']) -
                    max(first+epoch,r['started_unix_ns'])) / 1e9) for r in result['samples']]
                assert any(seconds > 0 for _,seconds in windows), windows
                report['partial_kernel_window'] = {'start_unix_ns':first+epoch,
                    'end_unix_ns':last+epoch,'sql_overlap_seconds':windows}
            db.close()
            if a.pinned_copy_threshold:
                assert len({row[0] for row in copies}) == 8, copies
            else:
                assert not copies, copies
            report['pinned_copy_kernel_proof'] = copies
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
