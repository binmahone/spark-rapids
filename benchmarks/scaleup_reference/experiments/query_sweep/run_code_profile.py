#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Profile frozen SQL with CPU and wall stacks, preserving unprofiled controls."""
import argparse
from dataclasses import replace
import json
import os
import re
from pathlib import Path
import signal
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, sha, write
from critical_path_variants import controls, cases
from variants import ORDER
from run_screen import launch_group
import services


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', type=Path, required=True)
    p.add_argument('--thread-id', required=True)
    p.add_argument('--label', required=True)
    p.add_argument('--mode', choices=('control', 'stacks'), required=True)
    p.add_argument('--queries', default='8,18,5')
    p.add_argument('--nvcomp-policy', choices=('STABLE', 'OFF'))
    p.add_argument('--concat-implementation', choices=('control', 'candidate'))
    p.add_argument('--scan-copy-implementation', choices=('original', 'sync', 'pair', 'paced-control', 'paced', 'paced-queued'))
    p.add_argument('--packed-view', action='store_true')
    a = p.parse_args()
    assert not a.packed_view or not (a.concat_implementation or a.scan_copy_implementation), 'Keep packed-view experiment isolated'
    assert a.label.replace('-', '').replace('_', '').isalnum()
    owner = read(a.work / 'evidence/owner.json')
    assert owner['thread_id'] == a.thread_id
    assert (a.work / 'evidence/prepare.exit').read_text().strip() == '0'
    assert not subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader'], text=True).strip()
    assert Path('/proc/sys/vm/watermark_scale_factor').read_text().strip() == '10'
    assert int(Path('/sys/kernel/mm/lru_gen/enabled').read_text().strip(), 0) == 7
    directory = a.work / 'code-profile' / a.label
    directory.mkdir(parents=True, exist_ok=False)
    scratch = Path(read(a.work / 'evidence/deployment.json')['scratch_root']) / 'code-profiles' / a.label
    scratch.mkdir(parents=True, exist_ok=False)
    assert subprocess.check_output(['findmnt', '-no', 'TARGET', '-T', str(scratch)], text=True).strip() != '/'
    options = None
    profiler = None
    if a.mode == 'stacks':
        profiler = read(a.work / 'profilers/manifest.json')
        assert sha(Path(profiler['library'])) == profiler['library_sha256']
        options = (f"-agentpath:{profiler['library']}=start,event=cpu,interval=10ms,wall=20ms,"
                   f"cstack=dwarf,jstackdepth=256,timeout=900,file={scratch}/profile-%p.jfr,log=stderr")
        if a.packed_view:
            options += f' -Xlog:class+load=info:file={scratch}/classes-%p.log'
    concat = None
    if a.concat_implementation:
        native = a.work / 'native-concat'
        assert (native / 'build.exit').read_text().strip() == '0'
        subprocess.run(['sha256sum', '--check', str(native / 'agent.sha256')], check=True)
        concat = {'implementation': a.concat_implementation,
                  'agent_sha256': sha(native / 'concat-offset-probe.jar'),
                  'library_sha256': sha(native / 'lib/libconcat_offsets_probe.so'),
                  'sdk': read(native / 'sdk-manifest.json')}
        agent = (f'-Dai.rapids.cudf.lib-native-dir={native}/lib '
                 f'-javaagent:{native}/concat-offset-probe.jar={a.concat_implementation},'
                 f'{native}/lib/libconcat_offsets_probe.so')
        options = (options + ' ' if options else '') + agent
    scan_copy = None
    if a.scan_copy_implementation:
        assert not a.concat_implementation, 'Keep native experiments isolated'
        native = a.work / ('native-scan-copy-paced' if a.scan_copy_implementation.startswith('paced') else 'native-scan-copy')
        assert (native / 'build.exit').read_text().strip() == '0'
        subprocess.run(['sha256sum', '--check', str(native / 'artifacts.sha256')], check=True)
        scan_copy = {'implementation': a.scan_copy_implementation,
                     'agent_sha256': sha(native / 'scan-copy-probe.jar'),
                     'library_sha256': sha(native / 'libscan_copy_probe.so'),
                     'manifest': read(native / 'manifest.json'),
                     'pair_mapping': read(native / 'gpu-pairs.json')}
        agent = (f'-Dai.rapids.cudf.lib-native-dir={a.work}/native-concat/lib '
                 f'-javaagent:{native}/scan-copy-probe.jar={a.scan_copy_implementation},'
                 f'{native}/libscan_copy_probe.so,{native}/gpu-pairs.properties,{native}/leases')
        options = (options + ' ' if options else '') + agent
    selected = controls()
    queries = list(ORDER) if a.queries == 'all' else [int(q) for q in a.queries.split(',')]
    assert queries and len(queries) == len(set(queries)) and set(queries) <= set(ORDER)
    if 7 in queries:
        q7 = cases()[0]
        selected[7] = replace(q7, overrides={**selected[7].overrides, **q7.overrides})
    selected = [replace(selected[q], overrides={**selected[q].overrides,
                'spark.rapids.sql.join.hashTable.reuse': 'false'}) for q in queries]
    # Validate guarded SQL prerequisites before allocating executors or running earlier queries.
    if any(case.name == 'critical_witness_int32' for case in selected):
        proof = read(a.work / 'critical/oracle.json')
        dataset = '/opt/dlami/nvme/daily-trend-hardened/dataset'
        assert proof['status'] == 'passed' and proof['dataset_bounds']['dataset'] == dataset
        records = proof['dataset_bounds']['files']
        assert {str(p) for p in Path(dataset).joinpath('lineitem').rglob('*.parquet')} == {
            r['path'] for r in records}
        for record in records:
            actual = Path(record['path']).stat()
            assert (actual.st_size, actual.st_mtime_ns) == (record['bytes'], record['mtime_ns'])
    state = {'status': 'running', 'owner': owner, 'mode': a.mode, 'profiler': profiler,
             'concat_probe': concat,
             'scan_copy_probe': scan_copy,
             'packed_view': a.packed_view,
             'queries': queries, 'nvcomp_policy': a.nvcomp_policy,
             'scratch': str(scratch), 'started_unix_ns': time.time_ns(),
             'classification': 'Code-path diagnosis; profiler times are not benchmark scores',
             'source_revision': subprocess.check_output(['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip()}
    write(directory / 'controller.json', {'pid': os.getpid(), 'start_time': services.start_time(os.getpid()), 'owner_uid': os.getuid()})
    write(directory / 'state.json', state)
    def interrupt(signum, frame):
        raise InterruptedError('Signal ' + str(signum))
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGALRM):
        signal.signal(sig, interrupt)
    signal.alarm(3600 if a.queries == 'all' else 1200)
    children = []
    try:
        for name, script, extra in [
            ('host', HERE.parent / 'runtime_prefilter/monitor_host.py', []),
            ('vm', HERE / 'monitor_vm.py', ['--thread-id', a.thread_id, '--controller', str(directory / 'controller.json')]),
            ('pool', HERE / 'monitor_read_pool.py', [])]:
            log = (directory / (name + '.log')).open('x')
            process = subprocess.Popen([sys.executable, str(script), '--work', str(a.work),
                '--output', str(directory / (name + '.jsonl')), *extra], stdout=log, stderr=subprocess.STDOUT)
            children.append((process, log))
        decoder = ({'spark.executorEnv.LIBCUDF_NVCOMP_POLICY': a.nvcomp_policy}
                   if a.nvcomp_policy else {})
        run, result = launch_group(a.work, 'code-profile-' + a.label, 0, selected, {
            **decoder,
            'spark.rapids.sql.reader.kvikio.numThreads': '32',
            'spark.rapids.sql.metrics.level': 'DEBUG',
            'spark.rapids.shuffle.compression.codec': 'none',
            'spark.task.resource.gpu.amount': '0.25',
            'spark.rapids.sql.format.parquet.hybridScan.maxH2DPackSize': '32m',
            'spark.rapids.shuffle.ucx.bounceBuffers.size': '4m'},
            experimental_engine=a.work / ('artifacts/packed-view' if a.packed_view else 'artifacts/decimal-null-copy'),
            runtime_hash_reuse_queries=[9] if 9 in queries else None,
            diagnostic_jvm_options=options)
        state.update(run=str(run), result=result)
        assert result['status'] == 'completed' and result['driver_exit_code'] == 0
        assert len(result['samples']) == 2 * len(queries)
        assert all(s['validation']['status'] == 'passed' for s in result['samples'])
        for sample in result['samples']:
            if sample['query'] == 9:
                assert sample['runtime_hash_reuse_proof']['physical_plan_unchanged']
                assert sum(n['metrics'].get('hashTableReuses', 0)
                           for n in sample['plan_metrics']) > 0
        if concat:
            logs = Path(read(a.work / 'evidence/deployment.json')['scratch_root']) / run.name / 'suite'
            proof = []
            for stderr in logs.glob('worker-*/work/*/*/stderr'):
                content = stderr.read_text()
                assert 'NATIVE_PROBE_RMM_SHARED registry=' in content, str(stderr)
                first_call = re.search(r'CONCAT_OFFSET_PROBE_FIRST_CALL mode=(\w+)', content)
                assert first_call and first_call[1] == a.concat_implementation
                first_string = 'CONCAT_OFFSET_PROBE_FIRST_STRING mode=candidate' in content
                if a.concat_implementation == 'candidate' and 5 in queries:
                    assert first_string, 'Candidate string branch not observed: ' + str(stderr)
                # Worker termination can bypass JVM shutdown hooks; first dispatches are synchronous.
                match = re.search(r'CONCAT_OFFSET_PROBE_COUNTS mode=(\w+) calls=(\d+) '
                    r'candidate_string_columns=(\d+) input_tables=(\d+)', content)
                proof.append({'stderr': str(stderr), 'first_native_call': first_call[1],
                    'first_candidate_string_call': first_string,
                    'shutdown_counters': list(match.groups()) if match else None})
            assert len(proof) == 8
            concat['runtime_proof'] = proof
        if scan_copy:
            logs = Path(read(a.work / 'evidence/deployment.json')['scratch_root']) / run.name / 'suite'
            mode = {'original': 0, 'sync': 1, 'pair': 2, 'paced-control': 0, 'paced': 3, 'paced-queued': 4}[a.scan_copy_implementation]
            proof = []
            for stderr in logs.glob('worker-*/work/*/*/stderr'):
                content = stderr.read_text()
                assert 'NATIVE_PROBE_RMM_SHARED registry=' in content, str(stderr)
                assert f'SCAN_COPY_NATIVE_ENTER mode={mode}' in content
                if mode in (1, 2):
                    assert f'SCAN_COPY_ADMITTED mode={mode}' in content
                    assert f'SCAN_COPY_COMPLETED mode={mode}' in content
                if mode in (3, 4):
                    branch = re.search(rf'SCAN_COPY_PACED_SUBMITTED mode={mode} bytes=(\d+) chunks=(\d+)', content)
                    assert branch and int(branch[1]) > 32 * 1024 * 1024 and int(branch[2]) > 1
                proof.append({'stderr': str(stderr), 'mode': mode,
                              'large_copy_completed': bool(mode)})
            assert len(proof) == 8
            scan_copy['runtime_proof'] = proof
        assert all(child.poll() is None for child, _ in children), 'An observer exited during the workload'
        pools = {}
        decoder_policies = {}
        for line in (directory / 'pool.jsonl').read_text().splitlines():
            for executor in json.loads(line)['executors']:
                pools.setdefault(str(executor['pid']), set()).update(executor['thread_names'])
                decoder_policies.setdefault(str(executor['pid']), set()).add(
                    executor.get('nvcomp_policy'))
        assert len(pools) == 8 and all(names == {'kvikio-' + str(i) for i in range(32)}
                                     for names in pools.values())
        state['native_threads'] = {pid: sorted(names) for pid, names in pools.items()}
        if a.packed_view and a.mode == 'stacks':
            loaded = {}
            expected = str(a.work / 'artifacts/packed-view/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar')
            for pid in pools:
                lines = (scratch / ('classes-' + pid + '.log')).read_text().splitlines()
                matches = [line for line in lines if 'ai.rapids.cudf.ColumnVector$OffHeapState source:' in line]
                assert len(matches) == 1 and expected in matches[0], (pid, matches)
                loaded[pid] = matches[0]
            state['packed_view_loaded_classes'] = loaded
        if a.nvcomp_policy:
            assert len(decoder_policies) == 8
            assert all(values == {a.nvcomp_policy} for values in decoder_policies.values())
        state['observed_nvcomp_policies'] = {pid: list(v) for pid, v in decoder_policies.items()}
        profiles = sorted(scratch.glob('*.jfr'))
        if a.mode == 'stacks':
            assert len(profiles) == 9 and all(p.stat().st_size > 1024 for p in profiles)
        state['profiles'] = [{'path': str(p), 'bytes': p.stat().st_size, 'sha256': sha(p)} for p in profiles]
        state['status'] = 'completed'
    except BaseException as error:
        state.update(status='failed', error=repr(error))
        raise
    finally:
        signal.alarm(0)
        for child, log in children:
            if child.poll() is None:
                child.terminate()
            if child.wait(timeout=30) != 0:
                state['status'] = 'failed'
            log.close()
        state['finished_unix_ns'] = time.time_ns()
        write(directory / 'state.json', state)


if __name__ == '__main__':
    main()
