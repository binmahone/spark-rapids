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
    p.add_argument('--mode', choices=('control', 'stacks', 'native-io'), required=True)
    p.add_argument('--queries', default='8,18,5')
    p.add_argument('--nvcomp-policy', choices=('STABLE', 'OFF'))
    p.add_argument('--mmap-policy', choices=('ON', 'OFF'))
    p.add_argument('--pinned-copy-threshold', type=int, choices=(0, 65536))
    p.add_argument('--shuffle-copy-implementation', choices=('control', 'kernel'))
    p.add_argument('--concat-implementation', choices=('control', 'candidate'))
    p.add_argument('--scan-copy-implementation', choices=('original', 'sync', 'pair', 'paced-control', 'paced', 'paced-queued', 'serial-original', 'serial-unlocked', 'serial-locked', 'batch-control', 'batch-scalar', 'batch-paced'))
    p.add_argument('--packed-view', action='store_true')
    p.add_argument('--frozen-native-directory', action='store_true')
    p.add_argument('--codec-library', choices=('bridge','baseline','candidate'))
    p.add_argument('--codec-library-layout', choices=('standalone','integrated'), default='standalone')
    p.add_argument('--native-io-nice',type=int,choices=(0,5))
    p.add_argument('--cuda-wait-policy',choices=('auto','blocking'))
    p.add_argument('--ucx-preserve-context',action='store_true')
    p.add_argument('--queue-index-probe',choices=('control','candidate'))
    a = p.parse_args()
    if a.queue_index_probe:
        assert a.mode in ('control','stacks') and a.frozen_native_directory and a.pinned_copy_threshold==0
        assert not (a.cuda_wait_policy or a.codec_library or a.mmap_policy or a.nvcomp_policy)
        assert a.native_io_nice is None
    assert not a.ucx_preserve_context or a.cuda_wait_policy
    if a.native_io_nice is not None:
        assert a.mode=='control' and a.frozen_native_directory and a.pinned_copy_threshold==0
        assert not (a.codec_library or a.mmap_policy or a.nvcomp_policy)
    if a.cuda_wait_policy:
        assert a.mode=='control' and a.frozen_native_directory and a.pinned_copy_threshold==0
        assert a.native_io_nice is None and not (a.codec_library or a.mmap_policy or a.nvcomp_policy)
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
    if a.frozen_native_directory:
        assert not (a.packed_view or a.concat_implementation or a.scan_copy_implementation or a.shuffle_copy_implementation)
        native = a.work / 'native-concat/lib'
        assert sha(native / 'libcudf.so') == '8a07e08432d9afbe80d1db24c69ce67e9a05048c703f7d65a1514dfc5620e9a3'
        options = (options + ' ' if options else '') + f'-Dai.rapids.cudf.lib-native-dir={native}'
    queue_probe=None
    if a.queue_index_probe:
        queue_root=a.work/'queue-index-probe'
        manifest=read(queue_root/'state.json');assert manifest['status']=='completed'
        assert sha(queue_root/'queue-index-probe.jar')==manifest['agent_sha256']
        options += f' -javaagent:{queue_root}/queue-index-probe.jar={a.queue_index_probe}'
        queue_probe={'mode':a.queue_index_probe,'manifest':manifest}
    wait_policy=None
    if a.cuda_wait_policy:
        policy_root=a.work/'native-cuda-wait-policy-v2'
        manifest=read(policy_root/'state.json');assert manifest['status']=='completed'
        assert all(sha(policy_root/name)==digest for name,digest in manifest['artifacts'].items())
        options += f' -agentpath:{policy_root}/libcuda_wait_policy.so={a.cuda_wait_policy}'
        wait_policy={'mode':a.cuda_wait_policy,'manifest':manifest}
    ucx_patch=None
    if a.ucx_preserve_context:
        ucx_root=a.work/'native-ucx-context-flags-v2'
        ucx_patch=read(ucx_root/'state.json');assert ucx_patch['status']=='completed'
        ucx_module=ucx_root/'modules/libuct_cuda.so'
        assert sha(ucx_module)==ucx_patch['module_sha256']
        wait_policy['ucx_patch']=ucx_patch
    codec_library = None
    diagnostic_library_path = None
    if a.codec_library:
        assert not (a.frozen_native_directory or a.packed_view or a.concat_implementation or
                    a.scan_copy_implementation or a.shuffle_copy_implementation or a.nvcomp_policy or
                    a.mmap_policy) and a.pinned_copy_threshold == 0
        build = a.work / ('native-codec-stack' if a.codec_library_layout == 'integrated' else 'native-codec-library')
        if a.codec_library == 'bridge':
            native = a.work / 'native-concat/lib'
            codec_library = {'status':'completed','action':'bridge','library_sha256':
                '8a07e08432d9afbe80d1db24c69ce67e9a05048c703f7d65a1514dfc5620e9a3'}
        else:
            codec_library = read(build / (a.codec_library + '-state.json'))
            assert codec_library['status'] == 'completed'
            native = build / a.codec_library
        assert sha(native / 'libcudf.so') == codec_library['library_sha256']
        codec_library['layout'] = a.codec_library_layout
        diagnostic_library_path = a.work / 'native-codec-library/compiler/lib'
        codec_library['runtime_dependencies'] = {
            path.name: sha(path) for path in diagnostic_library_path.glob('*.so.*') if path.is_file()}
        options = (options + ' ' if options else '') + f'-Dai.rapids.cudf.lib-native-dir={native}'
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
        native = a.work / ('native-scan-copy-batch' if a.scan_copy_implementation.startswith('batch-') else
                           'native-scan-copy-serial' if a.scan_copy_implementation.startswith('serial-') else
                           'native-scan-copy-paced' if a.scan_copy_implementation.startswith('paced') else 'native-scan-copy')
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
    shuffle_copy = None
    if a.shuffle_copy_implementation:
        assert not (a.concat_implementation or a.scan_copy_implementation or a.packed_view)
        assert a.pinned_copy_threshold == 0 and not (a.mmap_policy or a.nvcomp_policy)
        native = a.work / 'native-shuffle-copy'
        assert read(native / 'state.json')['status'] == 'completed'
        manifest = read(native / 'manifest.json')
        for name, digest in manifest['artifacts'].items():
            assert sha(native / name) == digest
        shuffle_copy = {'implementation': a.shuffle_copy_implementation, 'manifest': manifest}
        agent = (f'-Dai.rapids.cudf.lib-native-dir={a.work}/native-concat/lib '
                 f'-javaagent:{native}/shuffle-copy-probe.jar={a.shuffle_copy_implementation},'
                 f'{native}/libshuffle_copy_probe.so')
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
             'shuffle_copy_probe': shuffle_copy,
             'packed_view': a.packed_view,
             'frozen_native_directory': a.frozen_native_directory,
             'codec_library': codec_library,
             'cuda_wait_policy':wait_policy,
             'queue_index_probe':queue_probe,
             'pinned_copy_threshold': a.pinned_copy_threshold,
             'queries': queries, 'nvcomp_policy': a.nvcomp_policy, 'mmap_policy': a.mmap_policy,
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
        observers = [
            ('host', HERE.parent / 'runtime_prefilter/monitor_host.py', []),
            ('vm', HERE / 'monitor_vm.py', ['--thread-id', a.thread_id, '--controller', str(directory / 'controller.json')]),
            ('pool', HERE / 'monitor_read_pool.py', [])]
        if codec_library or a.native_io_nice is not None or wait_policy or queue_probe:
            observers.append(('native-modules', HERE / 'monitor_native_modules.py',
                              ['--expected-directory', str(native),
                               '--controller', str(directory / 'controller.json'),
                               *(['--expected-ucx-module',str(ucx_module)] if ucx_patch else [])]))
        if a.mode == 'native-io':
            observers.append(('native-io', HERE / 'monitor_native_io.py',
                ['--thread-id', a.thread_id, '--scratch', str(scratch),
                 '--controller', str(directory / 'controller.json')]))
        if a.native_io_nice is not None:
            observers.append(('io-priority',HERE/'monitor_native_io_priority.py',
                ['--controller',str(directory/'controller.json'),'--label',a.label,
                 '--nice',str(a.native_io_nice)]))
        for name, script, extra in observers:
            log = (directory / (name + '.log')).open('x')
            process = subprocess.Popen([sys.executable, str(script), '--work', str(a.work),
                '--output', str(directory / (name + '.jsonl')), *extra], stdout=log, stderr=subprocess.STDOUT)
            children.append((process, log))
        decoder = ({'spark.executorEnv.LIBCUDF_NVCOMP_POLICY': a.nvcomp_policy}
                   if a.nvcomp_policy else {})
        if ucx_patch:
            decoder['spark.executorEnv.UCX_PLUGIN_PATH']=str(ucx_module.parent)
        if a.mmap_policy:
            decoder['spark.executorEnv.LIBCUDF_MMAP_ENABLED'] = a.mmap_policy
        if a.pinned_copy_threshold is not None:
            decoder['spark.executorEnv.LIBCUDF_KERNEL_PINNED_COPY_THRESHOLD'] = str(a.pinned_copy_threshold)
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
            diagnostic_jvm_options=options, diagnostic_library_path=diagnostic_library_path)
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
            mode = {'original': 0, 'sync': 1, 'pair': 2, 'paced-control': 0, 'paced': 3, 'paced-queued': 4, 'serial-original': 0, 'serial-unlocked': 3, 'serial-locked': 5, 'batch-control': 0, 'batch-scalar': 3, 'batch-paced': 6}[a.scan_copy_implementation]
            proof = []
            for stderr in logs.glob('worker-*/work/*/*/stderr'):
                content = stderr.read_text()
                assert 'NATIVE_PROBE_RMM_SHARED registry=' in content, str(stderr)
                assert f'SCAN_COPY_NATIVE_ENTER mode={mode}' in content
                if mode in (1, 2):
                    assert f'SCAN_COPY_ADMITTED mode={mode}' in content
                    assert f'SCAN_COPY_COMPLETED mode={mode}' in content
                if mode in (3, 4, 5, 6):
                    branch = re.search(rf'SCAN_COPY_PACED_SUBMITTED mode={mode} bytes=(\d+) chunks=(\d+)', content)
                    assert branch and int(branch[1]) > 32 * 1024 * 1024 and int(branch[2]) > 1
                if mode == 5:
                    assert 'SCAN_COPY_SERIAL_ACQUIRED mode=5' in content
                    assert 'SCAN_COPY_SERIAL_CONTENDED mode=5' in content
                proof.append({'stderr': str(stderr), 'mode': mode,
                              'large_copy_completed': bool(mode)})
            assert len(proof) == 8
            scan_copy['runtime_proof'] = proof
        if shuffle_copy:
            logs = Path(read(a.work / 'evidence/deployment.json')['scratch_root']) / run.name / 'suite'
            proof = []
            for stderr in logs.glob('worker-*/work/*/*/stderr'):
                content = stderr.read_text()
                for name, count in (('BufferSendState', 1), ('BufferReceiveState', 3)):
                    assert f'SHUFFLE_COPY_TRANSFORMED class=com/nvidia/spark/rapids/shuffle/{name} calls={count}' in content
                for side in ('send', 'receive'):
                    assert f'SHUFFLE_COPY_SUBMITTED side={side} mode={a.shuffle_copy_implementation}' in content
                executed = 'SHUFFLE_COPY_KERNEL_SUBMITTED bytes=' in content
                assert executed == (a.shuffle_copy_implementation == 'kernel')
                proof.append({'stderr': str(stderr), 'kernel_submitted': executed})
            assert len(proof) == 8
            shuffle_copy['runtime_proof'] = proof
        assert all(child.poll() is None for child, _ in children), 'An observer exited during the workload'
        pools = {}
        decoder_policies = {}
        pinned_copy_policies = {}
        mmap_observations = {}
        for line in (directory / 'pool.jsonl').read_text().splitlines():
            for executor in json.loads(line)['executors']:
                pools.setdefault(str(executor['pid']), set()).update(executor['thread_names'])
                decoder_policies.setdefault(str(executor['pid']), set()).add(
                    executor.get('nvcomp_policy'))
                pinned_copy_policies.setdefault(str(executor['pid']), set()).add(executor.get('pinned_copy_threshold'))
                mmap_observations.setdefault(str(executor['pid']), []).append({
                    'policy': executor.get('mmap_policy'),
                    'dataset_mappings': executor.get('dataset_mappings', 0)})
        assert len(pools) == 8 and all(names == {'kvikio-' + str(i) for i in range(32)}
                                     for names in pools.values())
        if a.pinned_copy_threshold is not None:
            assert len(pinned_copy_policies) == 8
            assert all(v == {str(a.pinned_copy_threshold)} for v in pinned_copy_policies.values())
            state['pinned_copy_environment_proof'] = {k: list(v) for k,v in pinned_copy_policies.items()}
        state['native_threads'] = {pid: sorted(names) for pid, names in pools.items()}
        if queue_probe:
            logs=Path(read(a.work/'evidence/deployment.json')['scratch_root'])/run.name/'suite'
            proof=[]
            for stderr in logs.glob('worker-*/work/*/*/stderr'):
                content=stderr.read_text()
                assert f'QUEUE_INDEX_TRANSFORMED mode={a.queue_index_probe} class=com/nvidia/spark/rapids/HashedPriorityQueue' in content
                assert f'QUEUE_INDEX_EXECUTED mode={a.queue_index_probe} caller=UCXShuffleTransport item=PendingTransferRequest' in content
                assert 'QUEUE_INDEX_REJECTED' not in content
                proof.append(str(stderr))
            assert len(proof)==8
            queue_probe['runtime_proof']=proof
            mappings=[json.loads(line) for line in (directory/'native-modules.jsonl').read_text().splitlines()]
            assert len(mappings)==8 and {str(row['pid']) for row in mappings}==set(pools)
            queue_probe['runtime_module_mappings']=mappings
        if wait_policy:
            logs=Path(read(a.work/'evidence/deployment.json')['scratch_root'])/run.name/'suite'
            proof=[]
            for stderr in logs.glob('worker-*/work/*/*/stderr'):
                content=stderr.read_text()
                assert f'CUDA_WAIT_POLICY_CONFIGURED mode={a.cuda_wait_policy}' in content
                assert 'CUDA_WAIT_POLICY_BOUND ai/rapids/cudf/Cuda.streamSynchronize(J)V' in content
                executed=re.search(r'CUDA_WAIT_POLICY_EXECUTED flags=(\d+) context=(\S+) stream=(\d+)',content)
                assert executed and int(executed[1])&7==(4 if a.cuda_wait_policy=='blocking' else 0)
                assert 'CUDA_WAIT_POLICY_FAILED' not in content
                proof.append({'stderr':str(stderr),'flags':int(executed[1]),'context':executed[2]})
            assert len(proof)==8
            wait_policy['runtime_proof']=proof
            mappings=[json.loads(line) for line in (directory/'native-modules.jsonl').read_text().splitlines()]
            assert len(mappings)==8 and {str(row['pid']) for row in mappings}==set(pools)
            wait_policy['runtime_module_mappings']=mappings
            if ucx_patch:
                assert all(row['expected_ucx_module']==str(ucx_module) for row in mappings)
                assert sha(ucx_module)==ucx_patch['module_sha256']
        if a.native_io_nice is not None:
            records=[json.loads(line) for line in (directory/'io-priority.jsonl').read_text().splitlines()]
            io=[r for r in records if r['role']=='native_io']
            task=[r for r in records if r['role']=='task']
            assert len(io)==256 and {str(r['pid']) for r in io}==set(pools)
            for pid in pools:
                assert {r['name'] for r in io if str(r['pid'])==pid}==pools[pid]
                assert any(str(r['pid'])==pid for r in task)
            assert all(r['nice_after']==a.native_io_nice for r in io)
            assert all(r['nice_after']==0 for r in task)
            second=min(s['started_unix_ns'] for s in result['samples'] if s['iteration']==2)
            assert max(r['unix_ns'] for r in io)<second
            state['native_io_priority']={'nice':a.native_io_nice,'records':records,
                'applied_before_second_action':True}
            mappings=[json.loads(line) for line in (directory/'native-modules.jsonl').read_text().splitlines()]
            assert len(mappings)==8 and {str(row['pid']) for row in mappings}==set(pools)
            state['native_io_priority']['runtime_module_mappings']=mappings
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
        if a.mmap_policy:
            assert len(mmap_observations) == 8
            for observations in mmap_observations.values():
                assert all(row['policy'] == a.mmap_policy for row in observations)
                if a.mmap_policy == 'ON':
                    assert any(row['dataset_mappings'] > 0 for row in observations)
                else:
                    assert all(row['dataset_mappings'] == 0 for row in observations)
            state['mmap_runtime_proof'] = mmap_observations
        if codec_library:
            mappings = [json.loads(line) for line in (directory/'native-modules.jsonl').read_text().splitlines()]
            assert {str(row['pid']) for row in mappings} == set(pools)
            assert len(mappings) == 8
            expected_runtime = str(diagnostic_library_path.resolve())
            assert all(any(expected_runtime in line and 'libstdc++.so' in line for line in row['maps']) for row in mappings)
            codec_library['runtime_module_mappings'] = mappings
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
        if a.mode == 'native-io':
            receipt = directory / 'native-io.receipt.json'
            state['native_io'] = read(receipt) if receipt.exists() else {'status': 'missing'}
            if state['native_io']['status'] != 'completed':
                state['status'] = 'failed'
        write(directory / 'state.json', state)


if __name__ == '__main__':
    main()
