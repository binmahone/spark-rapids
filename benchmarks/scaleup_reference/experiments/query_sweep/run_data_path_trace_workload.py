#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Launch one isolated query application as a child of the CUDA profiler."""
import argparse
from dataclasses import replace
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, sha, write
from critical_path_variants import controls
from run_screen import launch_group
import services

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', required=True, type=Path)
p.add_argument('--thread-id', required=True)
p.add_argument('--output', required=True, type=Path)
p.add_argument('--variant', choices=('control', 'scan-pack', 'scan-split', 'scan-split-cuda', 'scan-split-cuda2', 'shuffle-buffer', 'shuffle-wait', 'code-profile'), default='control')
p.add_argument('--query', type=int, choices=(5, 8, 9, 10, 18, 20), default=8)
p.add_argument('--concat-implementation', choices=('control', 'candidate'))
p.add_argument('--scan-copy-implementation', choices=('paced-control', 'paced', 'paced-queued', 'serial-original', 'serial-unlocked', 'serial-locked', 'batch-control', 'batch-scalar', 'batch-paced'))
p.add_argument('--java-ranges', action='store_true')
a = p.parse_args()
assert not (a.concat_implementation and a.scan_copy_implementation)
assert read(a.work / 'evidence/owner.json')['thread_id'] == a.thread_id
write(a.output / 'workload-controller.json', {'pid': os.getpid(), 'owner_uid': os.getuid(),
                                            'start_time': services.start_time(os.getpid())})
def interrupt(signum, frame):
    raise InterruptedError('Signal ' + str(signum))
for sig in (signal.SIGTERM, signal.SIGINT):
    signal.signal(sig, interrupt)
started = time.time_ns()
assert (a.work / 'evidence/prepare.exit').read_text().strip() == '0'
selected = controls()[a.query]
selected = replace(selected, overrides={**selected.overrides,
    'spark.rapids.sql.join.hashTable.reuse': 'false'})
if a.variant in ('scan-split', 'scan-split-cuda', 'scan-split-cuda2'):
    selected = replace(selected, overrides={**selected.overrides,
        'spark.sql.files.maxPartitionBytes': '1g'})
native = a.work / 'native-concat/lib'
native_options = ('-Dai.rapids.cudf.lib-native-dir=' + str(native)
                  if a.variant == 'code-profile' and native.is_dir() else None)
if a.concat_implementation:
    assert a.variant == 'code-profile' and native_options
    root = native.parent
    assert (root / 'build.exit').read_text().strip() == '0'
    subprocess.run(['sha256sum', '--check', str(root / 'agent.sha256')], check=True)
    native_options += (f' -javaagent:{root}/concat-offset-probe.jar={a.concat_implementation},'
                       f'{native}/libconcat_offsets_probe.so')
    write(a.output / 'native-probe.json', {'mode': a.concat_implementation,
        'agent_sha256': sha(root / 'concat-offset-probe.jar'),
        'library_sha256': sha(native / 'libconcat_offsets_probe.so')})
if a.scan_copy_implementation:
    assert a.variant == 'code-profile' and native_options
    root = a.work / ('native-scan-copy-batch' if a.scan_copy_implementation.startswith('batch-') else
                           'native-scan-copy-serial' if a.scan_copy_implementation.startswith('serial-')
                     else 'native-scan-copy-paced')
    assert (root / 'build.exit').read_text().strip() == '0'
    subprocess.run(['sha256sum', '--check', str(root / 'artifacts.sha256')], check=True)
    native_options += (f' -javaagent:{root}/scan-copy-probe.jar={a.scan_copy_implementation},'
                       f'{root}/libscan_copy_probe.so,{root}/gpu-pairs.properties,{root}/leases')
    write(a.output / 'native-probe.json', {'kind': 'scan-copy', 'mode': a.scan_copy_implementation,
        'agent_sha256': sha(root / 'scan-copy-probe.jar'),
        'library_sha256': sha(root / 'libscan_copy_probe.so')})
path, result = launch_group(a.work, 'data-path-trace-' + a.output.name.replace('-process-tree', ''),
    0, [selected], {
    'spark.rapids.sql.reader.kvikio.numThreads': '32',
    'spark.rapids.sql.metrics.level': 'DEBUG',
    'spark.rapids.sql.nvtx.traceTaskGpuOwnership': 'false' if a.variant.startswith('scan-split-cuda') or a.variant == 'code-profile' else 'true',
    'spark.task.resource.gpu.amount': '0.25',
    'spark.rapids.shuffle.compression.codec': 'none',
    'spark.rapids.sql.format.parquet.hybridScan.maxH2DPackSize': '128m' if a.variant == 'scan-pack' else '32m',
    'spark.rapids.shuffle.ucx.bounceBuffers.size': '16m' if a.variant == 'shuffle-buffer' else '4m'},
    experimental_engine=a.work / 'artifacts' / ('shuffle-wait' if a.variant == 'shuffle-wait' else 'decimal-null-copy'),
    runtime_hash_reuse_queries=[9] if a.query == 9 else None,
    diagnostic_nvtx=a.java_ranges or not (a.variant.startswith('scan-split-cuda') or a.variant == 'code-profile'),
    diagnostic_jvm_options=native_options)
assert result['status'] == 'completed' and result['driver_exit_code'] == 0
assert len(result['samples']) == 2 and all(s['validation']['status'] == 'passed' for s in result['samples'])
if a.scan_copy_implementation:
    logs = Path(read(a.work / 'evidence/deployment.json')['scratch_root']) / path.name / 'suite'
    mode = {'paced-control': 0, 'paced': 3, 'paced-queued': 4, 'serial-original': 0, 'serial-unlocked': 3, 'serial-locked': 5, 'batch-control': 0, 'batch-scalar': 3, 'batch-paced': 6}[a.scan_copy_implementation]
    observed = []
    for stderr in logs.glob('worker-*/work/*/*/stderr'):
        content = stderr.read_text()
        assert 'NATIVE_PROBE_RMM_SHARED registry=' in content
        assert f'SCAN_COPY_NATIVE_ENTER mode={mode}' in content
        if mode:
            assert f'SCAN_COPY_PACED_SUBMITTED mode={mode}' in content
        if mode == 5:
            assert 'SCAN_COPY_SERIAL_ACQUIRED mode=5' in content
            assert 'SCAN_COPY_SERIAL_CONTENDED mode=5' in content
        observed.append(str(stderr))
    assert len(observed) == 8
    write(a.output / 'scan-copy-runtime-proof.json', {'mode': mode, 'executor_logs': observed})
if a.query == 9:
    for sample in result['samples']:
        assert sample['runtime_hash_reuse_proof']['physical_plan_unchanged']
        assert sum(n['metrics'].get('hashTableReuses', 0) for n in sample['plan_metrics']) > 0
write(a.output / 'workload.json', {'run': str(path), 'start_unix_ns': started,
                                  'end_unix_ns': time.time_ns(), 'result': result})
