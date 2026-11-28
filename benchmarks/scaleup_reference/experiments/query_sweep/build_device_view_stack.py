#!/usr/bin/env python3
"""Rebuild the isolated integrated native stack with device-view staging only."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, sha, write

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', type=Path, required=True)
p.add_argument('--thread-id', required=True)
p.add_argument('--revision', required=True)
a = p.parse_args()
assert read(a.work / 'evidence/owner.json')['thread_id'] == a.thread_id
assert not subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader'], text=True).strip()
for area in ('code-profile', 'data-path-trace'):
    assert all(read(path)['status'] not in ('starting', 'running') for path in (a.work / area).glob('*/state.json'))
cache = a.work / 'native-codec-stack'
source = cache / 'cudf'
assert not subprocess.check_output(['git', '-C', str(source), 'status', '--porcelain'], text=True)
baseline = read(cache / 'baseline-state.json')
assert baseline['status'] == 'completed'
assert sha(cache / 'baseline/libcudf.so') == baseline['library_sha256']
candidate = read(cache / 'candidate-state.json')
assert candidate['status'] == 'completed'
assert sha(cache / 'candidate/libcudf.so') == candidate['library_sha256']
probe = a.work / ('device-view-probe-' + a.revision[:10])
assert (probe / 'build.exit').read_text().strip() == '0'
assert 'UNSAFE_REUSE_MUTATION_REJECTED' in (probe / 'build.log').read_text()
assert shutil.disk_usage(cache).free > 50 * 1024**3
root = a.work / 'native-view-stack'
root.mkdir()
state = {'status': 'running', 'owner_thread': a.thread_id, 'source_revision': a.revision,
         'baseline_revision': baseline['source_revision'], 'jni_revision': baseline['jni_revision'],
         'started_unix_ns': time.time_ns(), 'commands': [], 'component_probe': str(probe),
         'reused_build_configuration': baseline['configuration'], 'artifact_scope': 'integrated cuDF and JNI'}
state_path = root / 'state.json'
write(state_path, state)
try:
    changes = subprocess.check_output(['git', '-C', str(source), 'diff', '--name-only',
        baseline['source_revision'], a.revision], text=True).splitlines()
    assert set(changes) == {'cpp/src/table/table_device_view.cu', 'cpp/src/column/column_device_view.cu',
        'cpp/src/utilities/device_view_staging.hpp', 'cpp/benchmarks/device_view_staging_probe.cu',
        'cpp/benchmarks/build_device_view_staging_probe.sh'}, changes
    state['changed_files'] = changes
    subprocess.run(['git', '-C', str(source), 'checkout', '--detach', a.revision], check=True)
    for name in ('cpp-configure.log', 'cpp-build.log', 'cudfjni-configure.log', 'cudfjni-build.log',
                 'sparkjni-configure.log', 'sparkjni-build.log'):
        command = baseline['completed_steps'][name]
        invocation = next(row for row in baseline['invocations'] if row['argv'] == command)
        env = {**os.environ, **invocation['environment'], **invocation['extra_environment']}
        assert command[0] == str(a.work / 'native-codec-library/build-venv/bin/cmake')
        state['commands'].append({'name': name, 'argv': command, 'environment': invocation['environment']})
        write(state_path, state)
        with (root / name).open('x') as log:
            subprocess.run(command, cwd=cache, env=env, stdout=log, stderr=subprocess.STDOUT,
                           check=True, timeout=1800)
    artifact = root / 'lib'
    artifact.mkdir()
    for name in ('libcudf.so', 'libcudfjni.so'):
        shutil.copyfile(cache / 'spark-jni-build' / name, artifact / name)
    for name in ('libcufilejni.so', 'libnvmljni.so'):
        (artifact / name).symlink_to(a.work / 'native-concat/lib' / name)
    state['library_sha256'] = sha(artifact / 'libcudf.so')
    state['jni_stub_sha256'] = sha(artifact / 'libcudfjni.so')
    state['runtime_library_path'] = str(a.work / 'native-codec-library/compiler/lib')
    exports = subprocess.check_output(['nm', '-D', '--defined-only', str(artifact / 'libcudf.so')], text=True)
    assert 'Java_ai_rapids_cudf_HybridScanReader_materializeAllColumns' in exports
    assert 'Java_com_nvidia_spark_rapids_jni_Hash_getMaxStackDepth' in exports
    env = {**os.environ, 'LD_LIBRARY_PATH': state['runtime_library_path'],
           'CUDA_VISIBLE_DEVICES': subprocess.check_output(['nvidia-smi', '--id=0', '--query-gpu=uuid',
                '--format=csv,noheader'], text=True).strip()}
    engine = a.work / 'artifacts/decimal-null-copy/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar'
    spark = Path(read(a.work / 'evidence/deployment.json')['spark_home'])
    for mode in ('control', 'isolated'):
        command = [str(a.work / 'jdk17/bin/java'), '-Dai.rapids.cudf.lib-native-dir=' + str(artifact),
            '-cp', str(cache / 'smoke') + ':' + str(engine) + ':' + str(spark / 'jars/*'), 'NativeStackSmoke']
        with (root / (mode + '-smoke.log')).open('x') as log:
            subprocess.run(command, env={**env, 'LIBCUDF_DEVICE_VIEW_STAGING': mode},
                stdout=log, stderr=subprocess.STDOUT, check=True, timeout=120)
        assert 'NATIVE_STACK_SMOKE_PASSED' in (root / (mode + '-smoke.log')).read_text()
    assert sha(cache / 'baseline/libcudf.so') == baseline['library_sha256']
    assert sha(cache / 'candidate/libcudf.so') == candidate['library_sha256']
    assert not subprocess.check_output(['git', '-C', str(source), 'status', '--porcelain'], text=True)
    state['status'] = 'completed'
except BaseException as error:
    state.update(status='failed', error=repr(error))
    raise
finally:
    state['finished_unix_ns'] = time.time_ns()
    write(state_path, state)
print(json.dumps(state), flush=True)
