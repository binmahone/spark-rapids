#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Build and validate an isolated shuffle copy implementation on the owned host."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import zipfile

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]/'scripts'))
from common import read, sha, write

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', type=Path, required=True)
p.add_argument('--thread-id', required=True)
a = p.parse_args()
assert read(a.work/'evidence/owner.json')['thread_id'] == a.thread_id
assert not subprocess.check_output(['nvidia-smi','--query-compute-apps=pid','--format=csv,noheader'],text=True).strip()
for area in ('code-profile','data-path-trace'):
    assert all(read(p)['status'] not in ('starting','running') for p in (a.work/area).glob('*/state.json'))
root = a.work/'native-shuffle-copy'
root.mkdir(exist_ok=False)
state = {'status': 'building', 'owner_thread': a.thread_id}
write(root/'state.json', state)
engine = a.work/'artifacts/decimal-null-copy/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar'
assert sha(engine) == 'ec38a58426e662ce99a433e806a66fa927bb011c014465a5df0b0c31ded4d1cc'
spark = a.work/'spark-venv/lib/python3.10/site-packages/pyspark/jars'
asm, = spark.glob('xbean-asm9-shaded-*.jar')
jdk = a.work/'jdk17'
commands = []
def run(command, logfile, **kwargs):
    commands.append(list(map(str, command)))
    with (root/logfile).open('x') as log:
        return subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=180, **kwargs)

try:
    classes = root/'classes'
    classes.mkdir()
    run([str(jdk/'bin/javac'),'--release','8','-cp',str(engine)+':'+str(asm),'-d',str(classes),
         str(HERE/'ShuffleCopyProbe.java'),str(HERE/'ShuffleCopyProbeTest.java')], 'javac.log', check=True)
    agent = root/'shuffle-copy-probe.jar'
    with zipfile.ZipFile(agent,'w',compression=zipfile.ZIP_DEFLATED) as jar:
        jar.writestr('META-INF/MANIFEST.MF',
            'Manifest-Version: 1.0\nPremain-Class: com.nvidia.spark.rapids.experiments.ShuffleCopyProbe\n\n')
        for path in classes.rglob('*.class'):
            jar.write(path,str(path.relative_to(classes)))
        with zipfile.ZipFile(asm) as dependency:
            for name in dependency.namelist():
                if name.startswith('org/apache/xbean/asm9/') and name.endswith('.class'):
                    jar.writestr(name,dependency.read(name))
    base = ['/usr/local/cuda-13.1/bin/nvcc','-std=c++17','-O3','-arch=sm_120','-ccbin','g++-12',
        '--default-stream','per-thread','-shared','-Xcompiler','-fPIC',
        '-I'+str(jdk/'include'),'-I'+str(jdk/'include/linux'),str(HERE/'shuffle_copy_probe.cu')]
    for suffix, flags in (('',[]),('-noop',['-DSHUFFLE_COPY_PROBE_TEST_NOOP'])):
        run(base+flags+['-o',str(root/('libshuffle_copy_probe'+suffix+'.so'))],
            'nvcc'+suffix+'.log',check=True)
    env = dict(os.environ, CUDA_VISIBLE_DEVICES=subprocess.check_output(
        ['nvidia-smi','--id=0','--query-gpu=uuid','--format=csv,noheader'],text=True).strip())
    for label, mode, suffix in (('control','control',''),('kernel','kernel',''),('noop','kernel','-noop')):
        command = [str(jdk/'bin/java'),'-Dai.rapids.cudf.lib-native-dir='+str(a.work/'native-concat/lib'),
            '-javaagent:'+str(agent)+'='+mode+','+str(root/('libshuffle_copy_probe'+suffix+'.so')),
            '-cp',str(agent)+':'+str(engine)+':'+str(spark/'*'),
            'com.nvidia.spark.rapids.experiments.ShuffleCopyProbeTest']
        result = run(command,'test-'+label+'.log',env=env)
        log = (root/('test-'+label+'.log')).read_text()
        assert log.count('SHUFFLE_COPY_TRANSFORMED class=') == 2, log[-2000:]
        if label == 'noop':
            assert result.returncode != 0 and 'SHUFFLE_COPY_BYTES' in log, log[-2000:]
        else:
            assert result.returncode == 0 and 'range rejection passed' in log, log[-2000:]
            if label == 'kernel':
                assert 'SHUFFLE_COPY_KERNEL_SUBMITTED' in log
    manifest = {'source_revision':subprocess.check_output(['git','-C',str(HERE),'rev-parse','HEAD'],text=True).strip(),
        'engine_sha256':sha(engine),'asm_jar':str(asm),'asm_sha256':sha(asm),'commands':commands,
        'sources':{name:sha(HERE/name) for name in ('ShuffleCopyProbe.java','ShuffleCopyProbeTest.java',
            'shuffle_copy_probe.cu','build_shuffle_copy_probe.py')},
        'artifacts':{name:sha(root/name) for name in ('shuffle-copy-probe.jar','libshuffle_copy_probe.so')},
        'tests':['Control and candidate verify every byte, guards, ordering, host fallback, bounds.',
                 'No-op native mutation rejected by the byte comparison.'],
        'limits':['Experimental replacement of four frozen shuffle call sites; not an upstream implementation.',
                  'Allocation, buffer ownership, stream synchronization and transport remain unchanged.',
                  'No RMM resource is created by the native library; input buffers remain owned by the caller.']}
    write(root/'manifest.json',manifest)
    state['status']='completed'
except BaseException as error:
    state.update(status='failed',error=repr(error))
    raise
finally:
    write(root/'state.json',state)
    print(json.dumps(state),flush=True)
