#!/usr/bin/env python3
"""Build an isolated context-policy agent and verify CPU waiting plus real cuDF JNI dispatch."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parents[1]/'scripts'))
from common import read,write,sha
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--work',type=Path,required=True)
p.add_argument('--thread-id',required=True)
a=p.parse_args()
assert read(a.work/'evidence/owner.json')['thread_id']==a.thread_id
assert not subprocess.check_output(['nvidia-smi','--query-compute-apps=pid','--format=csv,noheader'],text=True).strip()
for area in ('code-profile','data-path-trace'):
    assert all(read(path)['status'] not in ('starting','running') for path in (a.work/area).glob('*/state.json'))
root=a.work/'native-cuda-wait-policy';root.mkdir(exist_ok=False)
state={'status':'running','owner_thread':a.thread_id,'commands':[],
    'source_revision':subprocess.check_output(['git','-C',str(HERE),'rev-parse','HEAD'],text=True).strip()}
uuid=subprocess.check_output(['nvidia-smi','--id=0','--query-gpu=uuid','--format=csv,noheader'],text=True).strip()
env={**os.environ,'CUDA_VISIBLE_DEVICES':uuid,'LD_LIBRARY_PATH':str(a.work/'native-concat/lib')+
    ':/usr/local/cuda-13.1/lib64:'+os.environ.get('LD_LIBRARY_PATH','')}
def run(command,name):
    command=list(map(str,command));state['commands'].append(command);write(root/'state.json',state)
    with (root/name).open('x') as log:
        subprocess.run(command,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=180)
try:
    run(['g++-12','-std=c++17','-O2','-shared','-fPIC','-Wall','-Wextra','-Werror',
        '-I'+str(a.work/'jdk17/include'),'-I'+str(a.work/'jdk17/include/linux'),
        '-I/usr/local/cuda-13.1/include',HERE/'cuda_wait_policy_agent.cpp',
        '-L/usr/local/cuda-13.1/lib64/stubs','-lcuda','-o',root/'libcuda_wait_policy.so'],'agent-build.log')
    run(['/usr/local/cuda-13.1/bin/nvcc','-std=c++17','-O2','-arch=sm_120','-ccbin','g++-12',
        '--default-stream','per-thread',HERE/'cuda_wait_policy_probe.cu',
        '-L/usr/local/cuda-13.1/lib64/stubs','-lcuda','-o',root/'wait-policy-probe'],'probe-build.log')
    engine=a.work/'artifacts/decimal-null-copy/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar'
    assert sha(engine)=='ec38a58426e662ce99a433e806a66fa927bb011c014465a5df0b0c31ded4d1cc'
    classes=root/'classes';classes.mkdir()
    spark=Path(read(a.work/'evidence/deployment.json')['spark_home'])
    cp=str(engine)+':'+str(spark/'jars/*')
    run([a.work/'jdk17/bin/javac','--release','8','-cp',cp,'-d',classes,HERE/'CudaWaitPolicySmoke.java'],
        'smoke-build.log')
    probes={}
    for mode in ('auto','blocking'):
        run([root/'wait-policy-probe',mode],mode+'-probe.json')
        probes[mode]=read(root/(mode+'-probe.json'))
        run([a.work/'jdk17/bin/java','-agentpath:'+str(root/'libcuda_wait_policy.so')+'=smoke-'+mode,
            '-Dai.rapids.cudf.lib-native-dir='+str(a.work/'native-concat/lib'),
            '-cp',str(classes)+':'+cp,'CudaWaitPolicySmoke'],mode+'-smoke.log')
        content=(root/(mode+'-smoke.log')).read_text()
        assert 'CUDA_WAIT_POLICY_SMOKE_PASSED' in content
        assert 'CUDA_WAIT_POLICY_BOUND ai/rapids/cudf/Cuda.streamSynchronize(J)V' in content
        assert 'CUDA_WAIT_POLICY_EXECUTED flags=' in content
    assert probes['auto']['cpu_seconds']>.1
    assert probes['blocking']['cpu_seconds']<probes['auto']['cpu_seconds']*.25
    assert all(.18<v['wall_seconds']<.3 and v['value_correct'] for v in probes.values())
    state.update(status='completed',micro=probes,artifacts={name:sha(root/name) for name in
        ('libcuda_wait_policy.so','wait-policy-probe','classes/CudaWaitPolicySmoke.class')})
except BaseException as error:
    state.update(status='failed',error=repr(error));raise
finally:
    state['finished_unix_ns']=time.time_ns();write(root/'state.json',state)
print(json.dumps({'status':state['status'],'micro':state.get('micro')}),flush=True)
