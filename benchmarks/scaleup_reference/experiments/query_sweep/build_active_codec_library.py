#!/usr/bin/env python3
"""Build paired libcudf libraries with the frozen dependency pins on the owned AWS host."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parents[1]/'scripts'))
from common import read,sha,write

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--work',type=Path,required=True)
p.add_argument('--thread-id',required=True)
p.add_argument('--action',choices=('configure','baseline','candidate'),required=True)
p.add_argument('--candidate-revision',required=True)
a=p.parse_args()
assert read(a.work/'evidence/owner.json')['thread_id']==a.thread_id
assert not subprocess.check_output(['nvidia-smi','--query-compute-apps=pid','--format=csv,noheader'],text=True).strip()
for area in ('code-profile','data-path-trace'):
 assert all(read(path)['status'] not in ('starting','running') for path in (a.work/area).glob('*/state.json'))
root=a.work/'native-codec-library'
root.mkdir(exist_ok=True)
state_path=root/(a.action+'-state.json')
assert not state_path.exists()
state={'status':'running','action':a.action,'owner_thread':a.thread_id,'started_unix_ns':time.time_ns(),
       'candidate_revision':a.candidate_revision,'commands':[]}
write(state_path,state)
source=root/'source';build=root/'build';venv=root/'build-venv'

def run(command,logname,timeout=900):
 command=list(map(str,command));state['commands'].append(command);write(state_path,state)
 with (root/logname).open('x') as log:
  subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=timeout,
                 env={**os.environ,'CUDACXX':'/usr/local/cuda-13.1/bin/nvcc'})

try:
 if a.action=='configure':
  assert shutil.disk_usage(root).free>80*1024**3
  component=read(a.work/('active-codec-component-'+a.candidate_revision[:10])/'manifest.json')
  assert component['mutation_rejected']
  run(['git','-C',a.work/'native-concat/cudf','worktree','add','--detach',source,
       '5a2cad06ba5ef1b77a5cc3f1fc61fc0cee5bf57c'],'worktree.log')
  run(['python3','-m','venv',venv],'venv.log')
  run([venv/'bin/pip','install','cmake==4.1.3','ninja==1.13.0'],'tools.log')
  pins=a.work/'native-concat/pins.json'
  shutil.copyfile(pins,root/'dependency-pins.json')
  state['dependency_pins_sha256']=sha(pins)
  command=[venv/'bin/cmake','-S',source/'cpp','-B',build,'-G','Ninja',
   '-DCMAKE_BUILD_TYPE=Release','-DCMAKE_C_COMPILER=gcc-12','-DCMAKE_CXX_COMPILER=g++-12',
   '-DCMAKE_CUDA_COMPILER=/usr/local/cuda-13.1/bin/nvcc','-DCMAKE_CUDA_HOST_COMPILER=g++-12',
   '-DCMAKE_MAKE_PROGRAM='+str(venv/'bin/ninja'),'-DCMAKE_CUDA_ARCHITECTURES=120',
   '-DCUDF_USE_PER_THREAD_DEFAULT_STREAM=ON','-DCUDF_BUILD_STATIC_DEPS=FORCE',
   '-DBUILD_TESTS=ON','-DBUILD_BENCHMARKS=OFF','-DCUDA_ENABLE_LINEINFO=ON',
   '-DRAPIDS_CMAKE_CPM_OVERRIDE_VERSION_FILE='+str(root/'dependency-pins.json'),
   '-Drapids-cmake-sha=8cf47a7b600985f876f428ac3c335ffd2fdeeb1d',
   '-DCMAKE_INSTALL_PREFIX='+str(root/'install')]
  run(command,'configure.log',900)
 else:
  assert read(root/'configure-state.json')['status']=='completed'
  if a.action=='candidate':
   assert read(root/'baseline-state.json')['status']=='completed'
   run(['git','-C',source,'checkout','--detach',a.candidate_revision],'candidate-checkout.log')
  assert not subprocess.check_output(['git','-C',str(source),'status','--porcelain'],text=True)
  run([venv/'bin/cmake','--build',build,'--target','cudf','-j','48'],a.action+'-build.log',3600)
  library=build/'libcudf.so'
  assert library.exists()
  artifact=root/a.action;artifact.mkdir()
  shutil.copyfile(library,artifact/'libcudf.so')
  for name in ('libcudfjni.so','libcufilejni.so','libnvmljni.so'):
   (artifact/name).symlink_to(a.work/'native-concat/lib'/name)
  run(['ldd',artifact/'libcudf.so'],a.action+'-ldd.log',60)
  assert 'not found' not in (root/(a.action+'-ldd.log')).read_text()
  state.update(source_revision=subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD'],text=True).strip(),
               library_sha256=sha(artifact/'libcudf.so'))
 state['status']='completed'
except BaseException as error:
 state.update(status='failed',error=repr(error));raise
finally:
 state['finished_unix_ns']=time.time_ns();write(state_path,state)
print(json.dumps(state),flush=True)
