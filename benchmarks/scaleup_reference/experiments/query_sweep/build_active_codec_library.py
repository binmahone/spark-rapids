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
import urllib.request

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parents[1]/'scripts'))
from common import read,sha,write

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--work',type=Path,required=True)
p.add_argument('--thread-id',required=True)
p.add_argument('--action',choices=('configure','reconfigure','configure-toolchain','configure-prefix','configure-headers','baseline','candidate'),required=True)
p.add_argument('--candidate-revision',required=True)
a=p.parse_args()
assert read(a.work/'evidence/owner.json')['thread_id']==a.thread_id
assert not subprocess.check_output(['nvidia-smi','--query-compute-apps=pid','--format=csv,noheader'],text=True).strip()
for area in ('code-profile','data-path-trace'):
 assert all(read(path)['status'] not in ('starting','running') for path in (a.work/area).glob('*/state.json'))
root=a.work/'native-codec-library'
root.mkdir(exist_ok=True)
state_path=root/(a.action+'-state.json')
if state_path.exists():
 assert a.action=='baseline' and read(state_path)['status']=='failed'
 failure='baseline-failed-'+str(time.time_ns())
 shutil.copyfile(state_path,root/(failure+'-state.json'))
 (root/'baseline-build.log').rename(root/(failure+'-build.log'))
state={'status':'running','action':a.action,'owner_thread':a.thread_id,'started_unix_ns':time.time_ns(),
       'candidate_revision':a.candidate_revision,'commands':[]}
write(state_path,state)
source=root/'source';build=root/'build';venv=root/'build-venv'

def run(command,logname,timeout=900):
 command=list(map(str,command));state['commands'].append(command);write(state_path,state)
 with (root/logname).open('x') as log:
  subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=timeout,
                 env={**os.environ,'CUDACXX':'/usr/local/cuda-13.1/bin/nvcc',
                      'PKG_CONFIG_LIBDIR':str(root/'compiler/lib/pkgconfig')+':'+str(root/'compiler/share/pkgconfig')})

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
 elif a.action=='configure-toolchain':
  assert read(root/'baseline-state.json')['status']=='failed'
  executable=root/'micromamba'
  with urllib.request.urlopen('https://github.com/mamba-org/micromamba-releases/releases/download/2.10.0-0/micromamba-linux-64',timeout=60) as response:
   executable.write_bytes(response.read())
  assert sha(executable)=='701f39c06f8e6a670a0e186c8479b9e5c0502a2f0cda6cef65d83bdd2875e8bd'
  executable.chmod(0o755)
  compiler=root/'compiler'
  run([executable,'create','--yes','--no-rc','--root-prefix',root/'mamba-root','--prefix',compiler,
       '--channel','conda-forge','gxx_linux-64=14.3.0','libstdcxx=14.3.0','libgcc=14.3.0'],
      'compiler-install.log',600)
  run([executable,'list','--no-rc','--prefix',compiler,'--explicit'],'compiler-explicit.txt',60)
  build.rename(root/'build-gcc12')
  command=read(root/'configure-state.json')['commands'][-1]
  command=[arg.replace('=gcc-12','='+str(compiler/'bin/x86_64-conda-linux-gnu-gcc')).replace(
      '=g++-12','='+str(compiler/'bin/x86_64-conda-linux-gnu-g++')) for arg in command]
  run(command,'configure-gcc14.log',900)
  prior=read(root/'configure-state.json');prior.pop('error',None)
  prior.update(status='completed',commands=[command],compiler_state=str(state_path),finished_unix_ns=time.time_ns())
  write(root/'configure-state.json',prior)
 elif a.action=='configure-prefix':
  assert read(root/'configure-toolchain-state.json')['status']=='failed'
  compiler=root/'compiler'
  run([root/'micromamba','install','--yes','--no-rc','--root-prefix',root/'mamba-root',
       '--prefix',compiler,'--channel','conda-forge','zlib=1.3.2','openssl=3',
       'gxx_linux-64=14.3.0','libstdcxx=14.3.0','libgcc=14.3.0'],
       'compiler-dependencies.log',600)
  run([root/'micromamba','list','--no-rc','--prefix',compiler,'--explicit'],'compiler-dependencies-explicit.txt',60)
  command=read(root/'configure-toolchain-state.json')['commands'][-1]
  command += ['-DCMAKE_PREFIX_PATH='+str(compiler),'-DZLIB_ROOT='+str(compiler),
              '-DOPENSSL_ROOT_DIR='+str(compiler),'-DCMAKE_BUILD_RPATH='+str(compiler/'lib'),
              '-DCMAKE_INSTALL_RPATH='+str(compiler/'lib')]
  run(command,'configure-prefix.log',900)
  prior=read(root/'configure-state.json');prior.pop('error',None)
  prior.update(status='completed',commands=[command],compiler_prefix=str(compiler),
               compiler_state=str(state_path),finished_unix_ns=time.time_ns())
  write(root/'configure-state.json',prior)
 elif a.action=='configure-headers':
  assert read(root/'baseline-state.json')['status']=='failed'
  compiler=root/'compiler'
  run([root/'micromamba','install','--yes','--no-rc','--root-prefix',root/'mamba-root',
       '--prefix',compiler,'--channel','conda-forge','brotli-devel=1.2.0'],
       'compiler-brotli.log',600)
  command=read(root/'configure-state.json')['commands'][-1]
  command += ['-U*BROTLI*','-DZLIB_INCLUDE_DIR='+str(compiler/'include'),
              '-DZLIB_LIBRARY_RELEASE='+str(compiler/'lib/libz.so')]
  run(command,'configure-headers.log',900)
  prior=read(root/'configure-state.json');prior.pop('error',None)
  prior.update(status='completed',commands=[command],compiler_state=str(state_path),finished_unix_ns=time.time_ns())
  write(root/'configure-state.json',prior)
 elif a.action=='reconfigure':
  prior=read(root/'configure-state.json')
  assert prior['status']=='failed'
  shutil.copyfile(root/'configure-state.json',root/'configure-first-failed-state.json')
  run(prior['commands'][-1],'configure-retry.log',900)
  prior.pop('error',None)
  prior.update(status='completed',reconfigure_state=str(state_path),finished_unix_ns=time.time_ns())
  write(root/'configure-state.json',prior)
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
