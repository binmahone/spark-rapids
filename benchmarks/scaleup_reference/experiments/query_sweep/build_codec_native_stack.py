#!/usr/bin/env python3
"""Build the frozen integrated cuDF and JNI layout for a controlled native code comparison."""
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
p.add_argument('--action',choices=('baseline','candidate','tests'),required=True)
p.add_argument('--candidate-revision',default='a0ec28415a949329ba8727420e29ae7293c3dbc9')
a=p.parse_args()
assert read(a.work/'evidence/owner.json')['thread_id']==a.thread_id
assert not subprocess.check_output(['nvidia-smi','--query-compute-apps=pid','--format=csv,noheader'],text=True).strip()
for area in ('code-profile','data-path-trace'):
 assert all(read(path)['status'] not in ('starting','running') for path in (a.work/area).glob('*/state.json'))
root=a.work/'native-codec-stack'
root.mkdir(exist_ok=True)
state_path=root/(a.action+'-state.json')
previous=read(state_path) if state_path.exists() else {}
assert not previous or previous['status']=='failed'
if previous: shutil.copyfile(state_path,root/(a.action+'-failed-'+str(time.time_ns())+'.json'))
state={'status':'running','action':a.action,'owner_thread':a.thread_id,
       'started_unix_ns':time.time_ns(),'commands':[],
       'jni_revision':'bd15415531c40a2e83067d91147fcfa51a317b1d',
       'layout':'integrated','runtime_prefix':str(a.work/'native-codec-library/compiler')}
write(state_path,state)
old=a.work/'native-codec-library'
compiler=old/'compiler';cmake=old/'build-venv/bin/cmake'
sdk=old/'cufile-sdk/usr/local/cuda-13.1/targets/x86_64-linux'
source=root/'cudf';jni=root/'spark-jni';install=root/'install'
cpp_build=root/'cpp-build';java_build=root/'cudfjni-build';jni_build=root/'spark-jni-build'
env={**os.environ,'JAVA_HOME':str(a.work/'jdk17'),
     'PATH':str(old/'build-venv/bin')+':'+os.environ['PATH'],
     'PKG_CONFIG_LIBDIR':str(compiler/'lib/pkgconfig')+':'+str(compiler/'share/pkgconfig'),
     'LD_LIBRARY_PATH':str(compiler/'lib')+':/usr/local/cuda-13.1/lib64',
     'CUDF_ROOT':str(source),'CUDF_INSTALL_DIR':str(install),'CUDFJNI_BUILD_DIR':str(java_build)}

def run(command,name,timeout=900,overrides=None):
 command=list(map(str,command));state['commands'].append(command)
 completed=previous.get('completed_steps',{}).get(name)
 if completed and name in ('jni-init.log','jni-fetch.log','jni-checkout.log','cudf-worktree.log','boost.log'):
  assert completed==command
  state.setdefault('completed_steps',{})[name]=command
  write(state_path,state)
  return
 state.setdefault('invocations',[]).append({'argv':command,'environment':{
  key:env[key] for key in ('PATH','JAVA_HOME','PKG_CONFIG_LIBDIR','LD_LIBRARY_PATH','CUDF_ROOT','CUDF_INSTALL_DIR','CUDFJNI_BUILD_DIR')},
  'extra_environment':overrides or {}})
 write(state_path,state)
 log_path=root/(a.action+'-'+name)
 if log_path.exists():log_path.rename(root/(a.action+'-'+str(time.time_ns())+'-'+name))
 with log_path.open('x') as log:
  subprocess.run(command,cwd=root,env={**env,**(overrides or {})},stdout=log,
                 stderr=subprocess.STDOUT,check=True,timeout=timeout)
 state.setdefault('completed_steps',{})[name]=command
 write(state_path,state)

def smoke_check(artifact,tag):
 smoke=root/'smoke';smoke.mkdir(exist_ok=True)
 engine=a.work/'artifacts/decimal-null-copy/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar'
 assert sha(engine)=='ec38a58426e662ce99a433e806a66fa927bb011c014465a5df0b0c31ded4d1cc'
 spark=Path(read(a.work/'evidence/deployment.json')['spark_home'])
 classpath=str(engine)+':'+str(spark/'jars/*')
 run([a.work/'jdk17/bin/javac','-cp',classpath,'-d',smoke,HERE/'NativeStackSmoke.java'],tag+'-smoke-compile.log',120)
 uuid=subprocess.check_output(['nvidia-smi','--id=0','--query-gpu=uuid','--format=csv,noheader'],text=True).strip()
 run([a.work/'jdk17/bin/java','-Dai.rapids.cudf.lib-native-dir='+str(artifact),
      '-cp',str(smoke)+':'+classpath,'NativeStackSmoke'],tag+'-smoke.log',120,{'CUDA_VISIBLE_DEVICES':uuid})
 assert 'NATIVE_STACK_SMOKE_PASSED' in (root/(a.action+'-'+tag+'-smoke.log')).read_text()

try:
 if a.action=='tests':
  artifact=read(root/'candidate-state.json');assert artifact['status']=='completed'
  control=read(root/'baseline-state.json');assert control['status']=='completed'
  assert not subprocess.check_output(['git','-C',str(source),'diff',control['source_revision'],
   artifact['source_revision'],'--','java/src/main/native'],text=True)
  state['identical_cudf_jni_source_tree']=subprocess.check_output(['git','-C',str(source),'rev-parse',
   artifact['source_revision']+':java/src/main/native'],text=True).strip()
  library=root/'candidate/libcudf.so'
  assert sha(library)==artifact['library_sha256']
  executable=old/'build/gtests/PARQUET_ACTIVE_CODECS_TEST'
  assert executable.is_file()
  path=str(root/'candidate')+':'+str(compiler/'lib')+':'+str(old/'build')+':/usr/local/cuda-13.1/lib64'
  uuid=subprocess.check_output(['nvidia-smi','--id=0','--query-gpu=uuid','--format=csv,noheader'],text=True).strip()
  run(['/lib64/ld-linux-x86-64.so.2','--inhibit-rpath','','--library-path',path,executable,
       '--gtest_output=xml:'+str(root/'reader-tests.xml')],'reader.log',300,
       {'LD_DEBUG':'libs','CUDA_VISIBLE_DEVICES':uuid})
  import xml.etree.ElementTree as ET
  result=ET.parse(root/'reader-tests.xml').getroot()
  assert int(result.attrib['tests'])==3 and int(result.attrib['failures'])==0
  assert int(result.attrib['disabled'])==0 and not result.findall('.//skipped')
  loaded=[line.strip() for line in (root/'tests-reader.log').read_text().splitlines()
          if 'calling init:' in line and 'libcudf.so' in line]
  assert len(loaded)==1 and loaded[0].endswith('calling init: '+str(library)),loaded
  state.update(reader_tests=dict(result.attrib),tested_library_sha256=sha(library),
               tested_executable_sha256=sha(executable),tested_library_load_evidence=loaded)
  for implementation in ('baseline','candidate'):
   identity=read(root/(implementation+'-state.json'))
   assert identity['status']=='completed' and sha(root/implementation/'libcudf.so')==identity['library_sha256']
   smoke_check(root/implementation,implementation)
  state['java_gpu_smoke']={'baseline':'passed','candidate':'passed'}
 else:
  if a.action=='baseline':
   assert shutil.disk_usage(root).free>80*1024**3
   run(['git','init',jni],'jni-init.log')
   run(['git','-C',jni,'fetch','--depth=1','https://github.com/NVIDIA/cudf-spark-jni.git',state['jni_revision']],'jni-fetch.log')
   run(['git','-C',jni,'checkout','--detach','FETCH_HEAD'],'jni-checkout.log')
   run(['git','-C',a.work/'native-concat/cudf','worktree','add','--detach',source,
        '5a2cad06ba5ef1b77a5cc3f1fc61fc0cee5bf57c'],'cudf-worktree.log')
   assert read(jni/'thirdparty/cudf-pins/versions.json')==read(a.work/'native-concat/pins.json')
   run([old/'micromamba','install','--yes','--no-rc','--root-prefix',old/'mamba-root',
        '--prefix',compiler,'--channel','conda-forge','libboost-devel=1.86.0',
        'gxx_linux-64=14.3.0','libstdcxx=14.3.0','libgcc=14.3.0'],'boost.log',600)
  else:
   assert read(root/'baseline-state.json')['status']=='completed'
   run(['git','-C',source,'checkout','--detach',a.candidate_revision],'candidate-checkout.log')
  assert not subprocess.check_output(['git','-C',str(source),'status','--porcelain'],text=True)
  run([old/'micromamba','list','--no-rc','--prefix',compiler,'--explicit'],'compiler.txt',60)
  state['compiler_package_list_sha256']=sha(root/(a.action+'-compiler.txt'))
  jdk_sdk=root/'jdk-build-sdk'
  jdk_sdk.mkdir(exist_ok=True)
  jdk_version='17.0.21.1+2-1~22.04.1'
  for package in ('openjdk-17-jdk','openjdk-17-jre'):
   deb=root/(package+'_'+jdk_version+'_amd64.deb')
   if not deb.exists():
    run(['apt-get','download',package+'='+jdk_version],package+'-download.log',300)
   run(['dpkg-deb','-x',deb,jdk_sdk],package+'-extract.log',60)
   state.setdefault('jdk_build_packages',{})[deb.name]=sha(deb)
  jdk_awt=jdk_sdk/'usr/lib/jvm/java-17-openjdk-amd64'
  assert (jdk_awt/'include/jawt.h').is_file() and (jdk_awt/'lib/libjawt.so').is_file()
  common=['-G','Ninja','-DCMAKE_BUILD_TYPE=Release',
   '-DCMAKE_C_COMPILER='+str(compiler/'bin/x86_64-conda-linux-gnu-gcc'),
   '-DCMAKE_CXX_COMPILER='+str(compiler/'bin/x86_64-conda-linux-gnu-g++'),
   '-DCMAKE_CUDA_COMPILER=/usr/local/cuda-13.1/bin/nvcc',
   '-DCMAKE_CUDA_HOST_COMPILER='+str(compiler/'bin/x86_64-conda-linux-gnu-g++'),
   '-DCMAKE_MAKE_PROGRAM='+str(old/'build-venv/bin/ninja'),
   '-DCMAKE_CUDA_ARCHITECTURES=120','-DCUDF_USE_PER_THREAD_DEFAULT_STREAM=ON',
   '-DCUDA_STATIC_RUNTIME=ON','-DCUDA_STATIC_CUFILE=ON','-DBUILD_TESTS=OFF',
   '-DBUILD_BENCHMARKS=OFF','-DCUDF_DEPENDENCY_PIN_MODE=pinned',
   '-DRMM_LOGGING_LEVEL=OFF','-DLIBCUDF_LOGGING_LEVEL=OFF',
   '-DCMAKE_PREFIX_PATH='+str(compiler),'-DCMAKE_INSTALL_LIBDIR=lib64',
   '-DZLIB_ROOT='+str(compiler),'-DZLIB_INCLUDE_DIR='+str(compiler/'include'),
   '-DZLIB_LIBRARY_RELEASE='+str(compiler/'lib/libz.so'),
   '-DJAVA_AWT_INCLUDE_PATH='+str(jdk_awt/'include'),
   '-DJAVA_AWT_LIBRARY='+str(jdk_awt/'lib/libjawt.so'),
   '-DCMAKE_BUILD_RPATH='+str(compiler/'lib'),
   '-DCMAKE_EXE_LINKER_FLAGS=-Wl,-rpath-link,'+str(compiler/'lib'),
   '-DCMAKE_CXX_FLAGS=-I'+str(sdk/'include'),'-DCMAKE_CUDA_FLAGS=-I'+str(sdk/'include'),
   '-DCUDA_cuFile_LIBRARY='+str(sdk/'lib/libcufile.so'),
   '-DCUDA_cuFile_static_LIBRARY='+str(sdk/'lib/libcufile_static.a'),
   '-DCUDA_cuFile_rdma_LIBRARY='+str(sdk/'lib/libcufile_rdma.so'),
   '-DCUDA_cuFile_rdma_static_LIBRARY='+str(sdk/'lib/libcufile_rdma_static.a'),
   '-C',str(jni/'thirdparty/cudf-pins/setup.cmake')]
  state['configuration']={'common':common,'cpp':['BUILD_SHARED_LIBS=OFF','CUDF_KVIKIO_REMOTE_IO=OFF',
   'CUDF_LARGE_STRINGS_DISABLED=ON','CUDF_BUILD_STATIC_DEPS=FORCE'],
   'jni':['BUILD_SHARED_LIBS=OFF','CUDF_JNI_LIBCUDF_STATIC=ON','USE_GDS=OFF'],
   'adaptations':['Single sm_120 target','Isolated GCC 14.3 and runtime dependencies',
                  'Test utilities, profiler and fault injection omitted from the build'],
   'build_entrypoint_sha256':sha(jni/'build/buildcpp.sh'),
   'dependency_pins_sha256':sha(jni/'thirdparty/cudf-pins/versions.json')}
  run([cmake,'-S',source/'cpp','-B',cpp_build,*common,'-DBUILD_SHARED_LIBS=OFF',
       '-DCUDF_KVIKIO_REMOTE_IO=OFF','-DCUDF_LARGE_STRINGS_DISABLED=ON',
       '-DCUDF_BUILD_STATIC_DEPS=FORCE','-DCUDF_BUILD_TESTUTIL=OFF',
       '-DCMAKE_INSTALL_PREFIX='+str(install)],'cpp-configure.log')
  run([cmake,'--build',cpp_build,'--target','install','-j','48'],'cpp-build.log',3600)
  run([cmake,'-S',source/'java/src/main/native','-B',java_build,*common,
       '-DBUILD_SHARED_LIBS=OFF','-DCUDF_JNI_LIBCUDF_STATIC=ON','-DUSE_GDS=OFF',
       '-DBOOST_ROOT='+str(compiler),'-DBoost_ROOT='+str(compiler),
       '-DBoost_DIR='+str(compiler/'lib/cmake/Boost-1.86.0')],'cudfjni-configure.log')
  run([cmake,'--build',java_build,'-j','48'],'cudfjni-build.log',1800)
  run([cmake,'-S',jni/'src/main/cpp','-B',jni_build,*common,
       '-DBUILD_FAULTINJ=OFF','-DBUILD_PROFILER=OFF','-DUSE_GDS=OFF'],'sparkjni-configure.log')
  run([cmake,'--build',jni_build,'--target','spark_rapids_jni','cudfjnistub','-j','48'],
      'sparkjni-build.log',1800)
  artifact=root/a.action;artifact.mkdir(exist_ok=bool(previous))
  shutil.copyfile(jni_build/'libcudf.so',artifact/'libcudf.so')
  shutil.copyfile(jni_build/'libcudfjni.so',artifact/'libcudfjni.so')
  for name in ('libcufilejni.so','libnvmljni.so'):
   link=artifact/name;target=a.work/'native-concat/lib'/name
   if link.exists():assert link.resolve()==target.resolve()
   else:link.symlink_to(target)
  exports=subprocess.check_output(['nm','-D','--defined-only',str(artifact/'libcudf.so')],text=True)
  required=['Java_com_nvidia_spark_rapids_jni_Hash_getMaxStackDepth',
            'Java_ai_rapids_cudf_HybridScanReader_materializeAllColumns']
  assert all(name in exports for name in required)
  run(['ldd',artifact/'libcudf.so'],'ldd.log',60)
  assert 'not found' not in (root/(a.action+'-ldd.log')).read_text()
  state.update(source_revision=subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD'],text=True).strip(),
               library_sha256=sha(artifact/'libcudf.so'),jni_stub_sha256=sha(artifact/'libcudfjni.so'),
               required_exports=required)
  smoke_check(artifact,a.action)
  state['java_gpu_smoke']='passed'
 state['status']='completed'
except BaseException as error:
 state.update(status='failed',error=repr(error));raise
finally:
 state['finished_unix_ns']=time.time_ns();write(state_path,state)
print(json.dumps({'action':a.action,'status':state['status'],'library_sha256':state.get('library_sha256')}),flush=True)
