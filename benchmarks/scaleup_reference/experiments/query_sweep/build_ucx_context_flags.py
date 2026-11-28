#!/usr/bin/env python3
"""Reproduce UCX context flag clobbering and build one isolated CUDA transport module."""
import argparse
import os
from pathlib import Path
import shutil
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
root=a.work/'native-ucx-context-flags-v2';root.mkdir(exist_ok=False)
source=a.work/'ucx-source';prefix=a.work/'ucx';build=source/'src/uct/cuda'
assert subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD'],text=True).strip()=='8a6b06fb880accbb933a79cda893883872c68d9d'
relative='src/uct/cuda/cuda_copy/cuda_copy_md.c'
assert not subprocess.check_output(['git','-C',str(source),'diff','HEAD','--',relative],text=True)
(root/'original-source-diff.patch').write_text(subprocess.check_output(['git','-C',str(source),'diff','HEAD'],text=True))
state={'status':'running','owner_thread':a.thread_id,'commands':[],
    'ucx_revision':'8a6b06fb880accbb933a79cda893883872c68d9d',
    'original_source_diff_sha256':sha(root/'original-source-diff.patch'),
    'original_cuda_source_sha256':sha(source/relative),
    'original_module_sha256':sha(prefix/'lib/ucx/libuct_cuda.so'),
    'source_revision':subprocess.check_output(['git','-C',str(HERE),'rev-parse','HEAD'],text=True).strip()}
uuid=subprocess.check_output(['nvidia-smi','--id=0','--query-gpu=uuid','--format=csv,noheader'],text=True).strip()
env={**os.environ,'CUDA_VISIBLE_DEVICES':uuid,'UCX_TLS':'self,sm,cuda_copy,cuda_ipc',
    'UCX_MODULE_DIR':str(prefix/'lib/ucx'),
    'LD_LIBRARY_PATH':str(prefix/'lib')+':/usr/local/cuda-13.1/lib64'}
def run(command,name,cwd=root,extra=None):
    command=list(map(str,command));state['commands'].append({'argv':command,'cwd':str(cwd),'extra_environment':extra or {}})
    write(root/'state.json',state)
    with (root/name).open('x') as log:
        subprocess.run(command,cwd=cwd,env={**env,**(extra or {})},stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
try:
    run(['g++-12','-std=c++17','-O2','-I'+str(prefix/'include'),'-I/usr/local/cuda-13.1/include',
        HERE/'ucx_context_flags_probe.cpp','-L'+str(prefix/'lib'),'-lucp','-lucs',
        '-L/usr/local/cuda-13.1/lib64','-lcudart','-L/usr/local/cuda-13.1/lib64/stubs','-lcuda',
        '-o',root/'ucx-context-flags-probe'],'probe-build.log')
    run([root/'ucx-context-flags-probe','clobber'],'baseline-probe.log')
    patched=root/'source'/relative;patched.parent.mkdir(parents=True)
    shutil.copyfile(source/relative,patched)
    patch=HERE/'ucx-preserve-context-flags.patch'
    run(['git','apply','--check',patch],'patch-check.log',root/'source')
    run(['git','apply',patch],'patch.log',root/'source')
    state['patch_sha256']=sha(patch)
    state['patched_source_sha256']=sha(patched)
    common=['-O3','-g','-Wall','-Werror','-funwind-tables','-Wframe-larger-than=8192',
        '-Wno-missing-field-initializers','-Wno-unused-parameter','-Wno-unused-label','-Wno-long-long',
        '-Wno-endif-labels','-Wno-sign-compare','-Wno-multichar','-Wno-deprecated-declarations',
        '-Winvalid-pch','-Wno-pointer-sign','-Werror-implicit-function-declaration','-Wno-format-zero-length',
        '-Wnested-externs','-Wshadow','-Werror=declaration-after-statement','-no-suppress']
    run(['bash',source/'libtool','--silent','--tag=CC','--mode=compile','gcc','-DHAVE_CONFIG_H',
        '-DCPU_FLAGS=','-I'+str(source),'-I'+str(source/'src'),'-I'+str(build),
        '-I'+str(build/'cuda_copy'),'-I/usr/local/cuda-13.0/include',*common,
        '-c',patched,'-o',root/'cuda_copy_md.lo'],'module-compile.log',build)
    objects=['base/libuct_cuda_la-'+n+'.lo' for n in ('cuda_ctx','cuda_iface','cuda_md','cuda_nvml','cuda_util')]
    objects += [str(root/'cuda_copy_md.lo')]
    objects += ['cuda_copy/libuct_cuda_la-'+n+'.lo' for n in ('cuda_copy_iface','cuda_copy_ep')]
    objects += ['cuda_ipc/libuct_cuda_la-'+n+'.lo' for n in ('cuda_ipc_md','cuda_ipc_iface_address',
        'cuda_ipc_iface','cuda_ipc_ep','cuda_ipc_cache')]
    state['reused_objects']={name:sha(build/Path(name).parent/'.libs'/(Path(name).stem+'.o'))
        for name in objects if not name.startswith(str(root))}
    modules=root/'modules';modules.mkdir()
    run(['bash',source/'libtool','--silent','--tag=CC','--mode=link','gcc',*common,
        '-L/usr/local/cuda-13.0/lib64','-L/usr/local/cuda-13.0/lib64/stubs','-version-info','0:0:0',
        '-o',root/'libuct_cuda.la','-rpath',modules,*objects,
        source/'src/ucs/libucs.la',source/'src/uct/libuct.la','-lcuda','-lnvidia-ml','-lrt','-lrt'],
        'module-link.log',build)
    for item in (prefix/'lib/ucx').iterdir():
        if not item.name.startswith('libuct_cuda.'):(modules/item.name).symlink_to(item.resolve())
    shutil.copyfile(root/'.libs/libuct_cuda.so.0.0.0',modules/'libuct_cuda.so.0.0.0')
    for name in ('libuct_cuda.so','libuct_cuda.so.0'):(modules/name).symlink_to('libuct_cuda.so.0.0.0')
    run([root/'ucx-context-flags-probe','preserve'],'candidate-probe.log',extra={
        'UCX_PLUGIN_PATH':str(modules),'LD_DEBUG':'libs'})
    for mode in ('baseline','candidate'):
        import json
        lines=(root/(mode+'-probe.log')).read_text().splitlines()
        rows=[json.loads(line) for line in lines if line.startswith('{"initial"')]
        assert len(rows)==1;state[mode+'_probe']=rows[0]
    assert state['baseline_probe']['initial']&7==4
    assert state['baseline_probe']['after_ucp_mem_map']&7==0
    assert state['candidate_probe']['after_ucp_mem_map']&7==4
    loaded=[line.strip() for line in (root/'candidate-probe.log').read_text().splitlines()
        if 'calling init:' in line and 'libuct_cuda.so' in line]
    assert len(loaded)==1 and str(modules) in loaded[0],loaded
    assert sha(source/relative)==state['original_cuda_source_sha256']
    assert sha(prefix/'lib/ucx/libuct_cuda.so')==state['original_module_sha256']
    state.update(status='completed',module_sha256=sha(modules/'libuct_cuda.so'),
        tested_module_load_evidence=loaded,probe_sha256=sha(root/'ucx-context-flags-probe'))
except BaseException as error:
    state.update(status='failed',error=repr(error));raise
finally:
    state['finished_unix_ns']=time.time_ns();write(root/'state.json',state)
print(state['status'],flush=True)
