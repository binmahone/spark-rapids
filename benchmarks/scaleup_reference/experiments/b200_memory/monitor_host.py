#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Record physical I/O, memory pressure and GPU clocks with persistent NVML handles."""
import argparse,ctypes,json,os,signal,time
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--work',required=True);p.add_argument('--output');p.add_argument('--gpus',default='0,1,2,3');a=p.parse_args();work=Path(a.work).resolve();output=Path(a.output) if a.output else work/'evidence/host-monitor.jsonl';output.parent.mkdir(exist_ok=True)
running=True
def stop(*args):
 global running
 running=False
signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
lib=ctypes.CDLL('libnvidia-ml.so.1')
assert lib.nvmlInit_v2()==0
handles=[];count=ctypes.c_uint();assert lib.nvmlDeviceGetCount_v2(ctypes.byref(count))==0
gpu_indices=[int(i) for i in a.gpus.split(',')]
assert len(set(gpu_indices))==4 and set(gpu_indices)<=set(range(8))
for i in gpu_indices:
 handle=ctypes.c_void_p();assert lib.nvmlDeviceGetHandleByIndex_v2(i,ctypes.byref(handle))==0;handles.append(handle)
class Memory(ctypes.Structure):_fields_=[('total',ctypes.c_ulonglong),('free',ctypes.c_ulonglong),('used',ctypes.c_ulonglong)]
class Util(ctypes.Structure):_fields_=[('gpu',ctypes.c_uint),('memory',ctypes.c_uint)]
def gpu_sample(handle):
 result={};memory=Memory();code=lib.nvmlDeviceGetMemoryInfo(handle,ctypes.byref(memory))
 if code==0:result.update(memory_used_bytes=memory.used,memory_total_bytes=memory.total)
 u=Util();code=lib.nvmlDeviceGetUtilizationRates(handle,ctypes.byref(u))
 if code==0:result.update(util_gpu=u.gpu,util_memory=u.memory)
 for name,func,args in [('sm_clock_mhz','nvmlDeviceGetClockInfo',[1]),('memory_clock_mhz','nvmlDeviceGetClockInfo',[2]),('power_mw','nvmlDeviceGetPowerUsage',[]),('temperature_c','nvmlDeviceGetTemperature',[0])]:
  v=ctypes.c_uint();code=getattr(lib,func)(handle,*args,ctypes.byref(v));result[name]=v.value if code==0 else {'nvml_error':code}
 v=ctypes.c_ulonglong();code=lib.nvmlDeviceGetCurrentClocksThrottleReasons(handle,ctypes.byref(v));result['throttle_reasons']=v.value if code==0 else {'nvml_error':code}
 return result
def keyed(path,wanted=None):
 result={}
 for line in Path(path).read_text().splitlines():
  parts=line.replace(':','').split()
  if len(parts)>1 and (wanted is None or parts[0] in wanted):
   try:result[parts[0]]=int(parts[1])
   except ValueError:pass
 return result
pids=[];tick=0
try:
 with output.open('w',buffering=1) as stream:
  while running:
   stamp=time.time_ns()
   if tick%5==0:
    pids=[]
    for proc in Path('/proc').iterdir():
     if not proc.name.isdigit():continue
     try:
      if proc.stat().st_uid==os.getuid():
       cmd=proc.joinpath('cmdline').read_bytes()
       if b'CoarseGrainedExecutorBackend' in cmd and str(work).encode() in cmd:pids.append(int(proc.name))
     except (FileNotFoundError,ProcessLookupError,PermissionError):pass
   tasks=[]
   for pid in pids:
    proc=Path('/proc')/str(pid)
    try:
     stat=proc.joinpath('stat').read_text().split(') ',1)[1].split()
     tasks.append({'pid':pid,'io':keyed(proc/'io'),'cpu_ticks':{'utime':int(stat[11]),'stime':int(stat[12])},'memory_kib':keyed(proc/'status',{'VmRSS','RssAnon','RssFile','VmSwap'})})
    except (FileNotFoundError,ProcessLookupError,PermissionError):pass
   disks={}
   for line in Path('/proc/diskstats').read_text().splitlines():
    fields=line.split()
    if fields[2].startswith(('nvme','dm-','md')):disks[fields[2]]=[int(v) for v in fields[3:]]
   record={'time_unix_ns':stamp,'monotonic_ns':time.monotonic_ns(),'executors':tasks,'diskstats':disks,
    'memory_kib':keyed('/proc/meminfo',{'MemAvailable','MemFree','Cached','Buffers','Dirty','Writeback','AnonPages','SwapFree','Slab'}),
    'vmstat':keyed('/proc/vmstat',{'pgpgin','pgpgout','pgmajfault','pgscan_kswapd','pgsteal_kswapd','pgscan_direct','pgsteal_direct','workingset_refault_file','workingset_refault_anon'}),
    'cpu_ticks':[int(v) for v in Path('/proc/stat').read_text().splitlines()[0].split()[1:]],
    'gpu_indices':gpu_indices,'gpus':[gpu_sample(h) for h in handles]}
   for kind in ['io','memory','cpu']:
    pressure=Path('/proc/pressure')/kind
    if pressure.exists():record['pressure_'+kind]=pressure.read_text().strip()
   stream.write(json.dumps(record,separators=(',',':'))+'\n');tick+=1;time.sleep(1)
finally:lib.nvmlShutdown()
