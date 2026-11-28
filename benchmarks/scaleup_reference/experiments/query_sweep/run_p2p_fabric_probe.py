#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Measure isolated peer-copy throughput for the owned node's actual NUMA topology."""
import argparse
from collections import defaultdict
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parents[1]/'scripts'))
from common import read,sha,write
import services

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--work',type=Path,required=True)
p.add_argument('--thread-id',required=True)
a=p.parse_args()
assert read(a.work/'evidence/owner.json')['thread_id']==a.thread_id
assert not subprocess.check_output(['nvidia-smi','--query-compute-apps=pid','--format=csv,noheader'],text=True).strip()
for area in ('code-profile','data-path-trace'):
 assert all(read(p)['status'] not in ('starting','running') for p in (a.work/area).glob('*/state.json'))
out=a.work/'p2p-fabric-probe';out.mkdir(exist_ok=False)
state={'status':'building','owner_thread':a.thread_id,'started_unix_ns':time.time_ns(),
       'classification':'Isolated direct CUDA copies; not Spark or UCX throughput and not a query speedup.'}
write(out/'state.json',state)
try:
 cuda=Path('/usr/local/cuda-13.1');binary=out/'p2p-fabric-probe'
 command=[str(cuda/'bin/nvcc'),'-std=c++17','-O3','-arch=sm_120','-ccbin','g++-12',
          '-Xcompiler','-pthread',str(HERE/'p2p_fabric_probe.cu'),'-o',str(binary)]
 with (out/'build.log').open('x') as log:subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=120)
 inventory=subprocess.check_output(['nvidia-smi','--query-gpu=index,uuid,pci.bus_id,name,driver_version',
                                    '--format=csv,noheader'],text=True)
 (out/'gpu-inventory.txt').write_text(inventory)
 (out/'topology.txt').write_text(subprocess.check_output(['nvidia-smi','topo','-m'],text=True))
 params=Path('/proc/driver/nvidia/params').read_text()
 assert 'ForceP2P=17;RMForceP2PType=1;RMPcieP2PType=1' in params and 'DmaRemapPeerMmio: 0' in params
 (out/'driver-params.txt').write_text(params)
 nodes={};uuids={};pci_addresses={};cpus={};used=set()
 def cpu_list(spec):
  result=[]
  for part in spec.strip().split(','):
   values=list(map(int,part.split('-')))
   result.extend(range(values[0],values[-1]+1))
  return result
 allowed=os.sched_getaffinity(0)
 for line in inventory.splitlines():
  index,uuid,pci,name,driver=[v.strip() for v in line.split(',')];index=int(index)
  domain,bus,device=pci.lower().split(':')
  node=int((Path('/sys/bus/pci/devices')/f'{domain[-4:]}:{bus}:{device}'/'numa_node').read_text())
  assert node>=0
  selected=[c for c in cpu_list((Path('/sys/devices/system/node')/f'node{node}'/'cpulist').read_text())
            if c in allowed and c not in used]
  assert selected
  cpus[index]=selected[0];used.add(selected[0]);nodes[index]=node;uuids[index]=uuid
  pci_addresses[index]=(int(domain,16),int(bus,16),int(device.split('.')[0],16))
 assert set(nodes)==set(range(8)) and len(set(nodes.values()))==2
 manifest={'build_command':command,'source_sha256':sha(HERE/'p2p_fabric_probe.cu'),
           'binary_sha256':sha(binary),'source_revision':subprocess.check_output(['git','-C',str(HERE),'rev-parse','HEAD'],text=True).strip(),
           'numa_nodes':nodes,'submission_cpus':cpus,'gpu_uuids':uuids,
           'limits':['One process owns all eight primary contexts; UCX IPC and Spark scheduling are absent.',
                     'Both sender- and receiver-initiated calls are measured without assuming which path UCX uses.',
                     'Full destination buffers are checked after each repetition; preparation and validation are not timed.',
                     'Repeated direct transfers isolate transport and API submission; they do not reproduce shuffle packing or spill.']}
 write(out/'manifest.json',manifest)
 env=dict(os.environ,CUDA_VISIBLE_DEVICES=','.join(uuids[i] for i in range(8)),CUDA_DEVICE_ORDER='PCI_BUS_ID')
 command=[str(binary),','.join(str(nodes[i]) for i in range(8)),','.join(str(cpus[i]) for i in range(8))]
 child,record=services.launch(command,out/'results.jsonl',env)
 write(out/'services.json',[record]);state.update(status='running',service=record);write(out/'state.json',state)
 state['exit']=child.wait(timeout=180)
 assert state['exit']==0
 rows=[json.loads(line) for line in (out/'results.jsonl').read_text().splitlines()]
 observed=[r for r in rows if r['kind']=='device'];assert len(observed)==8
 for r in observed:assert tuple(r[k] for k in ('pci_domain','pci_bus','pci_device'))==pci_addresses[r['gpu']]
 measured=[r for r in rows if r['kind']=='measurement'];assert len(measured)==60
 assert all(r['validation']=='passed' for r in measured)
 groups=defaultdict(list)
 for r in measured:groups[(r['mode'],r['initiator'],r['chunk_bytes'])].append(r)
 summary=[]
 for (mode,initiator,chunk),values in groups.items():
  assert len(values)==3
  row={'mode':mode,'initiator':initiator,'chunk_bytes':chunk,
       'median_aggregate_GBps':statistics.median(r['aggregate_GBps'] for r in values),
       'individual_aggregate_GBps':[r['aggregate_GBps'] for r in values]}
  summary.append(row);print(json.dumps(row),flush=True)
 write(out/'summary.json',summary);state['status']='completed'
except BaseException as error:
 state.update(status='failed',error=repr(error));raise
finally:
 services.stop(out/'services.json')
 state['finished_unix_ns']=time.time_ns();write(out/'state.json',state)
