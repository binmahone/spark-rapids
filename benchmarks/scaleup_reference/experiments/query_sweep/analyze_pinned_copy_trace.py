#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Compare executed small-copy kernels and transfer activity in matching Q9 scan stages."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import sqlite3

from analyze_critical_path import schedule
from analyze_data_path_trace import merged, duration

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--control',type=Path,required=True)
p.add_argument('--candidate',type=Path,required=True)
p.add_argument('--output',type=Path,required=True)
a=p.parse_args()
result={'limits':['Diagnostic profiled times are not benchmark scores.',
                  'GPU activity intervals do not measure utilization or isolate cause.',
                  'Per-executor activity can overlap across streams; summed durations are not wall time.'],
        'runs':[]}
for label,trace in (('control',a.control),('candidate',a.candidate)):
 state=json.loads((trace/'state.json').read_text())
 assert state['status']=='completed' and state['nsys_exit']==0
 timeline=schedule(Path(state['run']))['q9-iteration2']
 stage=max(timeline['stages'],key=lambda r:r['tasks'])
 db=sqlite3.connect('file:'+str(trace/'q9.sqlite')+'?mode=ro',uri=True)
 epoch=db.execute('select utcEpochNs from TARGET_INFO_SESSION_START_TIME').fetchone()[0]
 start=timeline['window']['start']*1000000+round(stage['start_seconds']*1e9)-epoch
 end=timeline['window']['start']*1000000+round(stage['end_seconds']*1e9)-epoch
 names=dict(db.execute('select id,value from StringIds'))
 kinds=dict(db.execute('select id,label from ENUM_CUDA_MEMCPY_OPER'))
 intervals=defaultdict(lambda:defaultdict(list));counts=defaultdict(lambda:defaultdict(int));byte_counts=defaultdict(lambda:defaultdict(int))
 kernel_names=defaultdict(int)
 for s,e,pid,name in db.execute('select start,end,globalPid,demangledName from CUPTI_ACTIVITY_KIND_KERNEL '
                                  'where end>? and start<?',(start,end)):
  intervals[pid]['kernel'].append((max(s,start),min(e,end)))
  name=names[name]
  if 'cudf::detail::' in name and 'copy_kernel(' in name:
   intervals[pid]['small_pinned_copy_kernel'].append((max(s,start),min(e,end)))
   counts[pid]['small_pinned_copy_kernel']+=1;kernel_names[name]+=1
 for s,e,pid,kind,size in db.execute('select start,end,globalPid,copyKind,bytes from CUPTI_ACTIVITY_KIND_MEMCPY '
                                    'where end>? and start<?',(start,end)):
  kind=kinds[kind]
  bucket=kind+('-small' if size<65536 else '-bulk')
  intervals[pid][bucket].append((max(s,start),min(e,end)))
  counts[pid][bucket]+=1;byte_counts[pid][bucket]+=size
 for s,e,pid in db.execute('select start,end,globalPid from CUPTI_ACTIVITY_KIND_MEMSET where end>? and start<?',(start,end)):
  intervals[pid]['memset'].append((max(s,start),min(e,end)))
 assert len(intervals)==8
 rows=[]
 for pid,values in sorted(intervals.items()):
  active=duration(merged([interval for value in values.values() for interval in value]))
  row={'pid':pid,'union_seconds':{k:duration(merged(v)) for k,v in values.items()},
       'counts':dict(counts[pid]),'bytes':dict(byte_counts[pid]),'gap_seconds':(end-start)/1e9-active}
  rows.append(row)
  assert bool(counts[pid]['small_pinned_copy_kernel'])==(label=='candidate')
 entry={'label':label,'scan_stage':{k:v for k,v in stage.items() if k!='operator_metrics'},
        'copy_kernel_names':dict(kernel_names),'executors':rows}
 result['runs'].append(entry)
 print(label,'scan_wall',stage['span_seconds'],'copy_kernels',sum(kernel_names.values()),
       'mean_gpu_gap',sum(r['gap_seconds'] for r in rows)/8,flush=True)
 for key in sorted({k for r in rows for k in r['union_seconds']}):
  print(key,'mean_union_s',sum(r['union_seconds'].get(key,0) for r in rows)/8,
        'count',sum(r['counts'].get(key,0) for r in rows),'GB',sum(r['bytes'].get(key,0) for r in rows)/1e9,flush=True)
a.output.write_text(json.dumps(result,indent=2)+'\n')
