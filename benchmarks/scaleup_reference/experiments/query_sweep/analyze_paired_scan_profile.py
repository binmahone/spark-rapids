#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Associate sampled host stacks with recorded GPU activity gaps in the same scan window."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import sqlite3
import subprocess

from analyze_critical_path import schedule
from analyze_data_path_trace import merged, duration

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--work',type=Path,required=True)
p.add_argument('--trace',type=Path,required=True)
a=p.parse_args()
s=json.loads((a.trace/'state.json').read_text())
assert s['status']=='completed' and s['wall_stacks'] and not s['full_sql_trace']
db=sqlite3.connect('file:'+str(a.trace/'q9.sqlite')+'?mode=ro',uri=True)
epoch=db.execute('select utcEpochNs from TARGET_INFO_SESSION_START_TIME').fetchone()[0]
pids=[r[0] for r in db.execute('select distinct globalPid from CUPTI_ACTIVITY_KIND_KERNEL')]
assert len(pids)==8
all_ranges=defaultdict(list)
for table in ('CUPTI_ACTIVITY_KIND_KERNEL','CUPTI_ACTIVITY_KIND_MEMCPY','CUPTI_ACTIVITY_KIND_MEMSET'):
    for begin,end,pid in db.execute('select start,end,globalPid from '+table):
        all_ranges[pid].append((begin+epoch,end+epoch))
first=min(start for rows in all_ranges.values() for start,end in rows)+100000000
last=max(end for rows in all_ranges.values() for start,end in rows)-100000000
timeline=schedule(Path(s['run']))
candidates=[]
for token,q in timeline.items():
    for stage in q['stages']:
        if not any('lineitem' in row['node'] for row in stage['scan_batch_distribution']):continue
        begin=max(first,q['window']['start']*1000000+round(stage['start_seconds']*1e9))
        end=min(last,q['window']['start']*1000000+round(stage['end_seconds']*1e9))
        if end>begin:candidates.append((end-begin,token,stage,begin,end))
assert candidates,'Bounded capture missed the lineitem scan'
_,token,stage,begin,end=max(candidates,key=lambda x:x[0])
out=a.trace/'paired-stack-analysis';out.mkdir(exist_ok=False)
classes=out/'classes';classes.mkdir()
here=Path(__file__).resolve().parent
subprocess.run([str(a.work/'jdk17/bin/javac'),'--release','17','-d',str(classes),
    str(here/'ReadPairedStackSamples.java')],check=True)
result={'query_window':token,'stage':stage['stage'],'begin_unix_ns':begin,'end_unix_ns':end,
    'window_seconds':(end-begin)/1e9,'executors':[],'limits':[
    'CPU and wall samples are separate populations; counts are not query wall seconds.',
    'A stack present during a recorded GPU gap is an association, not proof it caused the gap.',
    'Wall samples can aggregate repeated observations. A gap assignment requires the timestamp plus and minus the recorded span to fit inside one gap.',
    'Small gaps may contain no samples. Mixed or boundary-spanning wall samples remain unresolved.',
    'Absence of recorded CUDA activity is not a hardware utilization measurement; foreign peer activity can be missing from this context.',
    'Only the lineitem stage portion inside the bounded capture is analyzed; both capture edges lose 100 ms.'
]}
for global_pid in pids:
    pid=(global_pid>>24)&0xffffff
    intervals=merged([(max(start,begin),min(stop,end)) for start,stop in all_ranges[global_pid] if stop>begin and start<end])
    gaps=[];cursor=begin
    for start,stop in intervals:
        if start>cursor:gaps.append((cursor,start))
        cursor=max(cursor,stop)
    if cursor<end:gaps.append((cursor,end))
    gapfile=out/f'gaps-{pid}.tsv';gapfile.write_text(''.join(f'{b}\t{e}\n' for b,e in gaps))
    output=out/f'samples-{pid}.jsonl'
    profile=a.trace/f'profile-{pid}.jfr';assert profile.exists()
    subprocess.run([str(a.work/'jdk17/bin/java'),'-cp',str(classes),
        'com.nvidia.spark.rapids.experiments.ReadPairedStackSamples',str(profile),str(begin),str(end),str(gapfile),str(output)],check=True,timeout=180)
    rows=[json.loads(line) for line in output.read_text().splitlines()]
    assert any(row['group'].startswith('cpu:task:') for row in rows)
    assert any(row['group'].startswith('wall:task:') for row in rows)
    row={'pid':pid,'recorded_gpu_active_seconds':duration(intervals),
        'recorded_gpu_gap_seconds':duration(gaps),'gap_count':len(gaps),
        'longest_gaps_seconds':sorted([(e-b)/1e9 for b,e in gaps],reverse=True)[:10],
        'samples':rows}
    result['executors'].append(row)
    print(pid,'gap_s',row['recorded_gpu_gap_seconds'],'samples',[(r['group'],r['weighted_samples']) for r in rows if ':task:' in r['group']],flush=True)
db.close()
(out/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
