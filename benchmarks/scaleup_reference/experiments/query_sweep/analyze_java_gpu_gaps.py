#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Align executed Java ranges, Spark task setup and GPU gaps in a completed trace."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import sqlite3
from urllib.parse import urlparse

from analyze_critical_path import schedule
from analyze_data_path_trace import merged, duration


def intersect(left, right):
    i = j = 0
    output = []
    while i < len(left) and j < len(right):
        s, e = max(left[i][0], right[j][0]), min(left[i][1], right[j][1])
        if s < e:
            output.append((s, e))
        if left[i][1] < right[j][1]: i += 1
        else: j += 1
    return output


p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--trace',type=Path,required=True)
p.add_argument('--query',type=int,required=True)
a=p.parse_args()
state=json.loads((a.trace/'state.json').read_text())
assert state['status']=='completed' and state['nsys_exit']==0 and state['java_ranges']
assert state.get('full_sql_trace',True), 'Partial kernel proof is not a full-stage timeline'
run=Path(state['run']); timeline=schedule(run)[f'q{a.query}-iteration2']
db=sqlite3.connect('file:'+str(a.trace/f'q{a.query}.sqlite')+'?mode=ro',uri=True)
epoch=db.execute('select utcEpochNs from TARGET_INFO_SESSION_START_TIME').fetchone()[0]
names=dict(db.execute('select id,value from StringIds'))
qstart=timeline['window']['start']*1000000-epoch
qend=timeline['window']['end']*1000000-epoch
pids=[r[0] for r in db.execute('select distinct globalPid from CUPTI_ACTIVITY_KIND_KERNEL')]
assert len(pids)==8
ranges=defaultdict(lambda:defaultdict(list))
for s,e,tid,label,label_id in db.execute('select start,end,globalTid,text,textId from NVTX_EVENTS '
        'where end>? and start<? and end is not null',(qstart,qend)):
    pid=(tid>>24)<<24
    if pid in pids:
        ranges[pid][label or names.get(label_id,'')].append((s,e))
for labels in ranges.values():
    for key,values in labels.items(): labels[key]=merged(values)
activity=defaultdict(list)
for table in ('KERNEL','MEMCPY','MEMSET'):
    for s,e,pid in db.execute(f'select start,end,globalPid from CUPTI_ACTIVITY_KIND_{table} '
                              'where end>? and start<?',(qstart,qend)):
        activity[pid].append((s,e))
for pid in activity: activity[pid]=merged(activity[pid])
result={'limits':['Range overlap does not assign causality and categories can overlap.',
                 'GPU activity is interval presence, not SM utilization.',
                 'Overlapping Spark stages share executor activity; do not sum their GPU times.'],
        'stages':[]}
for stage in timeline['stages']:
    if stage['span_seconds']<1: continue
    start=qstart+round(stage['start_seconds']*1e9)
    end=qstart+round(stage['end_seconds']*1e9)
    item={k:v for k,v in stage.items() if k!='operator_metrics'}; item['executors']=[]
    for pid in sorted(pids):
        gpu=intersect(activity[pid],[(start,end)])
        cursor=start; gaps=[]
        for s,e in gpu:
            if s>cursor:gaps.append((cursor,s))
            cursor=max(cursor,e)
        if cursor<end:gaps.append((cursor,end))
        startup=[(start,gpu[0][0])] if gpu and gpu[0][0]>start else []
        remaining=intersect(gaps,[(gpu[0][0],end)]) if gpu else gaps
        def attribution(windows):
            values=[{'range':name,'overlap_seconds':duration(intersect(windows,values))}
                    for name,values in ranges[pid].items()]
            return sorted([r for r in values if r['overlap_seconds']>.0001],
                          key=lambda r:-r['overlap_seconds'])[:25]
        row={'pid':pid,'gap_seconds':duration(gaps),'startup_gap_seconds':duration(startup),
             'startup_ranges':attribution(startup),'later_ranges':attribution(remaining)}
        item['executors'].append(row)
    result['stages'].append(item)
    print('stage',stage['stage'],'tasks',stage['tasks'],'wall',stage['span_seconds'],flush=True)
    for row in item['executors'][:2]:
        print(row['pid'],'gap',round(row['gap_seconds'],3),'startup',round(row['startup_gap_seconds'],3),
              'startup_ranges',row['startup_ranges'][:8],'later_ranges',row['later_ranges'][:10],flush=True)
# Retain the first scheduled wave's task setup accounting without interpreting it as native work.
scan=max(timeline['stages'],key=lambda r:r['tasks'])
profile=json.loads((run/'applications/suite/profile.json').read_text())
first=defaultdict(list)
for path in sorted(Path(urlparse(profile['spark.eventLog.dir']).path).iterdir()):
    if not path.is_file() or path.name.startswith('.'): continue
    assert not path.name.endswith('.inprogress')
    for line in path.open():
        if 'SparkListenerTaskEnd' not in line: continue
        e=json.loads(line)
        if e.get('Stage ID')!=scan['stage']: continue
        info=e['Task Info']; metrics=e['Task Metrics']
        first[info['Executor ID']].append({'task':info['Task ID'],
            'launch_ms':info['Launch Time'],'finish_ms':info['Finish Time'],
            'deserialize_ms':metrics.get('Executor Deserialize Time'),
            'run_ms':metrics.get('Executor Run Time'),'gc_ms':metrics.get('JVM GC Time')})
result['scan_first_wave']={k:sorted(v,key=lambda r:r['launch_ms'])[:4] for k,v in first.items()}
(a.trace/'java-gpu-gap-analysis.json').write_text(json.dumps(result,indent=2)+'\n')
