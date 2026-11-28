#!/usr/bin/env python3
"""Audit retained JFR stack coverage and map executor differences to physical workers."""
import argparse
from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import re
import subprocess

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--work',type=Path,required=True)
p.add_argument('--trace',type=Path,required=True)
a=p.parse_args()
s=json.loads((a.trace/'park-analysis/summary.json').read_text())
state=json.loads((a.trace/'state.json').read_text())
run=Path(state['run']);config=json.loads((run/'config.json').read_text())
scratch=Path(config['deployment']['scratch_root'])/run.name/'suite'
inventory=json.loads((scratch/'gpu-inventory.json').read_text())
records=json.loads(next((run/'services/suite').glob('*.json')).read_text())
if isinstance(records,dict):
    records=json.loads(next(path for path in (run/'services/suite').glob('*.json')
        if not path.name.endswith('.stopped.json')).read_text())
results=[]
for row in s['executors']:
    pid=row['pid'];executor=row['executor_id']
    logs=list(scratch.glob(f'worker-*/work/*/{executor}/stderr'));assert len(logs)==1
    worker=int(logs[0].parents[3].name.split('-')[1])
    recording=next((a.trace/'park-recordings').glob(f'hotspot-pid-{pid}-*.jfr'))
    parsed=json.loads(subprocess.check_output([a.work/'jdk17/bin/jfr','print','--json',
        '--stack-depth','256','--events','jdk.ThreadPark',recording],text=True))
    counts=Counter();frame_counts=Counter();unclassified=Counter()
    for event in parsed['recording']['events']:
        v=event['values']
        start=round(datetime.fromisoformat(v['startTime'].replace('Z','+00:00')).timestamp()*1e9)
        duration=float(re.fullmatch(r'PT([\d.]+)S',v['duration'])[1])*1e9
        if start+duration<=s['begin_unix_ns'] or start>=s['end_unix_ns']:continue
        trace=v.get('stackTrace') or {};frames=trace.get('frames',[])
        stack=';'.join(f['method']['type']['name'].replace('/','.')+'.'+f['method']['name'] for f in frames)
        counts['events']+=1;counts['truncated']+=bool(trace.get('truncated'))
        frame_counts[len(frames)]+=1
        if 'MultiFileCloudPartitionReaderBase.getNextBuffersAndMeta' in stack:
            counts['reader_waits']+=1
            counts['reader_waits_without_task_runner']+='Executor$TaskRunner.run' not in stack
        if 'Executor$TaskRunner.run' not in stack:
            unclassified[';'.join(stack.split(';')[:8])]+=1
    result={'pid':pid,'executor':executor,'worker_gpu_index':worker,'gpu_inventory':inventory[worker],
        'launch_command':records[worker+1]['command'][:3],
        'event_counts':dict(counts),'frame_counts':dict(frame_counts),
        'unclassified_top_stacks':unclassified.most_common(5),
        'gap_seconds':row['interval_totals_ns']['recorded_gpu_gap_ns']/1e9}
    results.append(result)
    print({k:v for k,v in result.items() if k not in ('unclassified_top_stacks','frame_counts')},flush=True)
(a.trace/'park-analysis/audit.json').write_text(json.dumps(results,indent=2)+'\n')
