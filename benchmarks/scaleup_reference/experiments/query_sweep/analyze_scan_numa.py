#!/usr/bin/env python3
"""Compare retained scan task work and latency across physical NUMA groups."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import statistics
from urllib.parse import urlparse

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--trace',type=Path,required=True)
a=p.parse_args()
state=json.loads((a.trace/'state.json').read_text())
summary=json.loads((a.trace/'park-analysis/summary.json').read_text())
audit=json.loads((a.trace/'park-analysis/audit.json').read_text())
mapping={row['executor']:row for row in audit}
profile=json.loads((Path(state['run'])/'applications/suite/profile.json').read_text())
tasks=defaultdict(list)
for path in Path(urlparse(profile['spark.eventLog.dir']).path).iterdir():
    if not path.is_file() or path.name.startswith('.'):continue
    assert not path.name.endswith('.inprogress')
    for line in path.open():
        event=json.loads(line)
        if event.get('Event')!='SparkListenerTaskEnd' or event['Stage ID']!=summary['stage']:continue
        info=event['Task Info'];m=event['Task Metrics']
        assert not info.get('Failed') and not info.get('Killed')
        tasks[info['Executor ID']].append({'task_id':info['Task ID'],'partition':info['Index'],
            'start':info['Launch Time']*1_000_000,'end':info['Finish Time']*1_000_000,
            'elapsed_ms':info['Finish Time']-info['Launch Time'],
            'run_ms':m['Executor Run Time'],'cpu_ns':m['Executor CPU Time'],
            'input_bytes':m.get('Input Metrics',{}).get('Bytes Read',0),
            'input_records':m.get('Input Metrics',{}).get('Records Read',0)})
result=[]
for executor,rows in sorted(tasks.items()):
    row=mapping[executor]
    inside=[r for r in rows if r['start']>=summary['begin_unix_ns'] and r['end']<=summary['end_unix_ns']]
    output={'executor':executor,'gpu_index':row['worker_gpu_index'],
        'numa_cpu_binding':row['launch_command'][1],'whole_stage_tasks':len(rows),
        'whole_stage_input_bytes':sum(r['input_bytes'] for r in rows),
        'inside_window_tasks':len(inside),'inside_window_input_bytes':sum(r['input_bytes'] for r in inside),
        'gpu_gap_seconds':row['gap_seconds'],'task_latency':[]}
    for key,group in [('whole_stage',rows),('inside_window',inside)]:
        assert group
        output['task_latency'].append({'scope':key,'median_elapsed_ms':statistics.median(r['elapsed_ms'] for r in group),
            'median_input_bytes':statistics.median(r['input_bytes'] for r in group),
            'median_input_records':statistics.median(r['input_records'] for r in group),
            'median_cpu_ms':statistics.median(r['cpu_ns']/1e6 for r in group),
            'elapsed_ms_per_gb':sum(r['elapsed_ms'] for r in group)/(sum(r['input_bytes'] for r in group)/1e9)})
    result.append(output);print(output,flush=True)
(a.trace/'park-analysis/numa-task-work.json').write_text(json.dumps({'executors':result,'tasks':tasks,
    'limits':['Tasks crossing capture edges are excluded from the inside-window byte counts.',
              'GPU assignment and NUMA launch arguments are retained; actual thread affinity and host-page locality were not captured in this run.',
              'Task elapsed time includes concurrent work and waits; it is not query wall time.']},indent=2)+'\n')
