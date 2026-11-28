#!/usr/bin/env python3
"""Audit retained JFR stack coverage and map executor differences to physical workers."""
import argparse
from collections import Counter
import json
from pathlib import Path
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
records=json.loads(next(path for path in (run/'services/suite').glob('*.json')
    if not path.name.endswith('.stopped.json')).read_text())
out=a.trace/'park-audit';out.mkdir(exist_ok=True)
classes=out/'classes';classes.mkdir(exist_ok=True)
subprocess.run([a.work/'jdk17/bin/javac','--release','17','-d',classes,
    Path(__file__).with_name('ReadParkIntervals.java')],check=True)
results=[]
for row in s['executors']:
    pid=row['pid'];executor=row['executor_id']
    logs=list(scratch.glob(f'worker-*/work/*/{executor}/stderr'));assert len(logs)==1
    worker=int(logs[0].parents[3].name.split('-')[1])
    recording=next((a.trace/'park-recordings').glob(f'hotspot-pid-{pid}-*.jfr'))
    parsed=out/f'parks-{pid}.jsonl'
    subprocess.run([a.work/'jdk17/bin/java','-cp',classes,
        'com.nvidia.spark.rapids.experiments.ReadParkIntervals',recording,
        str(s['begin_unix_ns']),str(s['end_unix_ns']),parsed],check=True)
    counts=Counter();frame_counts=Counter();unclassified=Counter()
    for line in parsed.read_text().splitlines():
        event=json.loads(line);stack=event['stack']
        counts['events']+=1;counts['truncated']+=event['truncated']
        frame_counts[event['frame_count']]+=1
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
