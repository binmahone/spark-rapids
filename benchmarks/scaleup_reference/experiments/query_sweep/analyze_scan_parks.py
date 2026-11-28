#!/usr/bin/env python3
"""Compare actual Java park intervals with scheduled tasks and recorded CUDA gaps."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import sqlite3
import subprocess
from urllib.parse import urlparse

from analyze_critical_path import schedule
from analyze_data_path_trace import merged


def overlap_counts(tasks, parks, gaps):
    events=defaultdict(lambda:defaultdict(int))
    for key, intervals in [('scheduled',tasks),('gap',gaps),*parks.items()]:
        for start,end in intervals:
            if end>start:
                events[start][key]+=1
                events[end][key]-=1
    counts=defaultdict(int);totals=defaultdict(int);previous=None
    for stamp,updates in sorted(events.items()):
        if previous is not None and counts['gap']:
            elapsed=stamp-previous
            active=counts['scheduled']
            blocked=sum(counts[key] for key in parks)
            totals['recorded_gpu_gap_ns']+=elapsed
            if active==0:
                totals['no_scheduled_task_ns']+=elapsed
            elif blocked>active:
                totals['unresolved_park_count_exceeds_scheduled_ns']+=elapsed
            else:
                if counts['reader_input']:
                    totals['at_least_one_task_waiting_input_ns']+=elapsed
                if counts['reader_input']==active:
                    totals['all_scheduled_tasks_waiting_input_ns']+=elapsed
                if blocked==active:
                    totals['all_scheduled_tasks_parked_ns']+=elapsed
        for key,delta in updates.items():
            counts[key]+=delta
            assert counts[key]>=0,(stamp,key,counts[key])
        previous=stamp
    assert not any(counts.values())
    return dict(totals)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work',type=Path,required=True)
    p.add_argument('--trace',type=Path,required=True)
    a=p.parse_args()
    state=json.loads((a.trace/'state.json').read_text())
    assert state['status']=='completed' and state['park_events'] and not state['full_sql_trace']
    db=sqlite3.connect('file:'+str(a.trace/'q9.sqlite')+'?mode=ro',uri=True)
    epoch=db.execute('select utcEpochNs from TARGET_INFO_SESSION_START_TIME').fetchone()[0]
    activity=defaultdict(list)
    for table in ('CUPTI_ACTIVITY_KIND_KERNEL','CUPTI_ACTIVITY_KIND_MEMCPY','CUPTI_ACTIVITY_KIND_MEMSET'):
        for start,end,global_pid in db.execute('select start,end,globalPid from '+table):
            activity[(global_pid>>24)&0xffffff].append((start+epoch,end+epoch))
    db.close()
    identities=json.loads((a.trace/'executor-identities.json').read_text())
    assert len(identities)==8 and set(activity)=={r['pid'] for r in identities}
    first=min(b for spans in activity.values() for b,e in spans)+100_000_000
    last=max(e for spans in activity.values() for b,e in spans)-100_000_000
    candidates=[]
    for token,q in schedule(Path(state['run'])).items():
        for stage in q['stages']:
            if not any('lineitem' in row['node'] for row in stage['scan_batch_distribution']):continue
            begin=max(first,q['window']['start']*1_000_000+round(stage['start_seconds']*1e9))
            end=min(last,q['window']['start']*1_000_000+round(stage['end_seconds']*1e9))
            if end>begin:candidates.append((end-begin,token,stage['stage'],begin,end))
    assert candidates
    _,token,stage,begin,end=max(candidates)
    profile=json.loads((Path(state['run'])/'profiles.json').read_text())['suite']
    tasks=defaultdict(list)
    for path in Path(urlparse(profile['spark.eventLog.dir']).path).iterdir():
        if not path.is_file() or path.name.startswith('.'):continue
        assert not path.name.endswith('.inprogress')
        for line in path.open():
            event=json.loads(line)
            if event.get('Event')!='SparkListenerTaskEnd':continue
            info=event['Task Info']
            assert not info.get('Failed') and not info.get('Killed')
            start=max(begin,info['Launch Time']*1_000_000)
            stop=min(end,info['Finish Time']*1_000_000)
            if stop>start:tasks[info['Executor ID']].append((start,stop))
    out=a.trace/'park-analysis';out.mkdir()
    classes=out/'classes';classes.mkdir()
    here=Path(__file__).resolve().parent
    subprocess.run([a.work/'jdk17/bin/javac','--release','17','-d',classes,here/'ReadParkIntervals.java'],check=True)
    result={'query_window':token,'stage':stage,'begin_unix_ns':begin,'end_unix_ns':end,
            'window_seconds':(end-begin)/1e9,'executors':[],
            'limits':['Diagnostic timings are excluded from SOL scores.',
            'Recorded CUDA gaps are not proof of hardware idle; peer activity from other contexts may be absent.',
            'Only completed Java parks lasting at least 1 ms are recorded. Native waits are not covered.',
            'Task launch and finish timestamps have millisecond precision. Park intervals lose 1 ms at each edge.',
            'JFR thread names can be cached before Spark renames a task thread; identity uses process PID and executor ID.',
            'All-task comparisons use all scheduled stages on the executor, not only the lineitem stage.',
            'An input wait identifies the awaited reader future, not the internal cause delaying that future.']}
    for identity in identities:
        pid=identity['pid'];executor=identity['executor_id']
        intervals=merged([(max(b,begin),min(e,end)) for b,e in activity[pid] if e>begin and b<end])
        gaps=[];cursor=begin
        for b,e in intervals:
            if b>cursor:gaps.append((cursor,b))
            cursor=max(cursor,e)
        if cursor<end:gaps.append((cursor,end))
        files=list((a.trace/'park-recordings').glob(f'hotspot-pid-{pid}-*.jfr'));assert len(files)==1
        output=out/f'parks-{pid}.jsonl'
        subprocess.run([a.work/'jdk17/bin/java','-cp',classes,
            'com.nvidia.spark.rapids.experiments.ReadParkIntervals',files[0],str(begin),str(end),output],check=True)
        by_thread=defaultdict(list);stats=defaultdict(int)
        for line in output.read_text().splitlines():
            park=json.loads(line);stack=park['stack']
            stats['all_park_events']+=1
            if 'Executor$TaskRunner.run' not in stack:continue
            stats['task_park_events']+=1
            category=('reader_input' if 'MultiFileCloudPartitionReaderBase.getNextBuffersAndMeta' in stack
                      and 'FutureTask.get' in stack else 'gpu_semaphore' if 'GpuSemaphore' in stack else 'other_task')
            b=max(begin,park['start']+1_000_000);e=min(end,park['end']-1_000_000)
            if e>b:by_thread[(park['os_tid'],category)].append((b,e))
        parks={'reader_input':[],'gpu_semaphore':[],'other_task':[]}
        for (_,category),spans in by_thread.items():parks[category].extend(merged(spans))
        assert stats['task_park_events']>0 and tasks[executor]
        measured=overlap_counts(tasks[executor],parks,gaps)
        result['executors'].append({**identity,**stats,'interval_totals_ns':measured})
        print(pid,{key:round(value/1e9,4) for key,value in measured.items()},flush=True)
    (out/'summary.json').write_text(json.dumps(result,indent=2)+'\n')


if __name__=='__main__':main()
