#!/usr/bin/env python3
"""Reconstruct task and I/O scheduling intervals, including preempted runnable tasks."""
import argparse
from collections import Counter,defaultdict
import json
from pathlib import Path
import re
import statistics
import subprocess
from analyze_critical_path import schedule


class Intervals:
    def __init__(self, tids, emit):
        self.tids=set(tids);self.emit=emit
        self.running={};self.queued={};self.blocked={}
    def switch(self,stamp,prev,state,nxt):
        if prev in self.tids:
            if prev in self.running:self.emit(prev,'cpu',self.running.pop(prev),stamp)
            if state in ('R','R+'):
                self.queued[prev]=(stamp,'preempted_runnable')
            else:self.blocked[prev]=stamp
        if nxt in self.tids:
            if nxt in self.queued:
                start,kind=self.queued.pop(nxt);self.emit(nxt,kind,start,stamp)
            elif nxt in self.blocked:
                self.emit(nxt,'missing_wakeup',self.blocked.pop(nxt),stamp)
            self.running[nxt]=stamp
    def wake(self,stamp,tid):
        if tid not in self.tids:return
        if tid in self.blocked:self.emit(tid,'blocked_until_waking',self.blocked.pop(tid),stamp)
        self.queued.setdefault(tid,(stamp,'waking_to_cpu'))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--capture',type=Path,required=True)
    a=p.parse_args()
    state=json.loads((a.capture/'state.json').read_text());assert state['status']=='completed'
    identities=json.loads((a.capture/'executor-threads.json').read_text())
    owners={}
    for row in identities:
        for thread in row['threads']:
            role=('task' if thread['name'].startswith('Executor task') else
                  'native_io' if thread['name'].startswith('kvikio-') else None)
            if role:
                owners[thread['tid']]={'pid':row['pid'],'executor':row['executor'],'role':role,
                    'cpus_allowed':thread['cpus_allowed']}
    assert Counter(r['role'] for r in owners.values())=={'task':32,'native_io':256}
    q=schedule(Path(state['run']))['q9-iteration2']
    stages=[s for s in q['stages'] if s['tasks']==1152 and any('lineitem' in r['node'] for r in s['scan_batch_distribution'])]
    assert len(stages)==1
    shift=state['capture_start_unix_ns']-state['capture_start_monotonic_ns']
    begin=q['window']['start']*1_000_000+round(stages[0]['start_seconds']*1e9)-shift
    end=q['window']['start']*1_000_000+round(stages[0]['end_seconds']*1e9)-shift
    totals=defaultdict(Counter);durations=defaultdict(list);events=Counter()
    def emit(tid,kind,start,stop):
        assert stop>=start
        start=max(start,begin);stop=min(stop,end)
        if stop<=start:return
        totals[tid][kind]+=stop-start;durations[tid,kind].append(stop-start)
    tracker=Intervals(owners,emit)
    stamp_re=re.compile(r'^\s*(\d+)\.(\d+):\s+sched:(\w+):\s+(.*)$')
    switch_re=re.compile(r'prev_pid=(\d+).*?prev_state=(\S+).*?next_pid=(\d+)')
    wake_re=re.compile(r'\bpid=(\d+)')
    first=last=None;lost=[]
    command=['sudo','-n','perf','script','-i',str(a.capture/'sched.data'),'--ns',
        '--show-lost-events','-F','trace:time,event,trace']
    with (a.capture/'script-errors.log').open('w') as errors:
        process=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=errors,text=True)
        for line in process.stdout:
            if 'LOST' in line:lost.append(line.strip())
            match=stamp_re.match(line)
            if not match:continue
            seconds,fraction,kind,body=match.groups()
            stamp=int(seconds)*1_000_000_000+int(fraction.ljust(9,'0'))
            assert last is None or stamp>=last,(last,stamp)
            if first is None:first=stamp
            last=stamp;events[kind]+=1
            if kind=='sched_switch':
                m=switch_re.search(body);assert m,body
                prev,task_state,nxt=m.groups();tracker.switch(stamp,int(prev),task_state,int(nxt))
            elif kind in ('sched_waking','sched_wakeup_new'):
                m=wake_re.search(body);assert m,body
                tracker.wake(stamp,int(m[1]))
        assert process.wait()==0
    assert not lost,lost
    assert events['sched_switch']>0 and events['sched_waking']>0
    groups=defaultdict(Counter);threads=[]
    for tid,metadata in owners.items():
        stats=dict(totals[tid]);groups[metadata['cpus_allowed'],metadata['role']].update(stats)
        threads.append({'tid':tid,**metadata,'interval_totals_ns':stats,
            'observed_intervals':{kind:{'count':len(v),'median_ns':statistics.median(v),'max_ns':max(v)}
                for (owner,kind),v in durations.items() if owner==tid}})
    result={'events':dict(events),'lost_events':lost,'stage':stages[0]['stage'],
        'stage_monotonic_ns':[begin,end],'event_monotonic_ns':[first,last],
        'observed_scan_seconds':(min(end,last)-max(begin,first))/1e9,
        'groups':[{'cpus_allowed':key[0],'role':key[1],'interval_totals_ns':dict(value)}
            for key,value in sorted(groups.items())],'threads':threads,
        'limits':['All totals sum concurrent thread intervals and are not SQL wall seconds.',
            'Preempted runnable time uses both R and R+ sched_switch states.',
            'waking_to_cpu starts at sched_waking, before enqueue completion; it includes wakeup-path overhead.',
            'blocked_until_waking includes non-runnable waiting; this trace alone does not identify its user-code cause.',
            'Initial and final incomplete intervals are omitted; intervals are clipped to the lineitem stage.',
            'The target thread inventory was captured before tracing; new untracked threads would not contribute.']}
    (a.capture/'scheduler-analysis.json').write_text(json.dumps(result,indent=2)+'\n')
    for group in result['groups']:
        print(group['cpus_allowed'],group['role'],{k:round(v/1e9,4) for k,v in group['interval_totals_ns'].items()},flush=True)


if __name__=='__main__':main()
