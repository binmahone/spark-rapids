#!/usr/bin/env python3
"""Summarize measured page-statistics calls without converting thread time into SQL savings."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import statistics
from analyze_critical_path import schedule

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--work',type=Path,required=True)
p.add_argument('--label',required=True)
a=p.parse_args()
out=a.work/'native-page-stats'/a.label
trace=json.loads((out/'state.json').read_text())
query=json.loads((a.work/'code-profile'/a.label/'state.json').read_text())
assert trace['status']==query['status']=='completed'
rows=[]
text=(out/'trace.log').read_text()
assert 'lost' not in text.lower() and 'error' not in text.lower()
shift=trace['started_unix_ns']-trace['started_monotonic_ns']
for line in text.splitlines():
 if not line.startswith('PAGE_STATS,'):continue
 end,pid,tid,codec,pages,selection,duration=map(int,line.split(',')[1:])
 assert codec in (1,2,4,6,7) and selection in (0,1) and 0<=pages<100000000
 assert 0<duration<10000000000
 rows.append(dict(start_ns=end-duration+shift,end_ns=end+shift,pid=pid,tid=tid,
                  codec=codec,pages=pages,selection=selection,duration_ns=duration))
pids={int(v) for v in query['native_threads']}
assert {row['pid'] for row in rows}==pids

def summary(items):
 if not items:return {'calls':0,'sum_thread_seconds':0}
 return {'calls':len(items),'sum_thread_seconds':sum(r['duration_ns'] for r in items)/1e9,
         'median_call_us':statistics.median(r['duration_ns'] for r in items)/1e3,
         'p95_call_us':sorted(r['duration_ns'] for r in items)[int(.95*(len(items)-1))]/1e3,
         'max_call_ms':max(r['duration_ns'] for r in items)/1e6,
         'median_pages':statistics.median(r['pages'] for r in items),
         'max_pages':max(r['pages'] for r in items)}

result={'calls':len(rows),'executors':sorted(pids),'iterations':[],
        'limits':['Call durations include uprobe overhead and possible descheduling.',
                  'Summed thread time overlaps across executor tasks and is not SQL wall time.',
                  'Non-Snappy codecs are not labeled redundant without actual input codec proof.',
                  'Stage windows can overlap. Calls selected by a stage time interval are not necessarily from that stage.']}
timeline=schedule(Path(query['run']))
for sample in query['result']['samples']:
 begin,end=sample['started_unix_ns'],sample['finished_unix_ns']
 selected=[r for r in rows if r['start_ns']>=begin and r['end_ns']<=end]
 item={'query':sample['query'],'iteration':sample['iteration'],'diagnostic_sql_seconds':sample['sql_seconds'],
       'all':summary(selected),'by_codec':{str(c):summary([r for r in selected if r['codec']==c]) for c in (1,2,4,6,7)},
       'by_executor':{str(pid):summary([r for r in selected if r['pid']==pid]) for pid in sorted(pids)},
       'max_thread_sum_seconds':max(sum(r['duration_ns'] for r in selected if r['tid']==tid)/1e9 for tid in {r['tid'] for r in selected}),
       'stage_windows':[]}
 token=f"q{sample['query']}-iteration{sample['iteration']}"
 for stage in timeline[token]['stages']:
  if not stage['scan_batch_distribution']:continue
  b=timeline[token]['window']['start']*1000000+round(stage['start_seconds']*1e9)
  e=timeline[token]['window']['start']*1000000+round(stage['end_seconds']*1e9)
  selected_stage=[r for r in selected if r['start_ns']>=b and r['end_ns']<=e]
  item['stage_windows'].append({'stage':stage['stage'],'scan':stage['scan_batch_distribution'],
                                'start_seconds':stage['start_seconds'],'end_seconds':stage['end_seconds'],
                                'calls_in_time_window':summary(selected_stage)})
 result['iterations'].append(item)
(out/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
