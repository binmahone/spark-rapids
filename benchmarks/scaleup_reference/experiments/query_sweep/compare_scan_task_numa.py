#!/usr/bin/env python3
"""Check physical NUMA/task-index patterns in completed unprofiled query controls."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import statistics
from urllib.parse import urlparse
from analyze_critical_path import schedule

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--work',type=Path,required=True)
p.add_argument('--labels',nargs='+',required=True)
p.add_argument('--output',type=Path,required=True)
a=p.parse_args()
results=[]
for label in a.labels:
    state=json.loads((a.work/'code-profile'/label/'state.json').read_text())
    assert state['status']=='completed'
    run=Path(state['run']);config=json.loads((run/'config.json').read_text())
    scratch=Path(config['deployment']['scratch_root'])/run.name/'suite'
    services=json.loads(next(path for path in (run/'services/suite').glob('*.json')
        if not path.name.endswith('.stopped.json')).read_text())
    mapping={}
    for stderr in scratch.glob('worker-*/work/*/*/stderr'):
        worker=int(stderr.parents[3].name.split('-')[1]);executor=stderr.parent.name
        numa=int(services[worker+1]['command'][1].split('=')[1]);mapping[executor]=numa
    assert len(mapping)==8
    chosen={}
    for token,q in schedule(run).items():
        scans=[s for s in q['stages'] if s['tasks']==1152 and any('lineitem' in r['node'] for r in s['scan_batch_distribution'])]
        assert len(scans)==1
        chosen[scans[0]['stage']]={'token':token,'begin':q['window']['start']+round(scans[0]['start_seconds']*1000),
            'span_seconds':scans[0]['span_seconds']}
    profile=json.loads((run/'applications/suite/profile.json').read_text());groups=defaultdict(list)
    for path in Path(urlparse(profile['spark.eventLog.dir']).path).iterdir():
        if not path.is_file() or path.name.startswith('.'):continue
        for line in path.open():
            event=json.loads(line)
            if event.get('Event')!='SparkListenerTaskEnd' or event['Stage ID'] not in chosen:continue
            stage=chosen[event['Stage ID']];info=event['Task Info'];metrics=event['Task Metrics']
            assert not info.get('Failed') and not info.get('Killed')
            row={'ms':info['Finish Time']-info['Launch Time'],'cpu_ms':metrics['Executor CPU Time']/1e6,
                'bytes':metrics['Input Metrics']['Bytes Read'],'partition':info['Index']}
            numa=mapping[info['Executor ID']]
            phase=('early' if info['Finish Time']<=stage['begin']+7555 else
                   'late' if info['Launch Time']>=stage['begin']+7555 else 'crossing')
            groups[stage['token'],numa,'time',phase].append(row)
            groups[stage['token'],numa,'partition',str(info['Index']//288)].append(row)
    rows=[]
    for key,values in sorted(groups.items()):
        rows.append({'query':key[0],'numa':key[1],'grouping':key[2],'group':key[3],
            'tasks':len(values),'median_elapsed_ms':statistics.median(r['ms'] for r in values),
            'median_cpu_ms':statistics.median(r['cpu_ms'] for r in values),
            'median_input_bytes':statistics.median(r['bytes'] for r in values),
            'elapsed_ms_per_gb':sum(r['ms'] for r in values)/(sum(r['bytes'] for r in values)/1e9)})
    results.append({'label':label,'stages':chosen,'rows':rows})
    print(label,[(r['numa'],r['group'],r['tasks'],r['median_elapsed_ms']) for r in rows
        if r['query']=='q9-iteration2' and r['grouping']=='time'],flush=True)
a.output.write_text(json.dumps({'runs':results,'limits':[
    'Unprofiled runs are compared using the first 7.555 seconds of each lineitem stage and partition-index quartiles.',
    'Task intervals and bytes are measured; CPU binding is from the retained launch arguments.',
    'Task sums are concurrent; NUMA association does not establish page locality or root cause.']},indent=2)+'\n')
