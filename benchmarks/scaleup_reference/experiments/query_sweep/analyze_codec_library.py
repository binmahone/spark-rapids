#!/usr/bin/env python3
"""Compare paired native libraries while checking SQL, plans, runtime modules and scan work."""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
from analyze_critical_path import pressure,schedule
from finalize_sol_reuse import normalize

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--work',type=Path,required=True)
p.add_argument('--labels',nargs='+',required=True)
p.add_argument('--output',type=Path,required=True)
a=p.parse_args()
reference=None
runs=[]
for label in a.labels:
 directory=a.work/'code-profile'/label
 state=json.loads((directory/'state.json').read_text())
 assert state['status']=='completed' and state['mode']=='control'
 assert not any(state.get(key) for key in ('concat_probe','scan_copy_probe','packed_view','shuffle_copy_probe','nvcomp_policy','mmap_policy'))
 assert state['pinned_copy_threshold']==0 and len(state['pinned_copy_environment_proof'])==8
 artifact=state['codec_library'];assert artifact['status']=='completed'
 assert len(artifact['runtime_module_mappings'])==8
 run=Path(state['run'])
 config=json.loads((run/'config.json').read_text())
 profiles=json.loads((run/'profiles.json').read_text())
 native_root='native-codec-stack' if artifact.get('layout')=='integrated' else 'native-codec-library'
 expected=a.work/('native-concat/lib' if artifact['action']=='bridge' else native_root+'/'+artifact['action'])
 for profile in profiles.values():
  for key,value in profile.items():
   if isinstance(value,str):
    value=value.replace(run.name,'<run>')
    if key.endswith('.extraJavaOptions'):
     arguments=shlex.split(value)
     selected=[arg for arg in arguments if arg.startswith('-Dai.rapids.cudf.lib-native-dir=')]
     assert selected==['-Dai.rapids.cudf.lib-native-dir='+str(expected)]
     value=' '.join(arg for arg in arguments if arg not in selected)
    profile[key]=value
 timeline=schedule(run);parity=[];rows=[]
 for sample in state['result']['samples']:
  assert sample['validation']['status']=='passed'
  token=f"q{sample['query']}-iteration{sample['iteration']}"
  plan=normalize((run/'applications/suite'/(token+'.executed.txt')).read_text())
  fixed={key:sample[key] for key in ('query','iteration','sql_sha256','case_conf','reader_metrics')}
  fixed['plan_sha256']=hashlib.sha256(plan.encode()).hexdigest();parity.append(fixed)
  row={**fixed,**{key:sample[key] for key in ('sql_seconds','cleanup_seconds','sql_plus_cleanup_seconds')},
       'pressure':pressure(directory,timeline[token]['window']),'schedule':timeline[token],
       'plan_metrics':sample['plan_metrics']}
  rows.append(row)
  print(label,token,round(row['sql_seconds'],3),row['pressure'],flush=True)
 fixed={'engine_sha256':config['engine_sha256'],'profiles':profiles,'rows':parity,
        'runtime_dependencies':artifact['runtime_dependencies']}
 if reference is None:reference=fixed
 else:
  for key in fixed:assert fixed[key]==reference[key],(label,key)
 runs.append({'label':label,'implementation':artifact['action'],'native_artifact':artifact,'samples':rows})
rebuilt=[r['native_artifact'] for r in runs if r['implementation']!='bridge']
if rebuilt:
 assert all(r['configuration']==rebuilt[0]['configuration'] for r in rebuilt)
 assert all(r['compiler_package_list_sha256']==rebuilt[0]['compiler_package_list_sha256'] for r in rebuilt)
a.output.write_text(json.dumps({'runs':runs,
 'parity':'Frozen Spark/JNI, SQL, normalized plans, scan work and runtime dependencies match; only the selected libcudf differs.',
 'limits':['Rebuilt control and candidate share a compiler and dependency configuration; the frozen library bridge tests the build transition.',
           'Cache residency is observed, not reset. Inspect storage reads and memory pressure.',
           'Native module mappings prove library identity, not execution of every changed branch.',
           'Task and operator time sums overlap and are not SQL wall seconds. Focused trials are not a suite score.']},indent=2)+'\n')
