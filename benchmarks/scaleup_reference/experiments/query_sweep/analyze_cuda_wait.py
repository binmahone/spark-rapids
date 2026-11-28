#!/usr/bin/env python3
"""Compare CUDA waiting policies with fixed SQL, native code and scan work."""
import argparse
import hashlib
import json
from pathlib import Path
from analyze_critical_path import pressure, schedule
from finalize_sol_reuse import normalize

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--work',type=Path,required=True)
p.add_argument('--labels',nargs='+',required=True)
p.add_argument('--output',type=Path,required=True)
a=p.parse_args();reference=None;runs=[]
for label in a.labels:
    directory=a.work/'code-profile'/label
    state=json.loads((directory/'state.json').read_text())
    assert state['status']=='completed' and state['mode']=='control'
    assert state['frozen_native_directory'] and state['pinned_copy_threshold']==0
    assert not any(state.get(k) for k in ('codec_library','concat_probe','scan_copy_probe',
        'packed_view','shuffle_copy_probe','nvcomp_policy','mmap_policy','native_io_priority'))
    policy=state['cuda_wait_policy'];mode=policy['mode']
    assert len(policy['runtime_proof'])==8 and len(policy['runtime_module_mappings'])==8
    assert all(r['flags']&7==(4 if mode=='blocking' else 0) for r in policy['runtime_proof'])
    run=Path(state['run']);config=json.loads((run/'config.json').read_text())
    profiles=json.loads((run/'profiles.json').read_text())
    agent='-agentpath:'+str(a.work/'native-cuda-wait-policy/libcuda_wait_policy.so')+'='+mode
    for profile in profiles.values():
        for key,value in profile.items():
            if isinstance(value,str):
                value=value.replace(run.name,'<run>')
                if key in ('spark.driver.extraJavaOptions','spark.executor.extraJavaOptions'):
                    assert agent in value
                    value=value.replace(agent,agent.rsplit('=',1)[0]+'=<policy>')
                profile[key]=value
    timeline=schedule(run);parity=[];samples=[]
    for sample in state['result']['samples']:
        assert sample['validation']['status']=='passed'
        token=f"q{sample['query']}-iteration{sample['iteration']}"
        plan=normalize((run/'applications/suite'/(token+'.executed.txt')).read_text())
        fixed={k:sample[k] for k in ('query','iteration','sql_sha256','case_conf','reader_metrics')}
        fixed['plan_sha256']=hashlib.sha256(plan.encode()).hexdigest();parity.append(fixed)
        samples.append({**fixed,**{k:sample[k] for k in ('sql_seconds','cleanup_seconds',
            'sql_plus_cleanup_seconds')},'pressure':pressure(directory,timeline[token]['window']),
            'schedule':timeline[token],'plan_metrics':sample['plan_metrics']})
    fixed={'engine_sha256':config['engine_sha256'],'profiles':profiles,'rows':parity,
        'probe_artifacts':policy['manifest']['artifacts']}
    if reference is None:reference=fixed
    else:
        for key in fixed:assert fixed[key]==reference[key],(label,key)
    runs.append({'label':label,'mode':mode,'samples':samples})
a.output.write_text(json.dumps({'runs':runs,
    'parity':'Engine, native library, instrumentation, SQL, normalized plans and scan work match; CUDA context wait scheduling differs.',
    'limits':['Native method checks verify actual cuDF JNI synchronization contexts in all eight executors.',
        'The agent skips the driver; it verifies a process-local primary context before activation.',
        'Cache residency is observed, not reset. Inspect storage reads and memory pressure.',
        'Task CPU sums and operator time sums overlap and are not SQL wall seconds.']},indent=2)+'\n')
