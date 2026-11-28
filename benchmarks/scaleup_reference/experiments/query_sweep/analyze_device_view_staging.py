#!/usr/bin/env python3
"""Compare device-view staging with fixed SQL, native code and scan work."""
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
    assert not state['frozen_native_directory'] and state['pinned_copy_threshold']==0
    assert not any(state.get(k) for k in ('codec_library','concat_probe','scan_copy_probe',
        'packed_view','shuffle_copy_probe','nvcomp_policy','mmap_policy','native_io_priority','queue_index_probe','cuda_wait_policy'))
    policy=state['device_view_staging'];mode=policy['mode']
    assert len(policy['runtime_proof'])==8 and len(policy['runtime_module_mappings'])==8
    run=Path(state['run']);config=json.loads((run/'config.json').read_text())
    profiles=json.loads((run/'profiles.json').read_text())
    for profile in profiles.values():
        for key,value in profile.items():
            if isinstance(value,str):
                value=value.replace(run.name,'<run>')
                if key=='spark.executorEnv.LIBCUDF_DEVICE_VIEW_STAGING':
                    assert value==mode
                    value='<policy>'
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
        'native_artifact':policy['manifest']['library_sha256'],'runtime_dependencies':policy['runtime_dependencies']}
    if reference is None:reference=fixed
    else:
        for key in fixed:assert fixed[key]==reference[key],(label,key)
    runs.append({'label':label,'mode':mode,'samples':samples})
a.output.write_text(json.dumps({'runs':runs,
    'parity':'Engine, native library, SQL, normalized plans and scan work match; device-view staging mode differs.',
    'limits':['Actual table and column staging branch markers and native library mappings are checked in all eight executors.',
        'Both arms use the same native ELF and first-call proof instrumentation. The control arm preserves the original allocation and copy behavior.',
        'Cache residency is observed, not reset. Inspect storage reads and memory pressure.',
        'Task CPU sums and operator time sums overlap and are not SQL wall seconds.']},indent=2)+'\n')
