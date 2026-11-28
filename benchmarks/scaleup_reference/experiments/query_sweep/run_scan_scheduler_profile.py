#!/usr/bin/env python3
"""Capture bounded kernel scheduler evidence during a repeated Q9 action."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parents[1]/'scripts'))
from common import read,write
import services

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--work',type=Path,required=True)
p.add_argument('--thread-id',required=True)
p.add_argument('--label',default='scan-scheduler-A1')
a=p.parse_args()
assert a.label.replace('-','').isalnum()
assert read(a.work/'evidence/owner.json')['thread_id']==a.thread_id
assert not subprocess.check_output(['nvidia-smi','--query-compute-apps=pid','--format=csv,noheader'],text=True).strip()
for area in ('code-profile','data-path-trace'):
    assert all(read(path)['status'] not in ('starting','running') for path in (a.work/area).glob('*/state.json'))
scratch=Path(read(a.work/'evidence/deployment.json')['scratch_root'])/'scheduler-profiles'/a.label
scratch.mkdir(parents=True,exist_ok=False)
assert subprocess.check_output(['findmnt','-no','TARGET','-T',str(scratch)],text=True).strip()!='/'
assert os.statvfs(scratch).f_bavail*os.statvfs(scratch).f_frsize>30*2**30
state={'status':'starting','owner_thread':a.thread_id,'label':a.label,
    'classification':'Diagnostic scheduler tracing; excluded from benchmark scores',
    'start_unix_ns':time.time_ns(),'start_monotonic_ns':time.monotonic_ns()}
write(scratch/'state.json',state)
def interrupt(signum,frame):raise InterruptedError(str(signum))
for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,interrupt)
command=['bash',str(HERE/'start_code_profile.sh'),str(a.work),a.thread_id,a.label,
    'control','9','','','','false','','0','','true']
query,record=services.launch(command,scratch/'query.log',dict(os.environ))
write(scratch/'query-service.json',[record])
perf=None
try:
    run=a.work/'screen'/('code-profile-'+a.label+'-0')
    result=run/'applications/suite/result.json'
    deadline=time.monotonic()+240
    while not result.exists() or len(read(result).get('samples',[]))<1:
        assert query.poll() is None
        assert time.monotonic()<deadline
        time.sleep(.1)
    assert len(read(result)['samples'])==1
    identities=[];snapshot_errors=[]
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():continue
        try:
            args=(proc/'cmdline').read_bytes().decode().split('\0')
            if (proc.stat().st_uid!=os.getuid() or
                'org.apache.spark.executor.CoarseGrainedExecutorBackend' not in args or
                not any(a.label in arg for arg in args)):continue
            threads=[]
            for task in (proc/'task').iterdir():
                try:
                    status=dict(line.split(':',1) for line in (task/'status').read_text().splitlines() if ':' in line)
                    threads.append({'tid':int(task.name),'name':(task/'comm').read_text().strip(),
                        'cpus_allowed':status['Cpus_allowed_list'].strip()})
                except (FileNotFoundError,ProcessLookupError):
                    snapshot_errors.append({'tid':task.name,'error':'Thread exited during snapshot'})
            try:pages=(proc/'numa_maps').read_text()
            except PermissionError:
                pages=subprocess.check_output(['sudo','-n','cat',str(proc/'numa_maps')],text=True)
            identities.append({'pid':int(proc.name),'executor':args[args.index('--executor-id')+1],
                'start_time':services.start_time(int(proc.name)),'threads':threads,
                'numa_maps':pages})
        except (FileNotFoundError,ProcessLookupError,PermissionError) as error:
            snapshot_errors.append({'pid':proc.name,'error':repr(error)})
    write(scratch/'snapshot-errors.json',snapshot_errors)
    write(scratch/'executor-threads.json',identities)
    assert len(identities)==8,[(r['pid'],r['executor']) for r in identities]
    for phase in ('before',):
        write(scratch/(phase+'-numa.json'),{str(path):path.read_text() for path in Path('/sys/devices/system/node').glob('node*/meminfo')})
    state.update(status='recording',capture_start_unix_ns=time.time_ns(),capture_start_monotonic_ns=time.monotonic_ns())
    perf_command=['sudo','-n','perf','sched','record','-a','-B','-N','-m','512',
        '--clockid','mono','-o',str(scratch/'sched.data'),'--','sleep','15']
    state['perf_command']=perf_command;write(scratch/'state.json',state)
    with (scratch/'perf-record.log').open('x') as stream:
        perf=subprocess.Popen(perf_command,stdout=stream,stderr=subprocess.STDOUT,start_new_session=True)
        perf_start=services.start_time(perf.pid)
        assert perf.wait(timeout=60)==0
    state.update(capture_end_unix_ns=time.time_ns(),capture_end_monotonic_ns=time.monotonic_ns())
    assert query.wait(timeout=180)==0
    trial=read(a.work/'code-profile'/a.label/'state.json');assert trial['status']=='completed'
    assert len(trial['result']['samples'])==2 and all(r['validation']['status']=='passed' for r in trial['result']['samples'])
    write(scratch/'after-numa.json',{str(path):path.read_text() for path in Path('/sys/devices/system/node').glob('node*/meminfo')})
    for row in identities:
        with (scratch/f"timehist-{row['pid']}.txt").open('x') as stream:
            subprocess.run(['sudo','-n','perf','sched','timehist','-i',str(scratch/'sched.data'),
                '--summary','--pid',str(row['pid'])],stdout=stream,stderr=subprocess.STDOUT,check=True,timeout=90)
    state.update(status='completed',run=str(run),query_samples=trial['result']['samples'],
        perf_data_bytes=(scratch/'sched.data').stat().st_size)
except BaseException as error:
    state.update(status='failed',error=repr(error));raise
finally:
    if perf is not None and perf.poll() is None:
        assert services.start_time(perf.pid)==perf_start
        subprocess.run(['sudo','-n','kill','-INT','--','-'+str(perf.pid)],check=True)
        perf.wait(timeout=30)
    if query.poll() is None:
        controller=a.work/'code-profile'/a.label/'controller.json'
        if controller.exists():
            c=read(controller);proc=Path('/proc')/str(c['pid'])
            if proc.exists() and services.start_time(c['pid'])==c['start_time']:
                assert proc.stat().st_uid==os.getuid();os.kill(c['pid'],signal.SIGTERM)
        try:query.wait(timeout=30)
        except subprocess.TimeoutExpired:services.stop(scratch/'query-service.json')
    state['finished_unix_ns']=time.time_ns();write(scratch/'state.json',state)
print(scratch,flush=True)
