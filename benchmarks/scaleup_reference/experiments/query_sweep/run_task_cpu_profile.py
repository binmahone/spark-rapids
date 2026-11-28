#!/usr/bin/env python3
"""Record instruction samples on owned executor threads during repeated Q9."""
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
from common import read,write,sha
import services

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--work',type=Path,required=True)
p.add_argument('--thread-id',required=True)
p.add_argument('--label',default='scan-task-cpu-A1')
p.add_argument('--scope',choices=('tasks','executors'),default='tasks')
p.add_argument('--delay-seconds',type=float,default=0)
p.add_argument('--duration-seconds',type=float,default=20)
p.add_argument('--frequency',type=int,default=499)
a=p.parse_args();assert a.label.replace('-','').isalnum()
assert 0<=a.delay_seconds<=25 and 1<=a.duration_seconds<=40 and 1<=a.frequency<=499
assert read(a.work/'evidence/owner.json')['thread_id']==a.thread_id
assert not subprocess.check_output(['nvidia-smi','--query-compute-apps=pid','--format=csv,noheader'],text=True).strip()
for area in ('code-profile','data-path-trace'):
    assert all(read(path)['status'] not in ('starting','running') for path in (a.work/area).glob('*/state.json'))
scratch=Path(read(a.work/'evidence/deployment.json')['scratch_root'])/'task-cpu-profiles'/a.label
scratch.mkdir(parents=True,exist_ok=False)
assert subprocess.check_output(['findmnt','-no','TARGET','-T',str(scratch)],text=True).strip()!='/'
assert os.statvfs(scratch).f_bavail*os.statvfs(scratch).f_frsize>30*2**30
native=a.work/'native-concat/lib/libcudf.so'
assert sha(native)=='8a07e08432d9afbe80d1db24c69ce67e9a05048c703f7d65a1514dfc5620e9a3'
state={'status':'starting','owner_thread':a.thread_id,'label':a.label,
    'classification':'Task CPU instruction sampling; excluded from benchmark scores',
    'library_sha256':sha(native),'source_revision':subprocess.check_output(
        ['git','-C',str(HERE),'rev-parse','HEAD'],text=True).strip()}
write(scratch/'state.json',state)
def interrupt(signum,frame):raise InterruptedError(str(signum))
for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,interrupt)
command=['bash',str(HERE/'start_code_profile.sh'),str(a.work),a.thread_id,a.label,
    'control','9','','','','false','','0','','true']
query,record=services.launch(command,scratch/'query.log',dict(os.environ))
write(scratch/'query-service.json',[record]);perf=None
try:
    run=a.work/'screen'/('code-profile-'+a.label+'-0')
    result=run/'applications/suite/result.json';deadline=time.monotonic()+240
    while not result.exists() or len(read(result).get('samples',[]))<1:
        assert query.poll() is None and time.monotonic()<deadline
        time.sleep(.1)
    assert len(read(result)['samples'])==1
    ready_monotonic=time.monotonic()
    identities=[]
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():continue
        try:
            args=(proc/'cmdline').read_bytes().decode().split('\0')
            if (proc.stat().st_uid!=os.getuid() or
                'org.apache.spark.executor.CoarseGrainedExecutorBackend' not in args or
                not any(a.label in arg for arg in args)):continue
            tids=[]
            for task in (proc/'task').iterdir():
                try:
                    name=(task/'comm').read_text().strip()
                    if name.startswith('Executor task'):
                        tids.append({'tid':int(task.name),'name':name,
                            'start_time':services.start_time(int(task.name))})
                except (FileNotFoundError,ProcessLookupError):continue
            assert len(tids)==4,(proc.name,tids)
            maps=(proc/'maps').read_text()
            cudf=[line.split() for line in maps.splitlines() if 'libcudf.so' in line]
            assert cudf and all(Path(row[5]).resolve()==native.resolve() and
                int(row[4])==native.stat().st_ino for row in cudf)
            identities.append({'pid':int(proc.name),'executor':args[args.index('--executor-id')+1],
                'start_time':services.start_time(int(proc.name)),'threads':tids,'maps':maps})
        except (FileNotFoundError,ProcessLookupError):continue
    assert len(identities)==8
    write(scratch/'executor-threads.json',identities)
    tids=[str(t['tid']) for row in identities for t in row['threads']]
    target=(['-t',','.join(tids),'--no-inherit'] if a.scope=='tasks' else
            ['-p',','.join(str(row['pid']) for row in identities)])
    remaining=a.delay_seconds-(time.monotonic()-ready_monotonic)
    if remaining>0:time.sleep(remaining)
    assert query.poll() is None and len(read(result)['samples'])==1
    perf_command=['sudo','-n','perf','record','-e','cpu-clock:u','-F',str(a.frequency),*target,'--clockid','mono','--no-buildid-cache','--call-graph','dwarf,16384',
        '-m','512','-o',str(scratch/'task-cpu.data'),'--','sleep',str(a.duration_seconds)]
    state.update(status='recording',scope=a.scope,delay_seconds=a.delay_seconds,perf_command=perf_command,
        capture_start_unix_ns=time.time_ns(),capture_start_monotonic_ns=time.monotonic_ns())
    write(scratch/'state.json',state)
    with (scratch/'perf-record.log').open('x') as stream:
        perf=subprocess.Popen(perf_command,stdout=stream,stderr=subprocess.STDOUT,start_new_session=True)
        perf_start=services.start_time(perf.pid);assert perf.wait(timeout=60)==0
    state.update(capture_end_unix_ns=time.time_ns(),capture_end_monotonic_ns=time.monotonic_ns())
    assert query.wait(timeout=180)==0
    trial=read(a.work/'code-profile'/a.label/'state.json');assert trial['status']=='completed'
    assert len(trial['result']['samples'])==2 and all(r['validation']['status']=='passed' for r in trial['result']['samples'])
    with (scratch/'self-report.txt').open('x') as stream:
        subprocess.run(['sudo','-n','perf','report','-i',str(scratch/'task-cpu.data'),
            '--stdio','--no-children','--call-graph','none','--sort','dso,symbol','--percent-limit','0.1','--column-widths','30,120'],
            stdout=stream,stderr=subprocess.STDOUT,check=True,timeout=120)
    state.update(status='completed',run=str(run),query_samples=trial['result']['samples'],
        perf_data_bytes=(scratch/'task-cpu.data').stat().st_size)
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
