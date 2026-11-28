#!/usr/bin/env python3
"""Apply and verify a per-thread scheduling policy on owned native I/O workers."""
import argparse
import json
import os
from pathlib import Path
import signal
import time

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--work',type=Path,required=True)
p.add_argument('--output',type=Path,required=True)
p.add_argument('--controller',type=Path,required=True)
p.add_argument('--label',required=True)
p.add_argument('--nice',type=int,choices=(0,5),required=True)
a=p.parse_args()
running=True;seen={};pids=set()
def stop(*args):
    global running
    running=False
signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
def fail_controller():
    owner=json.loads(a.controller.read_text());proc=Path('/proc')/str(owner['pid'])
    assert proc.stat().st_uid==owner['owner_uid']==os.getuid()
    assert (proc/'stat').read_text().rsplit(')',1)[1].split()[19]==str(owner['start_time'])
    assert b'run_code_profile.py' in (proc/'cmdline').read_bytes()
    os.kill(owner['pid'],signal.SIGTERM)
try:
    with a.output.open('x',buffering=1) as output:
        while running:
            for proc in Path('/proc').iterdir():
                if not proc.name.isdigit():continue
                try:
                    args=(proc/'cmdline').read_bytes().decode().split('\0')
                    if (proc.stat().st_uid!=os.getuid() or
                        'org.apache.spark.executor.CoarseGrainedExecutorBackend' not in args or
                        not any(a.label in arg for arg in args) or
                        not any(str(a.work) in arg for arg in args)):continue
                    pids.add(int(proc.name));assert len(pids)<=8
                    for task in (proc/'task').iterdir():
                        try:
                            name=(task/'comm').read_text().strip()
                            role=('native_io' if name.startswith('kvikio-') else
                                  'task' if name.startswith('Executor task') else None)
                            if role is None:continue
                            tid=int(task.name)
                            start=(task/'stat').read_text().rsplit(')',1)[1].split()[19]
                            desired=a.nice if role=='native_io' else 0
                            before=os.getpriority(os.PRIO_PROCESS,tid)
                            key=(int(proc.name),tid,start)
                            if key not in seen:
                                assert before==0,(key,name,before)
                                if role=='native_io':os.setpriority(os.PRIO_PROCESS,tid,desired)
                                after=os.getpriority(os.PRIO_PROCESS,tid);assert after==desired
                                row={'pid':int(proc.name),'tid':tid,'start_time':start,'name':name,
                                    'role':role,'nice_before':before,'nice_after':after,'unix_ns':time.time_ns()}
                                output.write(json.dumps(row)+'\n');seen[key]=desired
                            else:assert before==seen[key],(key,before,seen[key])
                        except (FileNotFoundError,ProcessLookupError):continue
                except (FileNotFoundError,ProcessLookupError,PermissionError):continue
            time.sleep(.5)
except BaseException:
    fail_controller();raise
