#!/usr/bin/env python3
"""Capture module mappings once for each owned executor without hashing during SQL."""
import argparse
import json
import os
from pathlib import Path
import signal
import time

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--work',type=Path,required=True)
p.add_argument('--output',type=Path,required=True)
p.add_argument('--expected-directory',type=Path,required=True)
a=p.parse_args()
expected=a.expected_directory/'libcudf.so'
expected_stat=expected.stat()
running=True
seen=set()

def stop(*args):
 global running
 running=False

signal.signal(signal.SIGTERM,stop)
signal.signal(signal.SIGINT,stop)
with a.output.open('x',buffering=1) as output:
 while running:
  for proc in Path('/proc').iterdir():
   if not proc.name.isdigit() or proc.name in seen:continue
   try:
    command=(proc/'cmdline').read_bytes()
    if proc.stat().st_uid!=os.getuid() or str(a.work).encode() not in command or b'CoarseGrainedExecutorBackend' not in command:continue
    lines=(proc/'maps').read_text().splitlines()
    selected=[line for line in lines if any(name in line for name in ('libcudf.so','libstdc++.so','libgcc_s.so','libz.so'))]
    cudf=[line.split() for line in selected if 'libcudf.so' in line]
    if not cudf:continue
    assert all(int(row[4])==expected_stat.st_ino and Path(row[5]).resolve()==expected.resolve() for row in cudf),selected
    output.write(json.dumps({'pid':int(proc.name),'start_time':(proc/'stat').read_text().rsplit(')',1)[1].split()[19],
                            'expected_library':str(expected),'expected_inode':expected_stat.st_ino,
                            'unix_ns':time.time_ns(),'maps':selected})+'\n')
    seen.add(proc.name)
   except (FileNotFoundError,ProcessLookupError,PermissionError):continue
  time.sleep(.5)
