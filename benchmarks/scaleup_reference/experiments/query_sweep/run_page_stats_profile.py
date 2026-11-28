#!/usr/bin/env python3
"""Measure real Parquet page-statistics call durations with an owned uprobe capture."""
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
from common import read,sha,write
import services

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--work',type=Path,required=True)
p.add_argument('--thread-id',required=True)
p.add_argument('--label',required=True)
a=p.parse_args()
assert read(a.work/'evidence/owner.json')['thread_id']==a.thread_id
assert a.label.replace('-','').isalnum()
assert not subprocess.check_output(['nvidia-smi','--query-compute-apps=pid','--format=csv,noheader'],text=True).strip()
for area in ('code-profile','data-path-trace'):
 assert all(read(path)['status'] not in ('starting','running') for path in (a.work/area).glob('*/state.json'))
library=a.work/'native-concat/lib/libcudf.so'
assert sha(library)=='8a07e08432d9afbe80d1db24c69ce67e9a05048c703f7d65a1514dfc5620e9a3'
symbols=subprocess.check_output(['nm',str(library)],text=True)
match=[line.split() for line in symbols.splitlines() if '11codec_stats9add_pages' in line]
assert len(match)==1
symbol=match[0][2]
out=a.work/'native-page-stats'/a.label
out.mkdir(parents=True,exist_ok=False)
script=out/'page-stats.bt'
script.write_text('''interval:s:1 { if (@ready == 0) { printf("BPF_READY\\n"); @ready = 1; } }
uprobe:LIBRARY:SYMBOL {
  @started[tid] = nsecs;
  @codec[tid] = *(uint8*)arg0;
  @pages[tid] = *(uint64*)(reg("sp") + 0x28);
  @selection[tid] = arg1;
  @stats[tid] = arg0;
  @output[tid] = *(uint64*)(arg0 + 0x28);
}
uretprobe:LIBRARY:SYMBOL /@started[tid]/ {
  printf("PAGE_STATS,%llu,%d,%d,%llu,%llu,%llu,%llu,%llu\\n", nsecs, pid, tid,
         @codec[tid], @pages[tid], @selection[tid], nsecs - @started[tid],
         *(uint64*)(@stats[tid] + 0x28) - @output[tid]);
  delete(@started[tid]); delete(@codec[tid]); delete(@pages[tid]); delete(@selection[tid]);
  delete(@stats[tid]); delete(@output[tid]);
}
'''.replace('LIBRARY',str(library)).replace('SYMBOL',symbol))
state={'status':'starting','owner_thread':a.thread_id,'symbol':symbol,'symbol_address':match[0][0],
       'argument_layout':'x86_64 frozen ELF: pages.size at entry rsp+0x28; selection in rsi; codec at this+0; output at this+0x28',
       'library_sha256':sha(library),'started_unix_ns':time.time_ns(),'started_monotonic_ns':time.monotonic_ns(),
       'classification':'Diagnostic uprobe durations include instrumentation overhead; not benchmark scores.'}
write(out/'state.json',state)
log=(out/'trace.log').open('x')
command=['sudo','-n','bpftrace','-B','line',str(script)]
trace=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
trace_start=services.start_time(trace.pid)
state.update(trace_pid=trace.pid,trace_start_time=trace_start,command=command)
write(out/'state.json',state)
try:
 for _ in range(120):
  assert trace.poll() is None, 'Uprobe failed to attach'
  if 'BPF_READY' in (out/'trace.log').read_text(): break
  time.sleep(.5)
 else: raise TimeoutError('Uprobe attach did not finish')
 state['status']='recording';write(out/'state.json',state)
 subprocess.run(['bash',str(HERE/'start_code_profile.sh'),str(a.work),a.thread_id,a.label,
                 'control','9','','','','false','','0','','true'],check=True,timeout=600)
 assert trace.poll() is None
 assert read(a.work/'code-profile'/a.label/'state.json')['status']=='completed'
 state['status']='completed'
except BaseException as error:
 state.update(status='failed',error=repr(error));raise
finally:
 if trace.poll() is None:
  assert services.start_time(trace.pid)==trace_start and os.getpgid(trace.pid)==trace.pid
  subprocess.run(['sudo','-n','kill','-INT','--','-'+str(trace.pid)],check=True)
  trace.wait(timeout=30)
 log.close()
 state.update(trace_exit_code=trace.returncode,finished_unix_ns=time.time_ns())
 write(out/'state.json',state)
print(json.dumps(state),flush=True)
