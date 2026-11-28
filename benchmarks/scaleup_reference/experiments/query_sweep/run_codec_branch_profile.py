#!/usr/bin/env python3
"""Count real page-statistics entries in paired libraries without decoding C++ arguments."""
import argparse
from collections import Counter
import json
import os
from pathlib import Path
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, sha, write
import services

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', type=Path, required=True)
p.add_argument('--thread-id', required=True)
p.add_argument('--label', required=True)
p.add_argument('--implementation', choices=('baseline', 'candidate'), required=True)
p.add_argument('--layout', choices=('standalone', 'integrated'), default='standalone')
a = p.parse_args()
assert read(a.work / 'evidence/owner.json')['thread_id'] == a.thread_id
assert a.label.replace('-', '').isalnum()
assert not subprocess.check_output(
    ['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader'], text=True).strip()
for area in ('code-profile', 'data-path-trace'):
    assert all(read(path)['status'] not in ('starting', 'running')
               for path in (a.work / area).glob('*/state.json'))
native_root = 'native-codec-stack' if a.layout == 'integrated' else 'native-codec-library'
artifact = read(a.work / native_root / (a.implementation + '-state.json'))
assert artifact['status'] == 'completed'
library = a.work / native_root / a.implementation / 'libcudf.so'
assert sha(library) == artifact['library_sha256']
symbols = subprocess.check_output(['nm', '--defined-only', str(library)], text=True)
matches = [line.split() for line in symbols.splitlines()
           if '11codec_stats9add_pages' in line and '.cold' not in line]
assert len(matches) == 1, matches
symbol = matches[0][2]
out = a.work / 'native-page-stats' / a.label
out.mkdir(parents=True, exist_ok=False)
with (out / 'function-disassembly.txt').open('x') as stream:
    subprocess.run(['objdump', '-d', '--disassemble=' + symbol, str(library)],
                   stdout=stream, stderr=subprocess.STDOUT, check=True)
script = out / 'entry-count.bt'
script.write_text('''interval:s:1 { if (@ready == 0) { printf("BPF_READY\\n"); @ready = 1; } }
uprobe:LIBRARY:SYMBOL { printf("PAGE_ENTRY,%llu,%d\\n", nsecs, pid); }
'''.replace('LIBRARY', str(library)).replace('SYMBOL', symbol))
state = {'status': 'starting', 'owner_thread': a.thread_id, 'implementation': a.implementation,
         'library_sha256': artifact['library_sha256'], 'symbol': symbol,
         'symbol_address': matches[0][0], 'started_unix_ns': time.time_ns(),
         'started_monotonic_ns': time.monotonic_ns(),
         'classification': 'Diagnostic call counts; query durations are not benchmark scores.'}
write(out / 'state.json', state)
log = (out / 'trace.log').open('x')
command = ['sudo', '-n', 'bpftrace', '-B', 'line', str(script)]
trace = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
trace_start = services.start_time(trace.pid)
state.update(command=command, trace_pid=trace.pid, trace_start_time=trace_start)
write(out / 'state.json', state)
try:
    for _ in range(120):
        assert trace.poll() is None, 'Entry probe failed to attach'
        if 'BPF_READY' in (out / 'trace.log').read_text():
            break
        time.sleep(.5)
    else:
        raise TimeoutError('Entry probe attachment did not finish')
    state['status'] = 'recording'
    write(out / 'state.json', state)
    subprocess.run(['bash', str(HERE / 'start_code_profile.sh'), str(a.work), a.thread_id,
                    a.label, 'control', '9', '', '', '', 'false', '', '0', '', 'false',
                    a.implementation, a.layout], check=True, timeout=600)
    assert trace.poll() is None
    query = read(a.work / 'code-profile' / a.label / 'state.json')
    assert query['status'] == 'completed'
    state['status'] = 'completed'
except BaseException as error:
    state.update(status='failed', error=repr(error))
    raise
finally:
    if trace.poll() is None:
        assert services.start_time(trace.pid) == trace_start and os.getpgid(trace.pid) == trace.pid
        subprocess.run(['sudo', '-n', 'kill', '-INT', '--', '-' + str(trace.pid)], check=True)
        trace.wait(timeout=30)
    log.close()
    state.update(trace_exit_code=trace.returncode, finished_unix_ns=time.time_ns())
    write(out / 'state.json', state)

text = (out / 'trace.log').read_text()
assert 'lost' not in text.lower() and 'error' not in text.lower()
shift = state['started_unix_ns'] - state['started_monotonic_ns']
entries = []
for line in text.splitlines():
    if line.startswith('PAGE_ENTRY,'):
        _, stamp, pid = line.split(',')
        entries.append((int(stamp) + shift, int(pid)))
pids = {int(pid) for pid in query['native_threads']}
assert len(pids) == 8 and {pid for _, pid in entries} == pids
samples = []
for sample in query['result']['samples']:
    assert sample['validation']['status'] == 'passed'
    counts = Counter(pid for stamp, pid in entries
                     if sample['started_unix_ns'] <= stamp <= sample['finished_unix_ns'])
    assert set(counts) == pids
    samples.append({'query': sample['query'], 'iteration': sample['iteration'],
                    'calls': sum(counts.values()), 'by_executor': dict(counts),
                    'sql_sha256': sample['sql_sha256'], 'reader_metrics': sample['reader_metrics'],
                    'validation': sample['validation']})
write(out / 'summary.json', {'implementation': a.implementation, 'library_sha256': sha(library),
                            'samples': samples,
                            'limits': ['Counts include out-of-line function entries only.',
                                       'No C++ arguments or durations are decoded.',
                                       'A lower call count does not establish a query speedup.']})
print(json.dumps(samples), flush=True)
