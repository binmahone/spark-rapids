#!/usr/bin/env python3
"""Retain resource-error observations from completed experiment application logs."""
import argparse
from collections import Counter
import json
from pathlib import Path
import re

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--work', required=True)
args = parser.parse_args()
work = Path(args.work).resolve()
groups = []
for group in sorted((work / 'screen').glob('ceiling-*')):
    result_path = group / 'applications/suite/result.json'
    if not result_path.exists():
        continue
    result = json.loads(result_path.read_text())
    if result['status'] not in ('completed', 'failed'):
        continue
    config = json.loads((group / 'config.json').read_text())
    scratch = Path(config['deployment']['scratch_root']) / group.name
    files = {p for p in group.rglob('*.log') if p.is_file() and not p.is_symlink()}
    files.update(p for p in scratch.rglob('*') if p.is_file() and not p.is_symlink()
                 and (p.name in ('stdout', 'stderr') or p.suffix == '.log'))
    counts, observations = Counter(), []
    for path in sorted(files):
        shutdown_seen = False
        for line in path.open(errors='replace'):
            shutdown_seen |= 'Driver commanded a shutdown' in line
            kind = ('cuda_buffer_leak' if 'A CUDA BUFFER WAS LEAKED' in line else
                    'oom_or_retry_log_match' if re.search(r'OutOfMemory|SplitAndRetry|RetryOOM', line) else None)
            if kind:
                counts[kind] += 1
                if kind == 'cuda_buffer_leak' and shutdown_seen:
                    counts['cuda_buffer_leak_after_shutdown_request'] += 1
                observations.append({'file': str(path), 'kind': kind,
                    'after_shutdown_request_in_same_log': shutdown_seen, 'line': line.strip()})
    groups.append({'run': str(group.relative_to(work)), 'status': result['status'],
        'files_checked': len(files), 'log_bytes_checked': sum(p.stat().st_size for p in files),
        'counts': dict(counts), 'observations': observations})
report = {'classification': 'Observed completed-log messages, not an allocation root-cause diagnosis',
    'groups': groups,
    'limits': ['A successful result check does not establish leak-free shutdown',
               'OOM/retry keyword matches are not necessarily fatal failures',
               'Process exit and retained GPU allocations require the separate lifecycle audit']}
output = work / 'evidence/resource-log-audit.json'
output.write_text(json.dumps(report, indent=2) + '\n')
for group in groups:
    print(group['run'], group['counts'])
