#!/usr/bin/env python3
"""Audit recorded experiment process groups without stopping or releasing the host."""
import argparse
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write
import services

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--work', required=True)
args = parser.parse_args()
work = Path(args.work).resolve()
assert (work / 'evidence/confirmation.exit').read_text().strip() == '0'
assert read(work / 'confirmation/state.json')['status'] == 'completed'
states = []
for root in [work / 'baseline', work / 'screen']:
    states.extend(root.glob('**/driver-service.json'))
    states.extend(path for path in root.glob('**/services/*/*.json')
                  if not path.name.endswith('.stopped.json'))
records = [{'state': str(path.relative_to(work)), 'receipt': record,
            'live_members': services.group_members(record)}
           for path in states for record in read(path)]
gpu = subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid,process_name,used_memory',
                               '--format=csv,noheader'], text=True).strip()
report = {'state_files': len(states), 'process_groups': len(records),
          'live_recorded_groups': [r for r in records if r['live_members']],
          'gpu_compute_processes': gpu.splitlines() if gpu else [],
          'scope': 'Recorded experiment Java process groups; no instance release operation',
          'instance_policy': 'Retain until explicit user release'}
write(work / 'evidence/sweep-lifecycle-audit.json', report)
assert not report['live_recorded_groups'], 'Owned experiment services remain alive'
print(report)
