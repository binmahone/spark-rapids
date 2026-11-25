#!/usr/bin/env python3
"""Stop only the recorded sweep services; leave the host and data available."""
import argparse
import os
from pathlib import Path
import signal
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read
import services

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--work', required=True)
args = parser.parse_args()
work = Path(args.work).resolve()
for controller in [work / name / 'controller.json' for name in ['screen', 'confirmation', 'followups']]:
    if not controller.exists():
        continue
    record = read(controller)
    try:
        if (record['owner_uid'] != os.getuid() or
                services.start_time(record['pid']) != record['start_time']):
            raise RuntimeError('Controller ownership does not match')
        os.kill(record['pid'], signal.SIGTERM)
    except (FileNotFoundError, ProcessLookupError):
        pass
for root in [work / 'baseline', work / 'screen']:
    for state in root.glob('**/driver-service.json'):
        services.stop(state)
    for state in root.glob('**/services/*/*.json'):
        if not state.name.endswith('.stopped.json'):
            services.stop(state)
print('Recorded sweep Java services stopped; EC2 instance was retained')
