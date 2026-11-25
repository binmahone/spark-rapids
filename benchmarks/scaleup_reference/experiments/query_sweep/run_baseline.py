#!/usr/bin/env python3
"""Run the unchanged 22-query reference workload without releasing the retained host."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--work', required=True)
args = parser.parse_args()
work = Path(args.work).resolve()
suite = Path(__file__).resolve().parents[2]
run = work / 'baseline'
if run.exists():
    raise RuntimeError('Baseline directory exists; inspect it before submitting another baseline')
os.environ.update(JAVA_HOME=str(work / 'jdk17'), PYSPARK_PYTHON='/usr/bin/python3')
owner = json.loads((work / 'evidence/owner.json').read_text())
os.environ['CODEX_THREAD_ID'] = owner['thread_id']
command = [sys.executable, str(suite / 'benchmark.py'), 'submit', '--platform', 'aws48x',
    '--deployment', str(work / 'evidence/deployment.json'), '--engine',
    str(work / 'artifacts/main/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar'), '--tools',
    str(work / 'artifacts/main/benchmark-tools.jar'), '--run', str(run)]
(work / 'evidence/baseline-command.json').write_text(json.dumps(command, indent=2) + '\n')
subprocess.run(command, check=True)
deadline = time.monotonic() + 7200
try:
    while True:
        path = run / 'summary.json'
        if path.exists():
            result = json.loads(path.read_text())
            stopped = list((run / 'services').glob('*/*.stopped.json'))
            if result['status'] in ('completed', 'failed') and stopped:
                break
        if time.monotonic() > deadline:
            raise TimeoutError('Full baseline exceeded its deadline')
        time.sleep(3)
    assert result['status'] == 'completed', result.get('error')
    assert len(result['samples']) == 44
    assert all(s['validation']['status'] == 'passed' for s in result['samples'])
    print(json.dumps({'status': 'completed', 'validated_actions': 44,
        'best_sql_sum_seconds': result['best_sql_sum_seconds'],
        'instance_policy': 'Retain until the user explicitly requests release'}), flush=True)
finally:
    subprocess.run([sys.executable, str(suite / 'benchmark.py'), 'stop', '--run', str(run)],
                   check=True, timeout=120)
