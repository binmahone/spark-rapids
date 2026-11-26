#!/usr/bin/env python3
"""Start an isolated trial only after its predecessor has stopped successfully."""
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import time

root = Path(os.environ['B200_PROBE_ROOT']).resolve()
owner = json.loads((root/'evidence/owner.json').read_text())
assert owner['thread_id'] == os.environ['CODEX_THREAD_ID']
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--after', required=True)
parser.add_argument('--label', required=True)
parser.add_argument('--queries', required=True)
parser.add_argument('--arms', required=True)
parser.add_argument('--gpus', default='0,3,4,6')
args = parser.parse_args()
for name in (args.after, args.label):
    assert re.fullmatch('[A-Za-z0-9_-]+', name)
ssh = ['ssh', '-F', '/home/nvidia/mahone_gluten/cluster_ssh_config',
       '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8', 'mahonem@10.34.1.4']
remote = owner['remote']
marker = remote + '/evidence/' + args.after + '/controller.exit'
deadline = time.monotonic() + 3600
while True:
    check = subprocess.run(ssh + ['test -f ' + shlex.quote(marker) +
        ' && cat ' + shlex.quote(marker)], text=True, capture_output=True, timeout=30)
    if check.returncode == 0:
        if check.stdout.strip() != '0':
            raise RuntimeError('Predecessor failed: ' + check.stdout.strip())
        break
    if check.returncode != 1 or time.monotonic() >= deadline:
        raise RuntimeError('Cannot verify completed predecessor: ' + check.stderr)
    time.sleep(5)
subprocess.run(ssh + ['git -C ' + shlex.quote(remote + '/source') + ' pull --ff-only'], check=True)
script = remote + '/source/benchmarks/scaleup_reference/experiments/b200_memory/start_suite.sh'
command = ['bash', script, remote, args.label, args.queries, args.arms, 'measure', args.gpus]
log = remote + '/evidence/' + args.label + '-launch.log'
launch = 'nohup ' + shlex.join(command) + ' > ' + shlex.quote(log) + ' 2>&1 < /dev/null & echo $!'
started = subprocess.run(ssh + [launch], check=True, capture_output=True, text=True)
receipt = dict(vars(args), pid=int(started.stdout.strip()), remote_log=log,
               thread_id=owner['thread_id'])
(root/'evidence'/('queue-' + args.label + '.json')).write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt), flush=True)
