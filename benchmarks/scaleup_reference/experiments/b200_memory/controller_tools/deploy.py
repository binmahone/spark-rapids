#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Stage committed sources and frozen artifacts without using the full RAID filesystem."""
import json
import os
from pathlib import Path
import shlex
import subprocess

ROOT = Path(os.environ['B200_PROBE_ROOT']).resolve()
REMOTE = '/tmp/mahonem-b200-261125-01a21183'
ssh = ['ssh', '-F', os.environ['B200_SSH_CONFIG'],
       '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8', 'mahonem@10.34.1.4']
thread = os.environ['CODEX_THREAD_ID']
assert thread == '01a21183-bc74-7a30-be37-7b3b0d57c292'
probe = subprocess.check_output(ssh + ['python3 -c ' + shlex.quote(
    'import os,socket,json; s=os.statvfs("/tmp"); '
    'print(json.dumps(dict(host=socket.gethostname(),free=s.f_bavail*s.f_frsize,uid=os.getuid())))')], text=True)
inventory = json.loads(probe)
assert inventory['host'] == 'umb-b200-220' and inventory['free'] > 256 * 2**30
subprocess.run(ssh + [f'test ! -e {REMOTE} && mkdir -m 700 {REMOTE} && '
    f'mkdir {REMOTE}/evidence {REMOTE}/artifacts && '
    'git clone --depth 1 --branch experiment/b200-runtime-prefilter-261125 '
    f'https://github.com/binmahone/spark-rapids.git {REMOTE}/source'], check=True)
owner = {'thread_id': thread, 'host': inventory['host'], 'remote': REMOTE,
         'gpu_indices': [0, 1, 2, 3], 'filesystem_free_before_bytes': inventory['free'],
         'minimum_free_bytes': 256 * 2**30, 'classification': 'B200 SF10K float diagnostic'}
program = 'from pathlib import Path\nimport sys\nPath(sys.argv[1]).write_text(sys.stdin.read())\n'
subprocess.run(ssh + ['python3 -c ' + shlex.quote(program) + ' ' +
    shlex.quote(REMOTE + '/evidence/owner.json')], input=json.dumps(owner, indent=2), text=True, check=True)
artifacts = Path(os.environ['B200_PROBE_ARTIFACTS']).resolve()
for name in ['cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar', 'benchmark-tools.jar', 'manifest.json']:
    subprocess.run(['scp', '-F', os.environ['B200_SSH_CONFIG'],
        str(artifacts / name), 'mahonem@10.34.1.4:' + REMOTE + '/artifacts/'], check=True)
subprocess.run(ssh + [f'nohup bash {REMOTE}/source/benchmarks/scaleup_reference/experiments/'
    f'b200_memory/prepare.sh {REMOTE} > {REMOTE}/evidence/prepare-launch.log 2>&1 < /dev/null &'], check=True)
(ROOT / 'evidence/owner.json').write_text(json.dumps(owner, indent=2) + '\n')
print(json.dumps(owner, indent=2), flush=True)
