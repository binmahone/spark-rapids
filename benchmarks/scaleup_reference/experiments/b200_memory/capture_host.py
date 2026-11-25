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

"""Record current B200 prerequisites and reject occupied selected GPUs."""
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys

work=Path(sys.argv[1]).resolve()
owner=json.loads((work/'evidence/owner.json').read_text())
assert owner['gpu_indices']==[0,1,2,3]
def command(*args):
    return subprocess.check_output(args,text=True,stderr=subprocess.STDOUT)
inventory=command('nvidia-smi','--query-gpu=index,uuid,name,memory.total,driver_version','--format=csv,noheader')
selected={row[1].strip() for row in csv.reader(io.StringIO(inventory)) if int(row[0]) in [0,1,2,3]}
processes=command('nvidia-smi','--query-compute-apps=gpu_uuid,pid,process_name','--format=csv,noheader')
if any(row and row[0].strip() in selected for row in csv.reader(io.StringIO(processes))):
    raise RuntimeError('A selected GPU already has a compute process')
report={'time_utc':datetime.now(timezone.utc).isoformat(),'gpu_indices':[0,1,2,3],
        'inventory':inventory,'preexisting_gpu_processes':processes,
        'topology':command('nvidia-smi','topo','-m'),'numa':command('numactl','--hardware'),
        'java':command(str(work/'jdk17/bin/java'),'-version'),
        'ucx':command(str(work/'ucx/bin/ucx_info'),'-v'),
        'filesystems':command('df','-hT','/','/raid',str(work)),
        'kernel':command('uname','-a'),'driver_params':Path('/proc/driver/nvidia/params').read_text(),
        'library_hashes':{str(p):hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted((work/'runtime/native').iterdir()) if not p.is_symlink()}}
(work/'evidence/host-prerequisites.json').write_text(json.dumps(report,indent=2)+'\n')
