#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
set -eo pipefail
source /etc/profile.d/modules.sh
module load slurm/slurm/25.05
set -u
python3 - "${1:?run directory}" "${2:?thread id}" <<'PY'
import getpass,json,subprocess,sys
from pathlib import Path
root=Path(sys.argv[1])
plan=json.loads((root/'plan.json').read_text())
assert plan['thread_id']==sys.argv[2]
job=json.loads((root/'submission.json').read_text())['job_id']
active=subprocess.check_output(['squeue','-h','-u',getpass.getuser(),'-o','%i'],text=True).split()
accounting=subprocess.check_output(['sacct','-X','-j',str(job),'-n','-P',
    '--format=JobIDRaw,State,ExitCode,ElapsedRaw'],text=True).strip()
cases=[]
for case in sorted((root/'runs').iterdir()):
    owners=[p for p in case.glob('services/*/*.json') if not p.name.endswith('.stopped.json')]
    owners.extend(case.glob('applications/*/driver-service.json'))
    if not owners:continue
    missing=[str(p.relative_to(root)) for p in owners if not Path(str(p)+'.stopped.json').exists()]
    remaining=[]
    for path in owners:
        stop=Path(str(path)+'.stopped.json')
        if stop.exists():remaining.extend(json.loads(stop.read_text())['remaining_recorded_processes'])
    cases.append({'case':case.name,'service_ownership_receipts':len(owners),
                  'missing_stop_receipts':missing,'remaining_recorded_processes':remaining})
print(json.dumps({'job_id':job,'job_active':str(job) in active,'accounting':accounting,
                  'case_service_audits':cases},indent=2))
PY
