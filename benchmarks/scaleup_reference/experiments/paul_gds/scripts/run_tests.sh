#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
set -euo pipefail
: "${PAUL_GDS_RUN:?}" "${PAUL_GDS_SUITE:?}"
bash "$SCALEUP_CONTAINER_SETUP"
export DEBIAN_FRONTEND=noninteractive
apt-get install -y --no-install-recommends python3-venv zip
readonly local=/var/tmp/paul-gds-tests-$SLURM_JOB_ID
mkdir -p "$local"
python3 -m venv "$local/venv"
export PATH="$local/venv/bin:$PATH"
readonly repository=$(cd "$PAUL_GDS_SUITE/../../../.." && pwd)
python -m pip install -r "$repository/integration_tests/requirements.txt" 'numpy<2' 'pandas<3' 'pytest<9'
python -m pip freeze > "$PAUL_GDS_RUN/python-packages.txt"
python - "$local/cufile.json" <<'PY'
import json,re,sys
from pathlib import Path
x=json.loads(re.sub(r'//[^\n]*','',Path('/etc/cufile.json').read_text()))
Path(sys.argv[1]).write_text(json.dumps(x,indent=2)+'\n')
PY
exec python "$PAUL_GDS_SUITE/gds_tests.py" node --run "$PAUL_GDS_RUN"
