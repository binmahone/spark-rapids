#!/usr/bin/env bash
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

set -euo pipefail
: "${SCALEUP_RUN:?}" "${SCALEUP_SUITE:?}"
mapfile -t nodes < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
export SCALEUP_HEAD=${nodes[0]}
export SCALEUP_MASTER_PORT=$((20000 + SLURM_JOB_ID % 10000))
export SCALEUP_MASTER_UI_PORT=$((SCALEUP_MASTER_PORT+10000))
mapfile -t container < <(python3 - "$SCALEUP_RUN/config.json" <<'PY'
import json,sys
x=json.load(open(sys.argv[1]))['deployment']
print(x['container_image'])
print(x['container_mounts'])
print(x.get('container_setup',''))
PY
)
export SCALEUP_CONTAINER_SETUP=${container[2]}
srun --kill-on-bad-exit=1 --container-image="${container[0]}" \
  --container-mounts="${container[1]}" --container-remap-root --container-writable \
  bash "$SCALEUP_SUITE/scripts/run_node.sh"
