#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
set -euo pipefail
: "${PAUL_SCALA_RUN:?}" "${PAUL_SCALA_SUITE:?}" "${PAUL_SCALA_PHASE:?}"
mapfile -t settings < <(python3 - "$PAUL_SCALA_RUN/config.json" <<'PY'
import json,sys
x=json.load(open(sys.argv[1]))['deployment']
print(x['container_image'])
print(x['container_mounts']+','+x['gds_test_host_directory']+':/gds-tests')
print(x['container_setup'])
PY
)
export SCALEUP_CONTAINER_SETUP=${settings[2]}
srun --kill-on-bad-exit=1 --container-image="${settings[0]}" \
  --container-mounts="${settings[1]}" --container-remap-root --container-writable \
  bash "$PAUL_SCALA_SUITE/scripts/scala_test_phase.sh"
