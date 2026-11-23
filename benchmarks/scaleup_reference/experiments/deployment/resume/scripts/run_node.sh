#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
set -euo pipefail
: "${SCALEUP_RUN:?}"
if [[ -n "${SCALEUP_CONTAINER_SETUP:-}" ]]; then
  bash "$SCALEUP_CONTAINER_SETUP"
fi
unset SCALEUP_CONTAINER_SETUP
readonly directory=$(cd "$(dirname "$0")/.." && pwd)
exec python3 "$directory/control.py" node --run "$SCALEUP_RUN"
