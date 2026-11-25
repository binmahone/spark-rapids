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
readonly target=/opt/dlami/nvme/daily-trend-hardened/dataset
test "$(findmnt -n -T /opt/dlami/nvme -o TARGET)" = /opt/dlami/nvme
readonly suite=$(cd "$(dirname "$0")/.." && pwd)
if [[ -d "$target" ]]; then
  python3 "$suite/scripts/dataset.py" --dataset "$target"
  exit 0
fi
readonly staging="${target}.scaleup-download"
mkdir -p "$(dirname "$target")"
exec 9>"${staging}.lock"
flock -n 9
mkdir -p "$staging"
aws s3 sync s3://haozhu-nf-us-east-2/TPC-H-sf10k-decimal/ "$staging/" \
  --region us-east-2 --only-show-errors --delete
python3 "$suite/scripts/dataset.py" --dataset "$staging"
test ! -e "$target"
mv -T "$staging" "$target"
