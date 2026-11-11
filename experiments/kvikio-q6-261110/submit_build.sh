#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly revision=$(git -C "$here" rev-parse HEAD)
readonly node=${1:-presto-gb200-gcn-04}
test -z "$(git -C "$here" status --short)"
ssh -F /home/nvidia/mahone_gluten/cluster_ssh_config -o ConnectTimeout=12 \
  presto-cp-02-via-mac bash -s -- "$revision" "$node" <<'REMOTE'
set -euo pipefail
export SLURM_CONF=/cm/shared/apps/slurm/etc/slurm/slurm.conf
export KVIKIO_SOURCE_REVISION=$1
readonly root=/home/mahonem/paul_q6_kvikio_261110
readonly source=$root/src/cudf-spark
readonly branch=test/paul-q6-kvikio-261110
mkdir -p "$root/src" "$root/logs"
if [[ ! -d "$source/.git" ]]; then
  git clone --filter=blob:none --single-branch --branch "$branch" \
    https://github.com/binmahone/spark-rapids.git "$source"
else
  test -z "$(git -C "$source" status --short)"
  git -C "$source" pull --ff-only
fi
test "$(git -C "$source" rev-parse HEAD)" = "$KVIKIO_SOURCE_REVISION"
/cm/local/apps/slurm/current/bin/sbatch --parsable --nodelist="$2" \
  --output="$root/logs/slurm-%j.out" --error="$root/logs/slurm-%j.err" \
  --export=ALL,KVIKIO_SOURCE_REVISION "$source/experiments/kvikio-q6-261110/build.slurm"
REMOTE
