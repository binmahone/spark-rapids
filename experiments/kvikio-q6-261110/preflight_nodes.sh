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
ssh -F /home/nvidia/mahone_gluten/cluster_ssh_config -o ConnectTimeout=12 \
  presto-cp-02-via-mac bash -s <<'REMOTE'
set -uo pipefail
for suffix in 04 07 08 10 13 14 15 16 17; do
  node=presto-gb200-gcn-$suffix
  ssh -o BatchMode=yes -o ConnectTimeout=8 "$node" \
    'hostname; timeout 8 stat -c "dataset_size=%s" /scratch/prestouser/tpch-rs-float-no-delta/scale-30000/lineitem/part.0.parquet;
    timeout 8 dd if=/scratch/prestouser/tpch-rs-float-no-delta/scale-30000/lineitem/part.0.parquet of=/dev/null bs=16 count=1 status=none;
    df -h /raid /tmp; nvidia-smi --query-gpu=name,memory.total,memory.used --format=csv,noheader'
done
REMOTE
