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
readonly work=${1:?Owned work directory is required}
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly gpus=0,3,5,6
bash "$here/start_suite.sh" "$work" trial-q9-0356 9 baseline,row-propagation,row-propagation,baseline measure "$gpus"
bash "$here/start_suite.sh" "$work" trial-preagg-0356 10,3 baseline,preaggregate,preaggregate,baseline measure "$gpus"
bash "$here/start_suite.sh" "$work" trial-q15-0356 15 baseline,aggregate-reuse,aggregate-reuse,baseline measure "$gpus"
