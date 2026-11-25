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
bash "$here/start_suite.sh" "$work" topn-prefix 3,10 baseline prefix
bash "$here/start_suite.sh" "$work" all22-remainder 4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22 baseline
