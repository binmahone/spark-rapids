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
readonly prefix=${1:?Usage: prepare_spark.sh ABSOLUTE_DESTINATION}
test "${prefix:0:1}" = /
test ! -e "$prefix"
python3 -m venv "$prefix"
"$prefix/bin/pip" install pyspark==3.5.3
"$prefix/bin/python" -c 'from pathlib import Path; import pyspark; print(Path(pyspark.__file__).parent)'
