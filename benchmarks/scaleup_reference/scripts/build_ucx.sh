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
readonly prefix=${1:?Usage: build_ucx.sh ABSOLUTE_INSTALL_PREFIX CUDA_HOME}
readonly cuda=${2:?CUDA_HOME is required}
readonly suite=$(cd "$(dirname "$0")/.." && pwd)
readonly patch="$suite/patches/ucx-1.22-interval-search.patch"
test "${prefix:0:1}" = /
readonly source_dir="${prefix}-source"
git clone --branch v1.22.0 https://github.com/openucx/ucx.git "$source_dir"
git -C "$source_dir" apply --check "$patch"
git -C "$source_dir" apply "$patch"
cd "$source_dir"
./autogen.sh
./configure --prefix="$prefix" --with-cuda="$cuda" --enable-mt --disable-logging \
  --disable-debug --disable-assertions --enable-gtest
make -j "${BUILD_JOBS:-8}"
make install
make -C test/gtest -j "${BUILD_JOBS:-8}" gtest
./test/gtest/gtest --gtest_filter='test_interval_tree.*'
sha256sum "$patch"
"$prefix/bin/ucx_info" -v
