#!/usr/bin/env bash
set -euo pipefail
readonly prefix=${1:?Install prefix is required}
readonly suite=${2:?Reference suite is required}
readonly cuda=${3:?CUDA home is required}
readonly patch="$suite/patches/ucx-1.22-interval-search.patch"
readonly source_dir="${prefix}-source"
if test ! -d "$source_dir"; then
    git clone --branch v1.22.0 https://github.com/openucx/ucx.git "$source_dir"
fi
test "$(git -C "$source_dir" describe --tags --exact-match HEAD)" = v1.22.0
if ! git -C "$source_dir" apply --reverse --check "$patch"; then
    git -C "$source_dir" apply --check "$patch"
    git -C "$source_dir" apply "$patch"
fi
cd "$source_dir"
./autogen.sh
./configure --prefix="$prefix" --with-cuda="$cuda" --enable-mt --disable-logging \
  --disable-debug --disable-assertions --enable-gtest
make -j "${BUILD_JOBS:-16}"
make install
make -C test/gtest -j "${BUILD_JOBS:-16}" gtest
./test/gtest/gtest --gtest_filter='test_interval_tree.*'
sha256sum "$patch"
"$prefix/bin/ucx_info" -v
