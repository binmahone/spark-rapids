#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
readonly work=${1:?Prepared work directory}
readonly thread=${2:?Owner thread}
readonly revision=${3:?Pinned candidate cuDF revision}
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly root="$work/native-concat"
mkdir -p "$root"
exec > >(tee -a "$root/build.log") 2>&1
trap 'code=$?; printf "%s\n" "$code" > "$root/build.exit"' EXIT
python3 "$here/prepare_native_concat.py" --work "$work" --thread-id "$thread" --cudf-revision "$revision"
readonly cuda=/usr/local/cuda-13.1
"$cuda/bin/nvcc" -std=c++20 -O3 -DNDEBUG --extended-lambda --expt-relaxed-constexpr \
  --default-stream per-thread -DCUDA_API_PER_THREAD_DEFAULT_STREAM=1 \
  -DCUDF_USE_PER_THREAD_DEFAULT_STREAM=1 -arch=sm_120 -ccbin g++-12 \
  -I"$root/include" -I"$root/cudf/cpp/include" -I"$root/CCCL/libcudacxx/include" \
  -I"$root/CCCL/thrust" -I"$root/CCCL/cub" -I"$root/rmm/cpp/include" \
  -I"$root/nvtx3/c/include" -I"$root/rapids_logger/include" -I"$root/spdlog/include" \
  "$root/candidate.cu" "$here/native_concat_probe.cu" \
  -L"$root/lib" -lcudf -Xlinker -rpath -Xlinker "$root/lib" \
  -ldl -lpthread -lrt -o "$root/native-concat-probe"
sha256sum "$root/native-concat-probe" > "$root/probe.sha256"
