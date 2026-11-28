#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
readonly work=${1:?Prepared work directory}
readonly thread=${2:?Owner thread}
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly root="$work/native-host-metadata"
readonly sdk="$work/native-concat"
python3 - "$work" "$thread" <<'PY'
import json,pathlib,sys
w=pathlib.Path(sys.argv[1])
assert json.loads((w/'evidence/owner.json').read_text())['thread_id']==sys.argv[2]
for area in ('code-profile','data-path-trace'):
 assert all(json.loads(p.read_text())['status'] not in ('running','starting') for p in (w/area).glob('*/state.json'))
PY
[[ -z $(nvidia-smi --query-compute-apps=pid --format=csv,noheader) ]]
mkdir "$root"
exec > >(tee "$root/build.log") 2>&1
trap 'code=$?; printf "%s\n" "$code" > "$root/build.exit"' EXIT
common=(-std=c++20 -O3 -DNDEBUG --extended-lambda --expt-relaxed-constexpr \
  --default-stream per-thread -DCUDA_API_PER_THREAD_DEFAULT_STREAM=1 \
  -DCUDF_USE_PER_THREAD_DEFAULT_STREAM=1 -arch=sm_120 -ccbin g++-12 \
  -I"$sdk/include" -I"$sdk/cudf/cpp/include" -I"$sdk/CCCL/libcudacxx/include" \
  -I"$sdk/CCCL/thrust" -I"$sdk/CCCL/cub" -I"$sdk/rmm/cpp/include" \
  -I"$sdk/nvtx3/c/include" -I"$sdk/rapids_logger/include" -I"$sdk/spdlog/include" \
  -I"$work/jdk17/include" -I"$work/jdk17/include/linux" \
  -I"$sdk/cudf/java/src/main/native/include" -I"$sdk/cudf/java/src/main/native/src" -I"$here")
printf '%s\n' '{ global: Java_com_nvidia_spark_rapids_experiments_HostMetadataAllocationProbe_*; *rmm*; local: *; };' > "$root/exports.map"
/usr/local/cuda-13.1/bin/nvcc "${common[@]}" -shared -Xcompiler -fPIC \
  "$here/host_metadata_allocation_probe.cu" -L"$sdk/lib" -lcudf \
  -Xlinker -rpath -Xlinker "$sdk/lib" -Xlinker --version-script -Xlinker "$root/exports.map" \
  -Xlinker -z -Xlinker defs -ldl -lpthread -o "$root/libhost_metadata_probe.so"
mkdir "$root/classes"
readonly engine="$work/artifacts/decimal-null-copy/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar"
"$work/jdk17/bin/javac" --release 8 -cp "$engine" -d "$root/classes" "$here/HostMetadataAllocationProbe.java"
sha256sum "$root/libhost_metadata_probe.so" "$root/classes/com/nvidia/spark/rapids/experiments/HostMetadataAllocationProbe.class" > "$root/artifacts.sha256"
