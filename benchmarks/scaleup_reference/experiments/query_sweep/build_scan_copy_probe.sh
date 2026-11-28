#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
readonly work=${1:?Prepared work directory}
readonly thread=${2:?Owner thread}
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly root="$work/native-scan-copy"
readonly sdk="$work/native-concat"
[[ -z $(nvidia-smi --query-compute-apps=pid --format=csv,noheader) ]]
python3 "$here/prepare_scan_copy_probe.py" --work "$work" --thread-id "$thread"
exec > >(tee "$root/build.log") 2>&1
trap 'code=$?; printf "%s\n" "$code" > "$root/build.exit"' EXIT
common=(-std=c++20 -O3 -DNDEBUG --extended-lambda --expt-relaxed-constexpr \
  --default-stream per-thread -DCUDA_API_PER_THREAD_DEFAULT_STREAM=1 \
  -DCUDF_USE_PER_THREAD_DEFAULT_STREAM=1 -arch=sm_120 -ccbin g++-12 \
  -I"$sdk/include" -I"$sdk/cudf/cpp/include" -I"$sdk/CCCL/libcudacxx/include" \
  -I"$sdk/CCCL/thrust" -I"$sdk/CCCL/cub" -I"$sdk/rmm/cpp/include" \
  -I"$sdk/nvtx3/c/include" -I"$sdk/rapids_logger/include" -I"$sdk/spdlog/include" \
  -I"$work/jdk17/include" -I"$work/jdk17/include/linux" \
  -I"$sdk/cudf/java/src/main/native/include" -I"$sdk/cudf/java/src/main/native/src" \
  -I"$here" -I"$root")
printf '%s\n' '{ global: Java_com_nvidia_spark_rapids_experiments_ScanCopyProbe_*; *rmm*; local: *; };' \
  > "$root/exports.map"
/usr/local/cuda-13.1/bin/nvcc "${common[@]}" -shared -Xcompiler -fPIC \
  "$root/memory_utils.cu" "$root/MemoryUtilsJni.cpp" "$here/scan_copy_probe_jni.cpp" \
  -L"$sdk/lib" -lcudf -Xlinker -rpath -Xlinker "$sdk/lib" \
  -Xlinker --version-script -Xlinker "$root/exports.map" -Xlinker -z -Xlinker defs \
  -ldl -lpthread -lrt -o "$root/libscan_copy_probe.so"
mkdir "$root/classes" "$root/leases"
chmod 700 "$root/leases"
"$work/jdk17/bin/javac" --release 8 \
  -cp "$work/artifacts/decimal-null-copy/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar" \
  -d "$root/classes" "$here/ScanCopyProbe.java" "$here/ScanCopyProbeTest.java"
printf '%s\n' 'Premain-Class: com.nvidia.spark.rapids.experiments.ScanCopyProbe' '' \
  > "$root/agent-manifest.mf"
"$work/jdk17/bin/jar" cfm "$root/scan-copy-probe.jar" "$root/agent-manifest.mf" -C "$root/classes" .
sha256sum "$root/libscan_copy_probe.so" "$root/scan-copy-probe.jar" > "$root/artifacts.sha256"
python3 "$here/prepare_copy_pair_mapping.py" --work "$work"
