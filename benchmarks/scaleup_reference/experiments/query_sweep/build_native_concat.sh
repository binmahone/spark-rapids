#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
readonly work=${1:?Prepared work directory}
readonly thread=${2:?Owner thread}
readonly revision=${3:?Pinned candidate cuDF revision}
readonly mode=${4:-all}
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly root="$work/native-concat"
mkdir -p "$root"
rm -f "$root/build.exit"
exec > >(tee -a "$root/build.log") 2>&1
trap 'code=$?; printf "%s\n" "$code" > "$root/build.exit"' EXIT
if [[ "$mode" == all ]]; then
  python3 "$here/prepare_native_concat.py" --work "$work" --thread-id "$thread" --cudf-revision "$revision"
else
  [[ "$mode" == agent ]]
  [[ $(git -C "$root/cudf" rev-parse HEAD) == "$revision" ]]
  [[ -z $(nvidia-smi --query-compute-apps=pid --format=csv,noheader) ]]
fi
readonly cuda=/usr/local/cuda-13.1
common=(-std=c++20 -O3 -DNDEBUG --extended-lambda --expt-relaxed-constexpr \
  --default-stream per-thread -DCUDA_API_PER_THREAD_DEFAULT_STREAM=1 \
  -DCUDF_USE_PER_THREAD_DEFAULT_STREAM=1 -arch=sm_120 -ccbin g++-12 \
  -I"$root/include" -I"$root/cudf/cpp/include" -I"$root/CCCL/libcudacxx/include" \
  -I"$root/CCCL/thrust" -I"$root/CCCL/cub" -I"$root/rmm/cpp/include" \
  -I"$root/nvtx3/c/include" -I"$root/rapids_logger/include" -I"$root/spdlog/include")
if [[ "$mode" == all ]]; then
"$cuda/bin/nvcc" "${common[@]}" -Xcompiler -fPIC -c "$root/candidate.cu" -o "$root/candidate.o"
"$cuda/bin/nvcc" "${common[@]}" -Xcompiler -fPIC -c \
  "$root/cudf/cpp/src/table/table_device_view.cu" -o "$root/table_device_view.o"
"$cuda/bin/nvcc" "${common[@]}" "$root/candidate.o" "$root/table_device_view.o" \
  "$here/native_concat_probe.cu" \
  -L"$root/lib" -lcudf -Xlinker -rpath -Xlinker "$root/lib" \
  -ldl -lpthread -lrt -o "$root/native-concat-probe"
sha256sum "$root/native-concat-probe" > "$root/probe.sha256"
fi
printf '%s\n' '{ global: Java_com_nvidia_spark_rapids_experiments_ConcatOffsetProbe_*; *rmm*; local: *; };' \
  > "$root/exports.map"
"$cuda/bin/nvcc" "${common[@]}" -shared -Xcompiler -fPIC \
  -I"$work/jdk17/include" -I"$work/jdk17/include/linux" \
  -I"$root/cudf/java/src/main/native/include" -I"$root/cudf/java/src/main/native/src" \
  "$root/candidate.o" "$root/table_device_view.o" "$here/concat_offset_probe_jni.cpp" \
  -L"$root/lib" -lcudf -Xlinker -rpath -Xlinker "$root/lib" \
  -Xlinker --version-script -Xlinker "$root/exports.map" -Xlinker -z -Xlinker defs \
  -ldl -lpthread -lrt -o "$root/lib/libconcat_offsets_probe.so"
mkdir -p "$root/classes"
"$work/jdk17/bin/javac" --release 8 \
  -cp "$work/artifacts/decimal-null-copy/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar" \
  -d "$root/classes" "$here/ConcatOffsetProbe.java" "$here/ConcatOffsetProbeTest.java"
printf '%s\n' 'Premain-Class: com.nvidia.spark.rapids.experiments.ConcatOffsetProbe' '' \
  > "$root/agent-manifest.mf"
"$work/jdk17/bin/jar" cfm "$root/concat-offset-probe.jar" "$root/agent-manifest.mf" -C "$root/classes" .
sha256sum "$root/lib/libconcat_offsets_probe.so" "$root/concat-offset-probe.jar" > "$root/agent.sha256"
