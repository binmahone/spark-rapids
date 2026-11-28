#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
ulimit -c 0
readonly work=${1:?Prepared work directory}
readonly kind=${2:?scan or concat}
readonly mode=${3:?Native probe mode}
readonly label=${4:?Unique result label}
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly output="$work/evidence/allocator-$kind-$mode-$label"
[[ ! -e "$output.log" ]]
[[ -z $(nvidia-smi --query-compute-apps=pid --format=csv,noheader) ]]
mkdir -p "$output.classes"
readonly engine="$work/artifacts/decimal-null-copy/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar"
"$work/jdk17/bin/javac" --release 8 -cp "$engine" -d "$output.classes" "$here/ProbeAllocatorTest.java"
export CUDA_VISIBLE_DEVICES
CUDA_VISIBLE_DEVICES=$(nvidia-smi --id=0 --query-gpu=uuid --format=csv,noheader)
if [[ "$kind" == scan ]]; then
  root="$work/native-scan-copy"
  [[ "$mode" != paced* ]] || root="$work/native-scan-copy-paced"
  [[ "$mode" != serial-* ]] || root="$work/native-scan-copy-serial"
  [[ "$mode" != batch-* ]] || root="$work/native-scan-copy-batch"
  agent="$root/scan-copy-probe.jar=$mode,$root/libscan_copy_probe.so,$root/gpu-pairs.properties,$root/leases"
elif [[ "$kind" == concat ]]; then
  root="$work/native-concat"
  agent="$root/concat-offset-probe.jar=$mode,$root/lib/libconcat_offsets_probe.so"
else
  exit 2
fi
"$work/jdk17/bin/java" -Dai.rapids.cudf.lib-native-dir="$work/native-concat/lib" \
  -javaagent:"$agent" \
  -cp "$output.classes:$engine:$work/spark-venv/lib/python3.10/site-packages/pyspark/jars/*" \
  com.nvidia.spark.rapids.experiments.ProbeAllocatorTest "$kind" 2>&1 | tee "$output.log"
