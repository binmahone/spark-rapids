#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
ulimit -c 0
readonly work=${1:?Prepared work directory}
readonly family=${2:-admission}
[[ "$family" == admission || "$family" == pacing || "$family" == serial ]]
root="$work/native-scan-copy"
modes=(original sync pair)
if [[ "$family" == pacing || "$family" == serial ]]; then
  root="$work/native-scan-copy-paced"
  modes=(paced-control paced-queued paced)
fi
if [[ "$family" == serial ]]; then
  root="$work/native-scan-copy-serial"
  modes=(serial-original serial-unlocked serial-locked)
fi
[[ $(cat "$root/build.exit") == 0 ]]
[[ -z $(nvidia-smi --query-compute-apps=pid --format=csv,noheader) ]]
sha256sum --check "$root/artifacts.sha256"
export CUDA_VISIBLE_DEVICES
CUDA_VISIBLE_DEVICES=$(nvidia-smi --id=0 --query-gpu=uuid --format=csv,noheader)
for mode in "${modes[@]}"; do
  "$work/jdk17/bin/java" -Dai.rapids.cudf.lib-native-dir="$work/native-concat/lib" \
    -javaagent:"$root/scan-copy-probe.jar=$mode,$root/libscan_copy_probe.so,$root/gpu-pairs.properties,$root/leases" \
    -cp "$work/spark-venv/lib/python3.10/site-packages/pyspark/jars/*:$root/scan-copy-probe.jar:$work/artifacts/decimal-null-copy/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar" \
    com.nvidia.spark.rapids.experiments.ScanCopyProbeTest 2>&1 | tee "$root/test-$mode.log"
done
