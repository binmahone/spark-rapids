#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
ulimit -c 0
readonly work=${1:?Prepared work directory}
readonly root="$work/native-host-metadata"
[[ $(cat "$root/build.exit") == 0 ]]
[[ -z $(nvidia-smi --query-compute-apps=pid --format=csv,noheader) ]]
sha256sum --check "$root/artifacts.sha256"
export CUDA_VISIBLE_DEVICES
CUDA_VISIBLE_DEVICES=$(nvidia-smi --id=0 --query-gpu=uuid --format=csv,noheader)
for mode in safe unsafe; do
  [[ ! -e "$root/test-$mode.log" ]]
  "$work/jdk17/bin/java" -Dai.rapids.cudf.lib-native-dir="$work/native-concat/lib" \
    -cp "$root/classes:$work/artifacts/decimal-null-copy/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar:$work/spark-venv/lib/python3.10/site-packages/pyspark/jars/*" \
    com.nvidia.spark.rapids.experiments.HostMetadataAllocationProbe "$root/libhost_metadata_probe.so" "$mode" \
    2>&1 | tee "$root/test-$mode.log"
done
