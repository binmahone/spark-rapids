#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
readonly work=${1:?Prepared work directory}
readonly root="$work/native-concat"
[[ $(cat "$root/build.exit") == 0 ]]
[[ -z $(nvidia-smi --query-compute-apps=pid --format=csv,noheader) ]]
sha256sum --check "$root/agent.sha256"
readonly spark=$(python3 - "$work" <<'PY'
import json,pathlib,sys
print(json.loads((pathlib.Path(sys.argv[1])/'evidence/deployment.json').read_text())['spark_home'])
PY
)
export CUDA_VISIBLE_DEVICES=0
export LD_LIBRARY_PATH="$root/lib:$work/ucx/lib:/usr/local/cuda-13.1/lib64:/usr/local/cuda-13.1/targets/x86_64-linux/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
for mode in control candidate; do
  "$work/jdk17/bin/java" -ea -Xmx4g -Dai.rapids.cudf.lib-native-dir="$root/lib" \
    -javaagent:"$root/concat-offset-probe.jar=$mode,$root/lib/libconcat_offsets_probe.so" \
    -cp "$root/concat-offset-probe.jar:$work/artifacts/decimal-null-copy/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar:$spark/jars/*" \
    com.nvidia.spark.rapids.experiments.ConcatOffsetProbeTest \
    2>&1 | tee "$root/agent-test-$mode.log"
done
