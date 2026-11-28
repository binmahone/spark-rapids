#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
ulimit -c 0
readonly work=${1:?Prepared work directory}
readonly label=${2:?Unique experiment label}
readonly kind=${3:-timing}
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly output="$work/bounded-scan/$label"
readonly engine="$work/artifacts/decimal-null-copy/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar"
[[ "$label" =~ ^[a-zA-Z0-9_-]+$ ]]
[[ "$kind" == timing || "$kind" == ncu || "$kind" == timeline || "$kind" == stacks || "$kind" == windowed || "$kind" == windowtrace ]]
[[ -z $(nvidia-smi --query-compute-apps=pid --format=csv,noheader) ]]
[[ $(sha256sum "$engine" | cut -d ' ' -f 1) == ec38a58426e662ce99a433e806a66fa927bb011c014465a5df0b0c31ded4d1cc ]]
mkdir -p "$work/bounded-scan"
mkdir "$output"
exec > >(tee "$output/run.log") 2>&1
trap 'code=$?; printf "%s\n" "$code" > "$output/exit"' EXIT
exec 8>"$work/bounded-scan/run.lock"
flock -n 8
git -C "$here" rev-parse HEAD > "$output/source-revision.txt"
mkdir "$output/classes"
readonly cp="$engine:$work/spark-venv/lib/python3.10/site-packages/pyspark/jars/*"
"$work/jdk17/bin/javac" --release 11 -cp "$cp" -d "$output/classes" \
  "$here/BoundedScanPipelineProbe.java"
sha256sum "$output/classes/com/nvidia/spark/rapids/experiments/BoundedScanPipelineProbe"*.class \
  > "$output/classes.sha256"
export CUDA_VISIBLE_DEVICES
CUDA_VISIBLE_DEVICES=$(nvidia-smi --id=0 --query-gpu=uuid --format=csv,noheader)
export LIBCUDF_NVCOMP_POLICY=STABLE
readonly dataset=/opt/dlami/nvme/daily-trend-hardened/dataset/lineitem/lineitem.1.parquet
profile_options=()
if [[ "$kind" == stacks ]]; then
  profiler=$(python3 - "$work/profilers/manifest.json" <<'PY'
import hashlib, json, pathlib, sys
p = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert hashlib.sha256(pathlib.Path(p['library']).read_bytes()).hexdigest() == p['library_sha256']
print(p['library'])
PY
)
  profile_options=("-agentpath:$profiler=start,event=cpu,interval=1ms,wall=2ms,cstack=dwarf,jstackdepth=256,file=$output/profile.jfr")
fi
command=(numactl --cpunodebind=0 --membind=0 "$work/jdk17/bin/java" -Xmx4g \
  -Dai.rapids.cudf.nvtx.enabled=true \
  "${profile_options[@]}" \
  -Dai.rapids.cudf.lib-native-dir="$work/native-concat/lib" \
  -cp "$output/classes:$cp" com.nvidia.spark.rapids.experiments.BoundedScanPipelineProbe \
  "$dataset")
python3 - "$output" "$dataset" "$engine" "${command[@]}" <<'PY'
import hashlib, json, os, pathlib, sys
out, data, engine = map(pathlib.Path, sys.argv[1:4])
s = data.stat()
(out / 'manifest.json').write_text(json.dumps({
    'classification': 'Isolated bounded lookahead experiment, not Spark query performance',
    'command_prefix': sys.argv[4:], 'dataset': str(data), 'dataset_bytes': s.st_size,
    'dataset_mtime_ns': s.st_mtime_ns, 'engine': str(engine),
    'gpu_uuid': os.environ['CUDA_VISIBLE_DEVICES'], 'nvcomp_policy': 'STABLE',
    'max_staged_inputs': 2, 'device_input_limit_bytes': 4 * 1024**3,
    'limitations': ['No concurrent Spark tasks or GPU admission semaphore.',
                   'Host reads happen before timing; no claim about storage throughput.',
                   'Repeated alternating splits reuse host input but rebuild the native reader.']
}, indent=2) + '\n')
PY
if [[ "$kind" == timing ]]; then
  "${command[@]}" 16 all
elif [[ "$kind" == windowed ]]; then
  "${command[@]}" 32 windowed
elif [[ "$kind" == stacks ]]; then
  "${command[@]}" 128 pipeline
elif [[ "$kind" == ncu ]]; then
  sudo env CUDA_VISIBLE_DEVICES="$CUDA_VISIBLE_DEVICES" LIBCUDF_NVCOMP_POLICY=STABLE \
    /usr/local/cuda-13.2/bin/ncu --target-processes all --kernel-name 'regex:.*unsnap_kernel.*' \
    --launch-skip 0 --launch-count 1 --set full --export "$output/snappy" \
    "${command[@]}" 4 original
else
  mode=all
  [[ "$kind" != windowtrace ]] || mode=windowed
  nsys profile --trace=cuda,nvtx --sample=none --cpuctxsw=none \
    --output="$output/timeline" "${command[@]}" 8 "$mode"
fi
