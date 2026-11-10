#!/usr/bin/env bash
set -euo pipefail
readonly python=${1:?Specify the isolated Python executable}
readonly manifest=${2:?Specify the sixteen-file range manifest}
readonly output=${3:?Specify a new evidence directory}
readonly here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$(dirname "$output")"
mkdir "$output"
printf '%q ' "$0" "$@" > "$output/command.txt"
printf '\n' >> "$output/command.txt"
cp "$manifest" "$output/manifest.json"
export CUDA_VISIBLE_DEVICES=''
export KVIKIO_COMPAT_MODE=ON
export KVIKIO_AUTO_DIRECT_IO_READ=OFF
common=(--manifest "$manifest" --file-workers 16 --repeats 3 --max-buffer-gib 32
  --cache-policy warm)
for spec in posix:32:1:0 posix-parallel:32:4:0 kvikio:32:1:0 kvikio:32:4:0 \
  kvikio:32:1:4194304 kvikio:32:1:1048576 kvikio:4:1:4194304 \
  kvikio:32:4:0 posix-parallel:32:4:0; do
  IFS=: read -r backend threads window task_bytes <<< "$spec"
  ordinal=$(find "$output" -name 'case-*.jsonl' | wc -l)
  "$python" "$here/range_benchmark.py" run "${common[@]}" --backend "$backend" \
    --threads "$threads" --window "$window" --task-bytes "$task_bytes" \
    | tee "$output/case-$ordinal-$backend-$threads-$window-$task_bytes.jsonl"
done
g++ -std=c++17 -O2 -shared -fPIC -pthread "$here/trace_pread.cpp" -ldl \
  -o "$output/trace_pread.so"
prefix=$("$python" -c 'import json,os,sys; print(os.path.commonpath([f["path"] for f in json.load(open(sys.argv[1]))["files"]]))' "$manifest")
KVIKIO_TRACE_PREFIX="$prefix" KVIKIO_TRACE_OUTPUT="$output/native-internal-split-trace.jsonl" \
  LD_PRELOAD="$output/trace_pread.so" \
  "$python" "$here/range_benchmark.py" run --manifest "$manifest" --backend kvikio \
    --file-workers 16 --threads 32 --window 1 --task-bytes 4194304 --repeats 1 \
    --max-buffer-gib 32 | tee "$output/traced-internal-split-not-performance.jsonl"
"$python" "$here/analyze_trace.py" "$output/native-internal-split-trace.jsonl" \
  --require-same-fd-overlap | tee "$output/native-internal-split-summary.json"
