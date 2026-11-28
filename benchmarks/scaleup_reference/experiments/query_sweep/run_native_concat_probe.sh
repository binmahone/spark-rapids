#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
readonly work=${1:?Prepared work directory}
readonly thread=${2:?Owner thread}
readonly label=${3:?Unique test label}
readonly root="$work/native-concat"
python3 - "$work" "$thread" "$label" <<'PY'
import json,pathlib,subprocess,sys
w=pathlib.Path(sys.argv[1]);label=sys.argv[3]
assert json.loads((w/'evidence/owner.json').read_text())['thread_id']==sys.argv[2]
assert label.replace('-', '').isalnum()
assert (w/'native-concat/build.exit').read_text().strip()=='0'
assert not subprocess.check_output(['nvidia-smi','--query-compute-apps=pid','--format=csv,noheader'],text=True).strip()
assert not (w/'native-concat'/('probe-'+label+'.jsonl')).exists()
PY
exec > >(tee "$root/probe-$label.jsonl") 2> >(tee "$root/probe-$label.stderr" >&2)
trap 'code=$?; printf "%s\n" "$code" > "$root/probe-$label.exit"' EXIT
sha256sum --check "$root/probe.sha256" >&2
export CUDA_VISIBLE_DEVICES=0
export LD_LIBRARY_PATH="$root/lib:$work/ucx/lib:/usr/local/cuda-13.1/lib64:/usr/local/cuda-13.1/targets/x86_64-linux/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
"$root/native-concat-probe"
