#!/usr/bin/env bash
set -euo pipefail
readonly work=${1:?Absolute owned work directory is required}
readonly suite=$(cd "$(dirname "$0")/../.." && pwd)
mkdir -p "$work/evidence" "$work/artifacts/main"
exec > >(tee -a "$work/evidence/prepare.log") 2>&1
trap 'code=$?; printf "%s\n" "$code" > "$work/evidence/prepare.exit"' EXIT
test -e /var/lib/scaleup-reference-prepared
test "$(findmnt -n -T /opt/dlami/nvme -o TARGET)" = /opt/dlami/nvme
ulimit -n 65536
prepare_runtime() {
    bash "$suite/scripts/prepare_spark.sh" "$work/spark-venv"
    BUILD_JOBS=24 bash "$suite/scripts/build_ucx.sh" "$work/ucx" /usr/local/cuda-13.0
    while sudo fuser /var/lib/dpkg/lock >/dev/null 2>&1; do sleep 3; done
    cp -a --reflink=never /usr/lib/jvm/java-17-openjdk-amd64 "$work/jdk17"
    "$work/jdk17/bin/java" -version
    readonly prefix=s3://mahonem-wild-sf10k-261012-503960270539-use1/scaleup-reference/261125/01a1c86b-170e-7920-99bb-c1f74294bb9b/68d906668dca/amd64/c267dafd65ae
    for artifact in cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar benchmark-tools.jar manifest.json; do
        aws s3 cp "$prefix/$artifact" "$work/artifacts/main/$artifact" --region us-east-1 --only-show-errors
    done
    python3 - "$work" <<'PY'
from pathlib import Path
import hashlib, json, sys
w=Path(sys.argv[1]); a=w/'artifacts/main'
expected={'cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar':'c267dafd65ae85b558d8c8df03a94c0947cbe42b79a1e29ddeaa63bfcadb8cce',
          'benchmark-tools.jar':'4d28e4919d16b6a607092f91db1e271afe837100550251976c330ee25a1a2b90'}
for name,digest in expected.items():
    assert hashlib.sha256((a/name).read_bytes()).hexdigest()==digest,name
homes=list((w/'spark-venv/lib').glob('python*/site-packages/pyspark'));assert len(homes)==1
deployment=dict(spark_home=str(homes[0]),java_home=str(w/'jdk17'),ucx_home=str(w/'ucx'),
                cuda_home='/usr/local/cuda-13.1',scratch_root='/opt/dlami/nvme/aws-filter-probe-261125',
                nodes=1,gpus_per_node=8)
(w/'evidence/deployment.json').write_text(json.dumps(deployment,indent=2)+'\n')
PY
}
prepare_data() {
    readonly target=/opt/dlami/nvme/daily-trend-hardened/dataset
    readonly staging="${target}.filter-probe-download"
    mkdir -p "$(dirname "$target")"
    exec 9>"${staging}.lock"
    flock -n 9
    if test -e "$target"; then
        python3 "$suite/scripts/dataset.py" --dataset "$target"
        return
    fi
    export AWS_CONFIG_FILE="$work/evidence/download-aws-config"
    cat > "$AWS_CONFIG_FILE" <<'EOF'
[default]
region = us-west-2
s3 =
    preferred_transfer_client = crt
    target_bandwidth = 200Gb/s
EOF
    mkdir -p "$staging"
    aws s3 sync s3://mahonem-wild-sf10k-261012-503960270539-usw2/TPC-H-sf10k-decimal/ \
        "$staging/" --region us-west-2 --only-show-errors
    python3 "$suite/scripts/dataset.py" --dataset "$staging"
    mv -T "$staging" "$target"
}
prepare_runtime > "$work/evidence/prepare-runtime.log" 2>&1 &
runtime_pid=$!
prepare_data > "$work/evidence/prepare-data.log" 2>&1 &
data_pid=$!
status=0
wait "$runtime_pid" || status=$?
wait "$data_pid" || status=$?
exit "$status"
