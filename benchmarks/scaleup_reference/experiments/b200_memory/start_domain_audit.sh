#!/usr/bin/env bash
set -euo pipefail
readonly work=${1:?Owned work directory required}
readonly here=$(cd "$(dirname "$0")" && pwd)
exec > >(tee "$work/evidence/integer-domain-audit.log") 2>&1
ulimit -c 0
export JAVA_HOME="$work/jdk17"
export CUDA_VISIBLE_DEVICES=''
export PYSPARK_PYTHON=/usr/bin/python3
export SPARK_LOCAL_IP=10.34.1.4
readonly spark_home=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["spark_home"])' "$work/evidence/deployment.json")
"$spark_home/bin/spark-submit" --master 'local[1]' --driver-memory 2g \
    --conf spark.plugins= --conf spark.ui.enabled=false \
    --conf spark.driver.host=10.34.1.4 --conf spark.driver.bindAddress=10.34.1.4 \
    --conf "spark.local.dir=$work/scratch/domain-audit" \
    "$here/integer_domains.py" --work "$work"
