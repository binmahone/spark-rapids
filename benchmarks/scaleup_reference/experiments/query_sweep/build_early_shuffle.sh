#!/usr/bin/env bash
set -euo pipefail
readonly work=${1:?Prepared work directory}
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly out="$work/artifacts/early-shuffle"
readonly jars="$work/spark-venv/lib/python3.10/site-packages/pyspark/jars"
mkdir -p "$out/classes"
if [[ -e "$out/early-shuffle.jar" ]]; then
  previous=$(sha256sum "$out/early-shuffle.jar" | cut -d ' ' -f 1)
  mkdir -p "$out/versions/$previous"
  cp "$out/early-shuffle.jar" "$out/build.log" "$out/source-revision.txt" "$out/sha256.txt" \
    "$out/versions/$previous/"
fi
exec > >(tee "$out/build.log") 2>&1
"$work/jdk17/bin/javac" --release 8 -Xlint:all -cp "$jars/*" \
  -d "$out/classes" "$here/EarlyShuffleCollector.java"
"$work/jdk17/bin/jar" cf "$out/early-shuffle.jar" -C "$out/classes" .
sha256sum "$out/early-shuffle.jar" "$here/EarlyShuffleCollector.java" > "$out/sha256.txt"
git -C "$here" rev-parse HEAD > "$out/source-revision.txt"
