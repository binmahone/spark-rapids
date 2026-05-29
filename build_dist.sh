#!/bin/bash
# Build spark-rapids dist jar for Spark 4.0 + Scala 2.13.
# Output: scala2.13/dist/target/rapids-4-spark_2.13-26.06.0-SNAPSHOT-cuda12.jar
# Use after editing sql-plugin / shuffle-plugin / RapidsConf / Plugin code.
set -euo pipefail
cd "$(dirname "$0")"
LOG=/tmp/spark_rapids_build_$(date +%y%m%d_%H%M%S).log
echo "Build log: $LOG"
mvn -B -Dbuildver=400 -DskipTests package -f scala2.13/ 2>&1 | tee "$LOG"
ls -lh scala2.13/dist/target/rapids-4-spark_2.13-26.06.0-SNAPSHOT-cuda12.jar
