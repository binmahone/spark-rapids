#!/bin/bash
# After B200 spark-rapids-jni build completes, sync the new jar to devcontainer
# maven local repo, then build spark-rapids dist jar, then ship it back to B200.
#
# Prereq: B200 build at /raid/mahonem/spark-rapids-jni/runs/build-*/ completed.
# Output: scala2.13/dist/target/rapids-4-spark_2.13-26.06.0-SNAPSHOT-cuda12.jar
#         deployed to B200 at /raid/mahonem/spark-rapids/jars/
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
B200=mahonem@10.34.1.4
B200_M2=/home/nfs/mahonem/.m2/repository
DEV_M2="$HOME/.m2/repository"
TS=$(date +%y%m%d_%H%M%S)

cd "$SCRIPT_DIR"

echo "=== [1/4] fetch new spark-rapids-jni jar from B200 target/ and install ==="
# B200 build produced jar under target/ but did not run `install`, so the local
# maven repo on B200 is empty. Fetch the jar directly and `install:install-file`
# into the dev maven repo so spark-rapids can resolve the dependency.
JNI_VER=26.06.0-SNAPSHOT
JNI_JAR_LOCAL=/tmp/spark-rapids-jni-${JNI_VER}-cuda12.jar
JNI_JAR_REMOTE=/raid/mahonem/spark-rapids-jni/target/spark-rapids-jni-${JNI_VER}-cuda12.jar
JNI_POM_REMOTE=/raid/mahonem/spark-rapids-jni/pom.xml

scp "$B200:$JNI_JAR_REMOTE" "$JNI_JAR_LOCAL"
JNI_POM_LOCAL=/tmp/spark-rapids-jni-${JNI_VER}.pom
scp "$B200:$JNI_POM_REMOTE" "$JNI_POM_LOCAL"

mvn -B install:install-file \
    -Dfile="$JNI_JAR_LOCAL" \
    -DpomFile="$JNI_POM_LOCAL" \
    -Dclassifier=cuda12 \
    -Dpackaging=jar

echo
echo "=== [2/4] verify installed ==="
ls -la "$DEV_M2/com/nvidia/spark-rapids-jni/$JNI_VER/" | head -10

echo
echo "=== [3/4] build spark-rapids dist jar ==="
LOG=/tmp/spark_rapids_build_${TS}.log
echo "build log: $LOG"
mvn -B -Dbuildver=400 -DskipTests package -f scala2.13/ 2>&1 | tee "$LOG"

DIST_JAR="scala2.13/dist/target/rapids-4-spark_2.13-26.06.0-SNAPSHOT-cuda12.jar"
ls -lh "$DIST_JAR"

echo
echo "=== [4/4] ship dist jar to B200 ==="
# Run scripts (run_q5_cgt5.sh etc) hardcode RAPIDS_JAR to /raid/mahonem/spark/...
# Back up old jar then drop the new one in place + also copy to /raid/mahonem/spark-rapids/jars/
DEPLOY_PATH=/raid/mahonem/spark/rapids-4-spark_2.13-26.06.0-SNAPSHOT-cuda12.jar
ssh "$B200" "mkdir -p /raid/mahonem/spark-rapids/jars && \
    if [ -f $DEPLOY_PATH ]; then cp $DEPLOY_PATH ${DEPLOY_PATH}.bak-${TS}; fi"
scp "$DIST_JAR" "$B200:$DEPLOY_PATH"
scp "$DIST_JAR" "$B200:/raid/mahonem/spark-rapids/jars/"
ssh "$B200" "ls -la $DEPLOY_PATH /raid/mahonem/spark-rapids/jars/$(basename $DIST_JAR)"
echo
echo "=== done ${TS} ==="
echo "Next: ssh $B200 'bash /raid/mahonem/spark-rapids/scripts/run_q5_cgt5.sh'"
