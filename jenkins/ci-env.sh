#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

SPARK_RAPIDS_POM=${SPARK_RAPIDS_POM:-'pom.xml'}

JNI_VERSION=$(mvn help:evaluate -q -f $SPARK_RAPIDS_POM -N -Dexpression=spark-rapids-jni.version -DforceStdout)
SPARK_RAPIDS_JNI_URL_SUFFIX="com/nvidia/spark-rapids-jni/$JNI_VERSION/spark-rapids-jni-$JNI_VERSION-cuda11.jar"

export SPARK_RAPIDS_JNI_URL=${SPARK_RAPIDS_JNI_URL:-"$ART_URL/$SPARK_RAPIDS_JNI_URL_SUFFIX"}
export SPARK_RAPIDS_BUILD_IMAGE=${SPARK_RAPIDS_BUILD_IMAGE:-"$ARTIFACTORY_NAME/sw-spark-docker/bd-rapids:20250418"}
export SPARK_RAPIDS_RUNTIME_IMAGE=${SPARK_RAPIDS_RUNTIME_IMAGE:-"$ARTIFACTORY_NAME/sw-spark-docker/bd-spark-rapids-runtime:20250418"}

echo "SPARK_RAPIDS_JNI_URL=$SPARK_RAPIDS_JNI_URL"
echo "SPARK_RAPIDS_BUILD_IMAGE=$SPARK_RAPIDS_BUILD_IMAGE"
echo "SPARK_RAPIDS_RUNTIME_IMAGE=$SPARK_RAPIDS_RUNTIME_IMAGE"
