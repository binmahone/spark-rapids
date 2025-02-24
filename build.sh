#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e
set -x

rm -rf output
mkdir output

export MAVEN_OPTS="-Xmx6000m"
mvn clean install -Drat.skip=true -Dmaven.javadoc.skip=true -Dskip -Dmaven.scalastyle.skip=true -DskipTests -Dbuildver=321 ${MVN_OPT}
mv dist/target/rapids-4-spark_2.12-*.jar ./output/rapids-4-spark_2.12.jar
