# Copyright (c) 2026, NVIDIA CORPORATION.
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

"""Build the optional result validator and required catalog-statistics bridge."""
import argparse
from pathlib import Path
import subprocess

suite = Path(__file__).resolve().parents[1]
root = suite.parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--arch', choices=('arm64', 'amd64'), required=True)
args = parser.parse_args()
classifier = 'cuda13-arm64' if args.arch == 'arm64' else 'cuda13'
engine = root / '.build/artifacts' / args.arch / (
    f'cudf-spark_2.12-26.12.0-SNAPSHOT-{classifier}.jar')
repository = Path(__import__('os').environ.get('MAVEN_REPOSITORY', root / '.build/maven-repository'))
spark = repository / 'org/apache/spark'
jars = [engine]
for name in ('spark-core_2.12', 'spark-sql_2.12', 'spark-catalyst_2.12',
             'spark-sql-api_2.12', 'spark-common-utils_2.12', 'spark-unsafe_2.12'):
    jars.append(spark / name / '3.5.3' / f'{name}-3.5.3.jar')
jars += list((repository / 'org/scala-lang/scala-library').glob('2.12.21/*.jar'))
classes = root / '.build/tools-classes'
classes.mkdir(exist_ok=True)
sources = sorted((suite / 'tools/src/main/java').rglob('*.java'))
subprocess.run(['javac', '--release', '8', '-cp', ':'.join(map(str, jars)),
                '-d', str(classes), *map(str, sources)], check=True)
subprocess.run(['jar', 'cf', str(root / '.build/artifacts' / args.arch / 'benchmark-tools.jar'),
                '-C', str(classes), '.'], check=True)
