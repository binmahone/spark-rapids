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

"""Validate dependencies in the allocated runtime, not against the Slurm controller filesystem."""
import shutil
from pathlib import Path

from common import TABLES


def validate_runtime(config):
    deployment = config['deployment']
    for key in ('spark_home', 'java_home', 'ucx_home'):
        if not deployment[key] or not Path(deployment[key]).is_dir():
            raise ValueError('Provide an existing deployment.' + key)
    for table in TABLES:
        if not (Path(config['dataset']) / table).is_dir():
            raise ValueError('Dataset table is unavailable: ' + table)
    for path in (Path(deployment['spark_home']) / 'bin/spark-submit',
                 Path(deployment['java_home']) / 'bin/java',
                 Path(deployment['ucx_home']) / 'lib/libucs.so'):
        if not path.exists():
            raise ValueError('Runtime dependency is unavailable: ' + str(path))
    for path in (Path(deployment['spark_home']) / 'jars').glob('*.jar'):
        if any(name in path.name.lower() for name in ('rapids', 'cudf', 'gluten', 'flux')):
            raise ValueError('Unexpected engine dependency in the Spark installation: ' + str(path))
    if config['platform'] == 'aws48x':
        from dataset import verify
        config['dataset_preflight'] = verify(config['dataset'], config['dataset_contract'])
    if shutil.which('numactl') is None:
        raise ValueError('numactl is required')
