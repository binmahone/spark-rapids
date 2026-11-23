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

"""Shared reference-benchmark configuration and receipt handling."""
import gzip
import hashlib
import json
from pathlib import Path

SUITE = Path(__file__).resolve().parents[1]
TABLES = ('customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier')


def read(path):
    path = Path(path)
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt') as stream:
        return json.load(stream)


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.staging')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def load_config(platform, deployment=None):
    config = read(SUITE / 'configs' / (platform + '.json'))
    if deployment:
        supplied = read(deployment)
        allowed = {'spark_home', 'java_home', 'ucx_home', 'scratch_root', 'nodes',
                   'gpus_per_node', 'slurm_partition', 'container_image', 'node_list',
                   'cuda_home', 'cufile_config', 'container_mounts', 'container_setup'}
        if set(supplied) - allowed:
            raise ValueError('Deployment file contains unrecognized keys')
        config['deployment'].update(supplied)
    return config


def expand(config, queries):
    if len(set(queries)) != len(queries) or not set(queries) <= set(range(1, 23)) or not queries:
        raise ValueError('Queries must be unique integers between 1 and 22')
    expected = config['deployment']['nodes'] * config['deployment']['gpus_per_node']
    if expected != int(config['spark_conf']['spark.executor.instances']):
        raise ValueError('Executor count does not match the requested GPU inventory')
    profiles = {}
    for query in queries:
        profile = dict(config['spark_conf'])
        profile.update(config['query_overrides'].get(str(query), {}))
        profiles[str(query)] = profile
    if config['application_mode'] == 'suite':
        startup = ('spark.task.resource.gpu.amount', 'spark.executor.instances',
                   'spark.executor.cores', 'spark.executor.memory')
        for key in startup:
            if len({p.get(key) for p in profiles.values()}) != 1:
                raise ValueError('Suite mode cannot mutate startup-only resources: ' + key)
    return profiles
