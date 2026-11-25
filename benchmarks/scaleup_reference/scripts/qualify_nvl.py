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

"""Qualify strict native host/GDS reads on every allocated GPU before starting Spark."""
import os
from pathlib import Path
import subprocess

from common import read, write


def qualify(config, local):
    local = Path(local)
    deployment = config['deployment']
    devices = sorted(Path('/dev').glob('nvidia-fs*'))
    if len(devices) != 16:
        raise ValueError('Expected all 16 nvidia-fs devices in the NVL container')
    for path in devices:
        with path.open('r+b', buffering=0):
            pass
    base = '/usr/local/cuda-13.0'
    env = dict(os.environ, LD_LIBRARY_PATH=base + '/targets/sbsa-linux/lib:' + os.environ.get('LD_LIBRARY_PATH', ''))
    check = subprocess.check_output([base + '/gds/tools/gdscheck', '-p'], env=env,
                                     text=True, stderr=subprocess.STDOUT)
    (local / 'gdscheck.txt').write_text(check)
    if 'rdma library' not in check or 'Loaded' not in check or 'Up: 4 Down: 0' not in check:
        raise ValueError('Expected loaded cuFile RDMA support and four active ports')
    inventory = subprocess.check_output(['nvidia-smi', '--query-gpu=uuid', '--format=csv,noheader'],
                                        text=True).splitlines()
    if len(inventory) != deployment['gpus_per_node']:
        raise ValueError('GPU inventory differs before native-reader qualification')
    source = next(p for p in sorted((Path(config['dataset']) / 'lineitem').glob('*.parquet'))
                  if p.stat().st_size >= 16 * 1024 * 1024)
    spark_jars = str(Path(deployment['spark_home']) / 'jars/*')
    classpath = config['engine'] + ':' + config['tools'] + ':' + spark_jars
    records = []
    for index, uuid in enumerate(inventory):
        for mode in ('host', 'gds'):
            logs = local / f'qualification-{index}-{mode}'
            logs.mkdir()
            cufile = read(local / 'cufile.json')
            cufile['logging']['dir'] = str(logs)
            settings = local / f'qualification-{index}-{mode}.json'
            write(settings, cufile)
            current = dict(env, CUDA_VISIBLE_DEVICES=uuid, CUFILE_ENV_PATH_JSON=str(settings),
                            CUFILE_ALLOW_COMPAT_MODE='false', CUFILE_STATS_LEVEL='0',
                            CUFILE_LOGGING_LEVEL='INFO')
            command = [str(Path(deployment['java_home']) / 'bin/java'),
                       '-Djava.io.tmpdir=' + str(local / 'java-tmp'), '-cp', classpath,
                       'com.nvidia.spark.rapids.benchmark.NativeReaderQualification',
                       mode, str(source), str(logs / 'process-maps.txt')]
            with (logs / 'reader.log').open('w') as output:
                result = subprocess.run(command, env=current, stdout=output,
                                         stderr=subprocess.STDOUT, timeout=120)
            text = (logs / 'reader.log').read_text(errors='replace')
            if result.returncode or f'NATIVE_READER_BYTES_VALIDATED mode={mode}' not in text:
                raise ValueError('Native-reader qualification failed: ' + str(logs))
            if mode == 'gds':
                cufile_logs = '\n'.join(p.read_text(errors='replace') for p in logs.glob('*.log'))
                for marker in ('GPFS KERNEL RDMA SUPPORT',
                               'best rdma device selected using round-robin policy for gpu:',
                               'profile.cufile_stats : 0'):
                    if marker not in cufile_logs:
                        raise ValueError('Missing cuFile runtime evidence: ' + marker)
            records.append({'gpu_uuid': uuid, 'mode': mode, 'bytes_compared': 16 * 1024 * 1024,
                            'input': str(source), 'evidence': str(logs)})
    write(local / 'native-reader-qualification.json', records)
    return records
