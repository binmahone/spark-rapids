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

"""Run one allocated node; all long-lived Java services have saved ownership receipts."""
import argparse
import os
from pathlib import Path
import re
import socket
import signal
import shutil
import subprocess
import time

from common import read, sha, write
from cohort import run_cohort
import services
from preflight import validate_runtime


def main():
    def terminate(signum, frame):
        raise SystemExit('Run interrupted by signal ' + str(signum))
    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', required=True)
    args = parser.parse_args()
    run = Path(args.run)
    config = read(run / 'config.json')
    deployment = config['deployment']
    local = Path(deployment['scratch_root']) / run.name
    local.mkdir(parents=True, exist_ok=True)
    (local / 'java-tmp').mkdir(exist_ok=True)
    validate_runtime(config)
    head = os.environ['SCALEUP_HEAD']
    node = socket.gethostname()
    write(run / 'nodes' / (node + '.json'), {'hostname': node, 'uid': os.getuid(),
        'uname': list(os.uname()), 'gpu': subprocess.check_output(['nvidia-smi',
        '--query-gpu=name,uuid,driver_version,memory.total', '--format=csv,noheader'], text=True),
        'mount': subprocess.check_output(['findmnt', '-T', config['dataset']], text=True)})
    state = None
    try:
        if config['platform'] == 'nvl72':
            source = Path(deployment.get('cufile_config', '/etc/cufile.json'))
            text = re.sub(r'//[^\n]*', '', source.read_text())
            cufile = __import__('json').loads(text)
            cufile['properties']['allow_compat_mode'] = False
            cufile['profile']['cufile_stats'] = 0
            cufile['logging']['level'] = 'INFO'
            logs = local / 'cufile-logs'
            logs.mkdir(exist_ok=True)
            cufile['logging']['dir'] = str(logs)
            write(local / 'cufile.json', cufile)
            source_lib = Path('/usr/local/cuda-13.0/targets/sbsa-linux/lib')
            source_hashes = {path.name: sha(path) for pattern in ('libcufile.so*', 'libcufile_rdma.so*')
                             for path in sorted(source_lib.glob(pattern))}
            write(run / 'nodes' / (node + '.cufile-source.json'), source_hashes)
            if config.get('deployment_experiment', {}).get('bundle_cufile', False):
                destination = local / 'cufile-runtime'
                destination.mkdir()
                records = {}
                for pattern in ('libcufile.so*', 'libcufile_rdma.so*'):
                    paths = sorted(source_lib.glob(pattern))
                    if not paths:
                        raise RuntimeError('Missing cuFile runtime dependency: ' + pattern)
                    for path in paths:
                        target = destination / path.name
                        shutil.copyfile(path, target)
                        original_hash, copied_hash = sha(path), sha(target)
                        if original_hash != copied_hash:
                            raise AssertionError('Copied cuFile library differs')
                        records[path.name] = {'source': str(path), 'sha256': copied_hash}
                write(run / 'nodes' / (node + '.cufile-runtime.json'), records)
            from qualify_nvl import qualify
            qualification = qualify(config, local)
            write(run / 'nodes' / (node + '.native-reader.json'), qualification)
            state = services.start(run, config, head, int(os.environ['SCALEUP_MASTER_PORT']),
                                   int(os.environ['SCALEUP_MASTER_UI_PORT']), 'suite')
            (run / 'nodes' / (node + '.ready')).touch()
        if node == head:
            deadline = time.monotonic() + 300
            while config['platform'] == 'nvl72' and len(list((run / 'nodes').glob('*.ready'))) != deployment['nodes']:
                if time.monotonic() >= deadline:
                    raise RuntimeError('Allocated nodes did not all become ready')
                time.sleep(1)
            run_cohort(run)
            (run / 'completed').touch()
        else:
            deadline = time.monotonic() + 2700
            while not (run / 'completed').exists():
                if time.monotonic() >= deadline:
                    raise RuntimeError('Head-node completion deadline expired')
                time.sleep(1)
    finally:
        if state:
            services.stop(state)


if __name__ == '__main__':
    main()
