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

"""Start and stop only the Java services recorded for a reference-benchmark run."""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.request
import uuid

from common import read, write


def start_time(pid):
    return Path(f'/proc/{pid}/stat').read_text().split(') ', 1)[1].split()[19]


def launch(command, log, env):
    token = uuid.uuid4().hex
    env = dict(env, SCALEUP_SERVICE_TOKEN=token)
    with Path(log).open('w') as output:
        process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT,
                                   env=env, start_new_session=True)
    return process, {'pid': process.pid, 'start_time': start_time(process.pid), 'command': command,
                     'token': token, 'owner_uid': os.getuid()}


def spawn(command, log, env):
    return launch(command, log, env)[1]


def group_members(record):
    """Find this service's process group, including children whose leader has exited."""
    members = []
    try:
        leader_matches = start_time(record['pid']) == record['start_time']
    except (FileNotFoundError, ProcessLookupError):
        leader_matches = False
    for entry in Path('/proc').iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry / 'stat').read_text().split(') ', 1)[1].split()
            if int(fields[2]) != record['pid'] or fields[0] == 'Z':
                continue
            if entry.stat().st_uid != record['owner_uid']:
                raise RuntimeError('Service process group has another owner')
            if not leader_matches:
                marker = ('SCALEUP_SERVICE_TOKEN=' + record['token']).encode()
                try:
                    environment = (entry / 'environ').read_bytes().split(b'\0')
                except PermissionError:
                    current = (entry / 'stat').read_text().split(') ', 1)[1].split()
                    if current[0] in ('Z', 'X'):
                        continue
                    raise
                if marker not in environment:
                    raise RuntimeError('Service process group ownership marker differs')
            members.append(int(entry.name))
        except (FileNotFoundError, ProcessLookupError):
            pass
    return members


def stop(state):
    if not Path(state).exists():
        return
    records = read(state)
    for sig in (signal.SIGTERM, signal.SIGKILL):
        for record in reversed(records):
            if group_members(record):
                try:
                    os.killpg(record['pid'], sig)
                except ProcessLookupError:
                    pass
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            live = [pid for record in records for pid in group_members(record)]
            if not live:
                write(str(state) + '.stopped.json', {'remaining_recorded_processes': [],
                                                    'owner_uid': os.getuid()})
                return
            time.sleep(.5)
    raise RuntimeError('Recorded Java services did not stop')


def start(run, config, master_host, master_port, master_ui_port, tag):
    state = Path(run) / 'services' / tag / (socket.gethostname() + '.json')
    try:
        return _start(run, config, master_host, master_port, master_ui_port, tag)
    except BaseException:
        stop(state)
        raise


def _start(run, config, master_host, master_port, master_ui_port, tag):
    run = Path(run)
    deployment = config['deployment']
    node = socket.gethostname()
    local = Path(deployment['scratch_root']) / run.name / tag
    local.mkdir(parents=True, exist_ok=True)
    state = run / 'services' / tag / (node + '.json')
    state.parent.mkdir(parents=True, exist_ok=True)
    if state.exists():
        raise RuntimeError('Service ownership record already exists: ' + str(state))
    records = []
    env = dict(os.environ, JAVA_HOME=deployment['java_home'], SPARK_HOME=deployment['spark_home'])
    env['PATH'] = deployment['java_home'] + '/bin:' + env.get('PATH', '')
    spark_class = str(Path(deployment['spark_home']) / 'bin/spark-class')
    head = os.environ.get('SCALEUP_HEAD', socket.gethostname())
    if node == head:
        records.append(spawn([spark_class, 'org.apache.spark.deploy.master.Master',
                              '--host', master_host, '--port', str(master_port),
                              '--webui-port', str(master_ui_port)], local / 'master.log', env))
        write(state, records)
    deadline = time.monotonic() + 120
    while True:
        try:
            with urllib.request.urlopen(f'http://{master_host}:{master_ui_port}/json/', timeout=3):
                break
        except Exception:
            if time.monotonic() >= deadline:
                stop(state)
                raise RuntimeError('Spark master did not start')
            time.sleep(1)
    inventory = subprocess.check_output(['nvidia-smi', '--query-gpu=index,uuid,pci.bus_id',
                                          '--format=csv,noheader'], text=True).splitlines()
    if len(inventory) != deployment['gpus_per_node']:
        raise AssertionError('GPU count differs from platform configuration')
    if config['platform'] == 'aws48x':
        params = Path('/proc/driver/nvidia/params').read_text()
        if ('ForceP2P=17;RMForceP2PType=1;RMPcieP2PType=1' not in params or
                'DmaRemapPeerMmio: 0' not in params):
            raise RuntimeError('AWS P2P driver preparation and reboot are required')
        for mode in ('r', 'w'):
            matrix = subprocess.check_output(['nvidia-smi', 'topo', '-p2p', mode], text=True)
            if sum(line.split().count('OK') for line in matrix.splitlines()
                   if line.lstrip().startswith('GPU')) != 56:
                raise RuntimeError('AWS does not expose all 56 directed P2P pairs')
            (local / f'p2p-{mode}.txt').write_text(matrix)
    write(local / 'gpu-inventory.json', inventory)
    for line in inventory:
        index, uuid, pci = [part.strip() for part in line.split(',')]
        index = int(index)
        domain, bus, device = pci.lower().split(':')
        numa_path = Path('/sys/bus/pci/devices') / f'{domain[-4:]}:{bus}:{device}' / 'numa_node'
        numa = int(numa_path.read_text())
        if numa < 0:
            raise RuntimeError('GPU NUMA placement is unavailable')
        worker = local / f'worker-{index}'
        worker.mkdir()
        discover = worker / 'discover-gpu.sh'
        discover.write_text('#!/bin/sh\nprintf \'%s\\n\' \'{"name":"gpu","addresses":["0"]}\'\n')
        discover.chmod(0o755)
        worker_env = dict(env, CUDA_VISIBLE_DEVICES=uuid, SPARK_WORKER_DIR=str(worker / 'work'),
                          SPARK_LOCAL_DIRS=str(worker / 'local'),
                          SPARK_WORKER_OPTS=('-Dspark.worker.resource.gpu.amount=1 '
                              '-Dspark.worker.resource.gpu.discoveryScript=' + str(discover)))
        binding = '--membind=' if config['platform'] == 'nvl72' else '--preferred='
        command = ['numactl', '--cpunodebind=' + str(numa), binding + str(numa), spark_class,
                   'org.apache.spark.deploy.worker.Worker', '--host', master_host if
                   config['platform'] == 'aws48x' else node, '--port', '0', '--webui-port', '0',
                   '--cores', '32' if config['platform'] == 'nvl72' else '24',
                   '--memory', '350g' if config['platform'] == 'nvl72' else '200g',
                   'spark://' + master_host + ':' + str(master_port)]
        records.append(spawn(command, worker / 'worker.log', worker_env))
        write(state, records)
    return state
