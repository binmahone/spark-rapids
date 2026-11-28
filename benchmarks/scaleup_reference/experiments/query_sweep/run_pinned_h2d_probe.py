#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Measure pinned H2D throughput with the benchmark's GPU-local NUMA preference."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, sha, write
import services

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', type=Path, required=True)
p.add_argument('--thread-id', required=True)
a = p.parse_args()
assert read(a.work / 'evidence/owner.json')['thread_id'] == a.thread_id
assert not subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid',
                                    '--format=csv,noheader'], text=True).strip()
output = a.work / 'pinned-h2d-probe'
output.mkdir(exist_ok=False)
binary = output / 'pinned-h2d-probe'
cuda = Path('/usr/local/cuda-13.1')
command = ['g++-12', '-O3', '-std=c++17', '-pthread', '-I' + str(cuda / 'include'),
           str(HERE / 'pinned_h2d_probe.cpp'), '-L' + str(cuda / 'lib64'),
           '-Wl,-rpath,' + str(cuda / 'lib64'), '-lcudart', '-o', str(binary)]
with (output / 'build.log').open('x') as log:
    subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
inventory = subprocess.check_output(['nvidia-smi', '--query-gpu=index,pci.bus_id',
                                     '--format=csv,noheader'], text=True)
nodes = {}
for line in inventory.splitlines():
    index, pci = [s.strip() for s in line.split(',')]
    domain, bus, device = pci.lower().split(':')
    node = int((Path('/sys/bus/pci/devices') / f'{domain[-4:]}:{bus}:{device}' / 'numa_node').read_text())
    assert node >= 0
    nodes[int(index)] = node
assert set(nodes) == set(range(8))
manifest = {'owner_thread': a.thread_id, 'build_command': command,
            'source_sha256': sha(HERE / 'pinned_h2d_probe.cpp'), 'binary_sha256': sha(binary),
            'gpu_numa_nodes': nodes, 'limits': ['Diagnostic copy ceiling, not a query speedup.',
            'One stream per GPU; repeated copies of a GPU-local pinned allocation.']}
write(output / 'manifest.json', manifest)
for name, gpus in [('one-gpu', [0]), ('eight-gpu', list(range(8)))]:
    directory = output / name
    directory.mkdir()
    children, records = [], []
    try:
        for gpu in gpus:
            command = ['numactl', '--cpunodebind=' + str(nodes[gpu]),
                       '--preferred=' + str(nodes[gpu]), str(binary), str(gpu), str(directory)]
            child, record = services.launch(command, directory / f'gpu-{gpu}.jsonl', os.environ)
            children.append(child)
            records.append(record)
            write(directory / 'services.json', records)
        for size in (2 << 20, 64 << 20, 1 << 30):
            for repetition in range(3):
                token = f'{size}-{repetition}'
                deadline = time.monotonic() + 30
                while not all((directory / f'{token}-ready-{gpu}').exists() for gpu in gpus):
                    assert all(c.poll() is None for c in children)
                    assert time.monotonic() < deadline
                    time.sleep(.02)
                (directory / (token + '-go')).touch()
        assert all(c.wait(timeout=30) == 0 for c in children)
    finally:
        services.stop(directory / 'services.json')
    rows = [json.loads(line) for gpu in gpus
            for line in (directory / f'gpu-{gpu}.jsonl').read_text().splitlines()]
    assert len(rows) == len(gpus) * 9 and all(r['validation'] == 'passed' for r in rows)
    write(directory / 'results.json', rows)
    for size in (2 << 20, 64 << 20, 1 << 30):
        measured = [r for r in rows if r['bytes_per_copy'] == size]
        print(name, size, 'per-GPU GB/s', [round(r['bytes_per_copy'] * r['copies'] /
              r['wall_seconds'] / 1e9, 2) for r in measured], flush=True)
