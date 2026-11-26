#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
"""Collect interval GPM metrics and restore each GPU's original streaming state."""
import argparse
import json
import math
import signal
import time
from pathlib import Path
import pynvml as n

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--directory', required=True)
a = p.parse_args()
directory = Path(a.directory)
running = True

def stop(*unused):
    global running
    running = False

signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
metrics = {2: 'sm_active_percent', 3: 'sm_occupancy_percent',
           10: 'dram_bandwidth_percent', 20: 'pcie_tx_mib_per_second',
           21: 'pcie_rx_mib_per_second'}
n.nvmlInit()
records, buffers = [], []
receipt = {'metrics': metrics, 'devices': records, 'restoration': []}
try:
    for i in range(n.nvmlDeviceGetCount()):
        h = n.nvmlDeviceGetHandleByIndex(i)
        supported = n.nvmlGpmQueryDeviceSupport(h).isSupportedDevice
        original = n.nvmlGpmQueryIfStreamingEnabled(h)
        record = {'index': i, 'uuid': n.nvmlDeviceGetUUID(h), 'supported': supported,
                  'original_streaming': original}
        records.append(record)
        assert supported, record
        if not original:
            n.nvmlGpmSetStreamingEnabled(h, 1)
        pair = [n.nvmlGpmSampleAlloc(), n.nvmlGpmSampleAlloc()]
        buffers.append((h, pair))
        n.nvmlGpmSampleGet(h, pair[0])
        record['pcie_max_generation'] = n.nvmlDeviceGetMaxPcieLinkGeneration(h)
        record['pcie_max_width'] = n.nvmlDeviceGetMaxPcieLinkWidth(h)
    previous = time.time_ns()
    first = True
    with (directory / 'gpm.jsonl').open('x', buffering=1) as stream:
        while running:
            time.sleep(0.5)
            stamp = time.time_ns()
            data = []
            for index, (h, pair) in enumerate(buffers):
                n.nvmlGpmSampleGet(h, pair[1])
                request = n.c_nvmlGpmMetricsGet_t()
                request.version = n.NVML_GPM_METRICS_GET_VERSION
                request.numMetrics = len(metrics)
                request.sample1, request.sample2 = pair
                for j, metric in enumerate(metrics):
                    request.metrics[j].metricId = metric
                n.nvmlGpmMetricsGet(request)
                values = {'index': index}
                for j, name in enumerate(metrics.values()):
                    result = request.metrics[j]
                    values[name] = result.value if result.nvmlReturn == 0 and math.isfinite(result.value) else None
                    if result.nvmlReturn:
                        values[name + '_error'] = int(result.nvmlReturn)
                values['pcie_generation'] = n.nvmlDeviceGetCurrPcieLinkGeneration(h)
                values['pcie_width'] = n.nvmlDeviceGetCurrPcieLinkWidth(h)
                data.append(values)
                pair.reverse()
            stream.write(json.dumps({'start_unix_ns': previous, 'end_unix_ns': stamp,
                                     'gpus': data}) + '\n')
            previous = stamp
            if first:
                assert all(all(v[name] is not None for name in metrics.values()) for v in data), data
                (directory / 'gpm-ready.json').write_text(json.dumps(receipt, indent=2) + '\n')
                first = False
finally:
    for h, pair in buffers:
        for sample in pair:
            n.nvmlGpmSampleFree(sample)
    for record in records:
        try:
            h = n.nvmlDeviceGetHandleByIndex(record['index'])
            n.nvmlGpmSetStreamingEnabled(h, record['original_streaming'])
            restored = n.nvmlGpmQueryIfStreamingEnabled(h)
            receipt['restoration'].append({'index': record['index'], 'streaming': restored,
                                          'passed': restored == record['original_streaming']})
        except Exception as error:
            receipt['restoration'].append({'index': record['index'], 'error': repr(error)})
    n.nvmlShutdown()
    (directory / 'gpm-receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
