#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Verify physical PCIe roots before assigning the diagnostic copy leases."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', type=Path, required=True)
a = p.parse_args()
groups = defaultdict(list)
for line in subprocess.check_output(['nvidia-smi', '--query-gpu=index,uuid,pci.bus_id',
                                    '--format=csv,noheader'], text=True).splitlines():
    index, uuid, bus = map(str.strip, line.split(','))
    domain, pci_bus, device = bus.lower().split(':')
    path = (Path('/sys/bus/pci/devices') / f'{domain[-4:]}:{pci_bus}:{device}').resolve()
    upstream = next(parent for parent in path.parents if parent.name.startswith('pci0000:'))
    groups[str(upstream)].append({'index': int(index), 'uuid': uuid, 'path': str(path)})
assert len(groups) == 4 and all(len(v) == 2 for v in groups.values())
rows = []
for group, members in sorted(groups.items()):
    label = group.rsplit('/', 1)[-1].replace(':', '-')
    rows.extend(member['uuid'] + '=' + label + '\n' for member in members)
root = a.work / 'native-scan-copy'
(root / 'gpu-pairs.properties').write_text(''.join(rows))
(root / 'gpu-pairs.json').write_text(json.dumps(groups, indent=2) + '\n')
