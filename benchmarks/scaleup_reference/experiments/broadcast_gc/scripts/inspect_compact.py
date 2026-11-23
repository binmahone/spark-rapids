#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""Read only the controller status and completed-case summaries."""
import json
from pathlib import Path
import sys

root = Path(sys.argv[1])
print((root / 'status.json').read_text().strip())
path = root / 'progress.json'
if path.exists():
    for row in json.loads(path.read_text()):
        keys = ['label', 'validated_samples', 'system_gc_events', 'full_system_gc_events',
                'retirement_calls', 'pair_wall_seconds', 'sql_seconds']
        print(json.dumps({k: row[k] for k in keys}))
for path in (root / 'failures').glob('*.json'):
    print(path.name, path.read_text().strip())
