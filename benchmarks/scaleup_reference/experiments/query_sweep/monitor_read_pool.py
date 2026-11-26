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
"""Record the actual native read pool of executors owned by this work directory."""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', required=True)
p.add_argument('--output', required=True)
a = p.parse_args()
work = str(Path(a.work).resolve()).encode()
running = True

def stop(*unused):
    global running
    running = False

signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
with Path(a.output).open('x', buffering=1) as output:
    while running:
        executors = []
        for proc in Path('/proc').iterdir():
            if not proc.name.isdigit():
                continue
            try:
                if proc.stat().st_uid != os.getuid():
                    continue
                command = (proc / 'cmdline').read_bytes()
                if b'CoarseGrainedExecutorBackend' not in command or work not in command:
                    continue
                names = []
                for thread in (proc / 'task').iterdir():
                    try:
                        name = (thread / 'comm').read_text().strip()
                        if re.fullmatch(r'kvikio-\d+', name):
                            names.append(name)
                    except (FileNotFoundError, ProcessLookupError):
                        pass
                executors.append({'pid': int(proc.name), 'thread_names': sorted(names)})
            except (FileNotFoundError, ProcessLookupError, PermissionError):
                pass
        output.write(json.dumps({'time_unix_ns': time.time_ns(), 'executors': executors}) + '\n')
        time.sleep(2)
