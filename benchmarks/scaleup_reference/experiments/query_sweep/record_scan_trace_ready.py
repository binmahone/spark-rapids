#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Record the profiler's collection-start callback before launching GPU executors."""
import json
import os
from pathlib import Path
import sys
import time

directory = Path(sys.argv[1])
assert os.environ['NSYS_SESSION_NAME'] == sys.argv[2]
with (directory / 'nsys-ready.json').open('x') as stream:
    json.dump({'session': os.environ['NSYS_SESSION_NAME'], 'time_unix_ns': time.time_ns(),
               'monotonic_ns': time.monotonic_ns()}, stream)
