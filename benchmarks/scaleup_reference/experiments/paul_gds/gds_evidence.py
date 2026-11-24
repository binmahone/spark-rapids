# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""Observe the metrics and GDS-required decisions checked by the unmodified tests."""
import json
import os
from pathlib import Path

_current = None


def record(values):
    path = Path(os.environ['PAUL_GDS_METRICS_LOG'])
    with path.open('a') as output:
        output.write(json.dumps(dict(test=_current, **values)) + '\n')


def pytest_runtest_setup(item):
    global _current
    _current = item.nodeid
    module = item.module
    if not module.__name__.endswith('hybrid_scan_test') or getattr(module, '_gds_observer_attached', False):
        return
    original_metric = module.plan_metric
    original_required = module._gds_required

    def observe_metric(plan, name):
        value = original_metric(plan, name)
        record({'metric': name, 'value': value})
        return value

    def observe_required():
        required = original_required()
        record({'gds_required': required})
        return required

    module.plan_metric = observe_metric
    module._gds_required = observe_required
    module._gds_observer_attached = True
