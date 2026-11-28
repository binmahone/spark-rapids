#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Exercise real interprocess exclusion, independent pairs, and failure release."""
from pathlib import Path
import subprocess
import tempfile
import time

here = Path(__file__).resolve().parent
with tempfile.TemporaryDirectory(prefix='copy-lease-test-') as temporary:
    root = Path(temporary)
    binary = root / 'lease-test'
    subprocess.run(['g++', '-std=c++17', '-O2', '-pthread', '-I' + str(here),
                    str(here / 'copy_lease_test.cpp'), '-o', str(binary)], check=True)
    children = []
    def start(name, group, mode='hold'):
        child = subprocess.Popen([str(binary), str(root / (group + '.lock')),
            str(root / (name + '.ready')), str(root / (name + '.release')), mode])
        children.append(child)
        return child
    def ready(name):
        deadline = time.monotonic() + 5
        while not (root / (name + '.ready')).exists():
            assert time.monotonic() < deadline, name
            time.sleep(.01)
    try:
        first = start('first', 'pair0'); ready('first')
        second = start('second', 'pair0')
        independent = start('independent', 'pair1'); ready('independent')
        time.sleep(.2)
        assert not (root / 'second.ready').exists() and second.poll() is None
        (root / 'first.release').touch(); assert first.wait(timeout=5) == 0
        ready('second')
        for name, child in [('second', second), ('independent', independent)]:
            (root / (name + '.release')).touch(); assert child.wait(timeout=5) == 0
        failed = start('failed', 'pair0', 'throw')
        ready('failed')
        after = start('after-exception', 'pair0'); ready('after-exception')
        assert failed.poll() is None, 'Exception owner must stay alive to verify RAII release'
        (root / 'failed.release').touch(); assert failed.wait(timeout=5) == 1
        after.terminate(); assert after.wait(timeout=5) != 0
        after_death = start('after-death', 'pair0'); ready('after-death')
        (root / 'after-death.release').touch(); assert after_death.wait(timeout=5) == 0
        print('Exclusion, independent pairs, exception release and process-death release passed')
    finally:
        for child in children:
            if child.poll() is None: child.terminate()
            child.wait(timeout=5)
