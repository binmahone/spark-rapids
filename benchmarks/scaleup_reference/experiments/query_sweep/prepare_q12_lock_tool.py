#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Install one checksum-pinned diagnostic binary in the owned work directory."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import urllib.request

work = Path(sys.argv[1]).resolve()
owner = json.loads((work / 'evidence/owner.json').read_text())
assert owner['instance_id'] and owner['thread_id']
version = 'v0.27.1'
url = f'https://github.com/bpftrace/bpftrace/releases/download/{version}/bpftrace-x86_64'
expected = 'c8d570302725bd1a3ed2f23bfa8d83dd78bde95daa98856279343052db5799c9'
target = work / 'tools' / ('bpftrace-' + version)
target.parent.mkdir(exist_ok=True)
if not target.exists():
    partial = target.with_suffix('.partial')
    with urllib.request.urlopen(url, timeout=60) as response, partial.open('wb') as output:
        while block := response.read(1024 * 1024):
            output.write(block)
    assert hashlib.sha256(partial.read_bytes()).hexdigest() == expected
    partial.chmod(0o755)
    partial.rename(target)
assert hashlib.sha256(target.read_bytes()).hexdigest() == expected
actual = subprocess.check_output([str(target), '--version'], text=True).strip()
assert version in actual, actual
receipt = {'url': url, 'sha256': expected, 'path': str(target), 'version': actual,
           'thread_id': owner['thread_id'], 'instance_id': owner['instance_id']}
(work / 'evidence/bpftrace-tool.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt))
