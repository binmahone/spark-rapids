#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Install one checksum-pinned diagnostic binary in the owned work directory."""
import hashlib
import json
import os
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
extracted = work / 'tools' / ('bpftrace-' + version + '-extracted')
if not extracted.exists():
    staging = work / 'tools' / ('bpftrace-' + version + '-extracting')
    staging.mkdir()
    with (work / 'evidence/bpftrace-extraction.log').open('w') as output:
        subprocess.run([str(target), '--appimage-extract'], cwd=staging,
                       stdout=output, stderr=subprocess.STDOUT, check=True, timeout=300)
    (staging / 'squashfs-root').rename(extracted)
    staging.rmdir()
binary = extracted / 'AppRun'
prefix = [] if os.geteuid() == 0 else ['sudo', '-n']
actual = subprocess.check_output(prefix + [str(binary), '--version'], text=True,
                                 timeout=30).strip()
assert version in actual, actual
receipt = {'url': url, 'archive_sha256': expected, 'archive_path': str(target),
           'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
           'path': str(binary), 'version': actual,
           'thread_id': owner['thread_id'], 'instance_id': owner['instance_id']}
(work / 'evidence/bpftrace-tool.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt))
