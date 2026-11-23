#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""Collect terminal trial evidence through private S3, excluding raw Spark event logs."""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import tarfile

import botocore.session

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--remote-run', required=True)
parser.add_argument('--s3-prefix', required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--ssh-config', required=True)
args = parser.parse_args()
bucket, prefix = args.s3_prefix[5:].split('/', 1)
key = prefix.rstrip('/') + '/evidence.tar.gz'
client = botocore.session.get_session().create_client('s3', region_name='us-east-1')
ticket = client.generate_presigned_url('put_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=900)
remote = '''
import hashlib,json,sys,tarfile,urllib.request
from pathlib import Path
x=json.load(sys.stdin)
root=Path(x['root'])
assert str(root).startswith('/home/mahonem/wild_spark_rapids/261123_scaleup_reference_gc/.build/')
status=json.loads((root/'status.json').read_text())
assert status['status'] in ['completed','failed']
files=set(root.glob('*.json'))|set(root.glob('*.txt'))|set(root.glob('slurm-*.log'))
for case in (root/'runs').iterdir():
    files.update(case.glob('*.json'))
    for folder in ['nodes','gc','node-controller','services']:
        files.update(p for p in (case/folder).rglob('*') if p.is_file())
    for app in (case/'applications').glob('*'):
        files.update(p for p in app.iterdir() if p.is_file() and p.suffix in ['.json','.log','.conf','.txt'])
archive=root/'evidence.tar.gz'
with tarfile.open(archive,'w:gz') as out:
    for path in sorted(files):out.add(path,arcname=str(path.relative_to(root)),recursive=False)
body=archive.read_bytes()
with urllib.request.urlopen(urllib.request.Request(x['url'],data=body,method='PUT'),timeout=120) as response:
    assert response.status==200
print(json.dumps({'sha256':hashlib.sha256(body).hexdigest(),'bytes':len(body),'files':len(files),'status':status['status']}))
'''
command = ['ssh', '-C', '-F', args.ssh_config, '-o', 'KexAlgorithms=curve25519-sha256',
           '-o', 'ConnectTimeout=8', '-o', 'ServerAliveInterval=5', '-o', 'ServerAliveCountMax=2',
           'presto-cp-02-via-mac', 'python3 -c ' + shlex.quote(remote)]
result = subprocess.run(command, input=json.dumps({'root': args.remote_run, 'url': ticket}),
                        capture_output=True, text=True, timeout=180)
if result.returncode:
    raise RuntimeError('Evidence upload failed; signed ticket omitted from error output')
receipt = json.loads(result.stdout)
args.output.mkdir(parents=True, exist_ok=False)
archive = args.output / 'evidence.tar.gz'
response = client.get_object(Bucket=bucket, Key=key)
with archive.open('wb') as stream:
    for chunk in iter(lambda: response['Body'].read(1024 * 1024), b''):
        stream.write(chunk)
assert hashlib.sha256(archive.read_bytes()).hexdigest() == receipt['sha256']
with tarfile.open(archive) as source:
    source.extractall(args.output / 'run', filter='data')
receipt.update(s3_uri='s3://' + bucket + '/' + key, remote_run=args.remote_run)
(args.output / 'collection.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt, indent=2))
