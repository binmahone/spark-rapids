#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""Stage small diagnostic artifacts using private S3 and in-memory download tickets."""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import subprocess

import botocore.session
from botocore.config import Config

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--build-directory', type=Path, required=True)
parser.add_argument('--s3-prefix', required=True)
parser.add_argument('--remote-directory', required=True)
parser.add_argument('--ssh-config', required=True)
args = parser.parse_args()
assert args.s3_prefix.startswith('s3://')
bucket, prefix = args.s3_prefix[5:].split('/', 1)
client = botocore.session.get_session().create_client('s3', region_name='us-east-1',
                                                     config=Config(signature_version='s3v4'))
identity = botocore.session.get_session().create_client('sts').get_caller_identity()
assert identity['Account'] == '503960270539' and identity['Arn'].endswith(':user/mahone')
items = []
for name in ['broadcast-gc-tools.jar', 'build-receipt.json']:
    body = (args.build_directory / name).read_bytes()
    key = prefix.rstrip('/') + '/' + name
    client.put_object(Bucket=bucket, Key=key, Body=body)
    items.append({'name': name, 'sha256': hashlib.sha256(body).hexdigest(),
                  'url': client.generate_presigned_url('get_object',
                      Params={'Bucket': bucket, 'Key': key}, ExpiresIn=900)})
remote = '''
import hashlib,json,sys,urllib.request
from pathlib import Path
x=json.load(sys.stdin)
root=Path(x['directory'])
assert str(root).startswith('/home/mahonem/wild_spark_rapids/261123_scaleup_reference_gc/.build/')
root.mkdir(parents=True,exist_ok=True)
for item in x['items']:
    path=root/item['name']
    if not path.exists():
        staging=path.with_suffix(path.suffix+'.staging')
        with urllib.request.urlopen(item['url'],timeout=90) as response:body=response.read()
        assert hashlib.sha256(body).hexdigest()==item['sha256']
        staging.write_bytes(body)
        staging.replace(path)
    assert hashlib.sha256(path.read_bytes()).hexdigest()==item['sha256']
print(json.dumps({'directory':str(root),'verified_files':[i['name'] for i in x['items']]}))
'''
command = ['ssh', '-C', '-F', args.ssh_config, '-o', 'KexAlgorithms=curve25519-sha256',
           '-o', 'ConnectTimeout=8', '-o', 'ServerAliveInterval=5', '-o', 'ServerAliveCountMax=2',
           'presto-cp-02-via-mac', 'python3 -c ' + shlex.quote(remote)]
result = subprocess.run(command, input=json.dumps({'directory': args.remote_directory, 'items': items}),
                        capture_output=True, text=True, timeout=180)
if result.returncode:
    raise RuntimeError('Remote staging failed; signed tickets omitted from error output')
print(result.stdout)
