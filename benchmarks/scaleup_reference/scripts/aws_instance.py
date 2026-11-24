#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
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

"""Allocate or release one explicitly owned AWS reference host; separate from benchmark submission."""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import subprocess
import urllib.request

from common import read, write


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('allocate', 'status', 'terminate'))
    parser.add_argument('--state', required=True, help='Directory for cloud ownership receipts')
    parser.add_argument('--region', default='us-east-2')
    parser.add_argument('--ami', default='ami-0e1e73dead0bb279d')
    parser.add_argument('--subnet', default='subnet-e76cdcab')
    parser.add_argument('--instance-profile', default='mahonem-wild-sf10k-261012')
    parser.add_argument('--public-key', default=str(Path.home() / '.ssh/id_ed25519.pub'))
    parser.add_argument('--hours', type=int, choices=range(1, 9), default=4)
    parser.add_argument('--market', choices=('spot', 'on-demand'), default='spot')
    args = parser.parse_args()
    state = Path(args.state).resolve()
    state.mkdir(parents=True, exist_ok=True)
    thread = os.environ.get('CODEX_THREAD_ID')
    if args.action == 'allocate' and not thread:
        parser.error('Set CODEX_THREAD_ID to a unique workstream ownership identifier')
    receipt_path = state / 'allocation.json'
    if receipt_path.exists():
        receipt = read(receipt_path)
        args.region = receipt['region']
        if thread and receipt['thread_id'] != thread:
            raise ValueError('Cloud receipt belongs to another workstream')
    else:
        receipt = None

    def aws(*command):
        result = subprocess.run(['aws', '--region', args.region, '--output', 'json', *command],
                                capture_output=True, text=True, timeout=120,
                                env=dict(os.environ, AWS_MAX_ATTEMPTS='1'))
        if result.returncode:
            raise RuntimeError(result.stderr.strip())
        return json.loads(result.stdout or '{}')

    identity = aws('sts', 'get-caller-identity')
    if receipt and identity['Account'] != receipt['account']:
        raise ValueError('AWS account differs from the ownership receipt')
    if args.action in ('status', 'terminate'):
        if receipt is None:
            parser.error('No allocation ownership receipt exists')
        nodes = aws('ec2', 'describe-instances', '--instance-ids', receipt['instance_id'])
        node = nodes['Reservations'][0]['Instances'][0]
        tags = {t['Key']: t['Value'] for t in node.get('Tags', [])}
        if tags.get('CodexThreadId') != receipt['thread_id']:
            raise ValueError('Instance ownership tag differs')
        if args.action == 'terminate' and node['State']['Name'] != 'terminated':
            aws('ec2', 'terminate-instances', '--instance-ids', receipt['instance_id'])
            print('Termination requested: ' + receipt['instance_id'])
        else:
            print(json.dumps({'instance_id': node['InstanceId'], 'state': node['State']['Name'],
                              'public_ip': node.get('PublicIpAddress'),
                              'release_deadline_utc': receipt['release_deadline_utc']}, indent=2))
        return
    if receipt:
        raise ValueError('An allocation receipt exists; inspect it before allocating another host')
    name = 'scaleup-reference-' + thread[:12]
    existing = aws('ec2', 'describe-instances', '--filters',
                   'Name=tag:Workstream,Values=' + name,
                   'Name=instance-state-name,Values=pending,running,stopping,stopped')
    if existing['Reservations']:
        raise ValueError('A tagged instance exists; reconcile it before making another request')
    image = aws('ec2', 'describe-images', '--image-ids', args.ami)['Images'][0]
    if image['State'] != 'available' or image['Architecture'] != 'x86_64':
        raise ValueError('AMI is unavailable or has the wrong architecture')
    subnet = aws('ec2', 'describe-subnets', '--subnet-ids', args.subnet)['Subnets'][0]
    deadline = (datetime.now(timezone.utc) + timedelta(hours=args.hours)).strftime('%Y-%m-%d %H:%M:%S UTC')
    tags = [{'Key': k, 'Value': v} for k, v in {
        'Name': name, 'Workstream': name, 'Owner': 'mahonem', 'CodexThreadId': thread,
        'Purpose': 'Reusable Scale-Up reference validation', 'ReleaseDeadlineUTC': deadline}.items()]
    access_file = state / 'access-resources.json'
    if access_file.exists():
        access = read(access_file)
        if access['thread_id'] != thread or access['region'] != args.region:
            raise ValueError('Access resource ownership differs')
    else:
        ip = urllib.request.urlopen('https://checkip.amazonaws.com', timeout=20).read().decode().strip()
        ipaddress.IPv4Address(ip)
        group = aws('ec2', 'create-security-group', '--group-name', name,
                     '--description', 'Owned Scale-Up reference host', '--vpc-id', subnet['VpcId'],
                     '--tag-specifications', json.dumps([{'ResourceType': 'security-group', 'Tags': tags}]))['GroupId']
        access = {'region': args.region, 'thread_id': thread, 'security_group_id': group, 'key_name': name}
        write(access_file, access)
        aws('ec2', 'authorize-security-group-ingress', '--group-id', group, '--ip-permissions',
             json.dumps([{'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22,
                           'IpRanges': [{'CidrIp': ip + '/32'}]}]))
        aws('ec2', 'import-key-pair', '--key-name', name, '--public-key-material',
             'fileb://' + str(Path(args.public_key).resolve()), '--tag-specifications',
             json.dumps([{'ResourceType': 'key-pair', 'Tags': tags}]))
    prepare = Path(__file__).with_name('prepare_aws_host.sh').read_text()
    bootstrap = '''#!/usr/bin/env bash
set -euo pipefail
exec > /var/log/scaleup-reference-bootstrap.log 2>&1
cat > /etc/systemd/system/scaleup-reference-release.service <<'EOF'
[Unit]
Description=Release the owned Scale-Up reference instance
[Service]
Type=oneshot
ExecStart=/usr/sbin/poweroff
EOF
cat > /etc/systemd/system/scaleup-reference-release.timer <<'EOF'
[Timer]
OnCalendar=DEADLINE
Persistent=true
Unit=scaleup-reference-release.service
[Install]
WantedBy=timers.target
EOF
systemctl daemon-reload
systemctl enable --now scaleup-reference-release.timer
'''.replace('DEADLINE', deadline)
    bootstrap += prepare + '\ntouch /var/lib/scaleup-reference-prepared\nsystemctl reboot\n'
    userdata = state / 'user-data.sh'
    userdata.write_text(bootstrap)
    command = ['ec2', 'run-instances', '--image-id', args.ami, '--instance-type', 'g7e.48xlarge',
        '--count', '1', '--subnet-id', args.subnet, '--key-name', access['key_name'],
        '--security-group-ids', access['security_group_id'], '--associate-public-ip-address',
        '--instance-initiated-shutdown-behavior', 'terminate', '--iam-instance-profile',
        json.dumps({'Name': args.instance_profile}), '--metadata-options',
        json.dumps({'HttpTokens': 'required', 'HttpEndpoint': 'enabled'}),
        '--block-device-mappings', json.dumps([{'DeviceName': image['RootDeviceName'],
            'Ebs': {'VolumeSize': 200, 'VolumeType': 'gp3', 'Encrypted': True, 'DeleteOnTermination': True}}]),
        '--tag-specifications', json.dumps([{'ResourceType': kind, 'Tags': tags}
            for kind in ('instance', 'volume', 'network-interface')]),
        '--user-data', 'file://' + str(userdata), '--client-token',
        hashlib.sha256((thread + args.region + args.market + args.ami + args.subnet).encode()).hexdigest()]
    if args.market == 'spot':
        command += ['--instance-market-options', json.dumps({'MarketType': 'spot',
                      'SpotOptions': {'SpotInstanceType': 'one-time', 'InstanceInterruptionBehavior': 'terminate'}})]
    write(state / 'allocation-command.json', {'argv': command, 'identity': identity})
    reply = aws(*command)
    node = reply['Instances'][0]
    receipt = {**access, 'account': identity['Account'], 'instance_id': node['InstanceId'],
                'ami': args.ami, 'subnet': args.subnet, 'market': args.market,
                'release_deadline_utc': deadline}
    write(receipt_path, receipt)
    print(json.dumps(receipt, indent=2))


if __name__ == '__main__':
    main()
