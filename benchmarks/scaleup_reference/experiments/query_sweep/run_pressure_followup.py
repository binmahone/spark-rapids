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
"""Measure a reversible Linux reclaim policy with fixed SQL, data, and engine artifacts."""
import argparse
from dataclasses import replace
import hashlib
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write
from run_screen import launch_group
from variants import ORDER, Variant, catalog, original
import services


def kernel_state():
    settings = {key: Path('/proc/sys/vm/' + key).read_text().strip() for key in
                ('watermark_scale_factor', 'min_free_kbytes', 'watermark_boost_factor', 'zone_reclaim_mode')}
    return {'time_unix_ns': time.time_ns(), 'settings': settings,
            'zoneinfo': Path('/proc/zoneinfo').read_text(),
            'meminfo': Path('/proc/meminfo').read_text()}


def set_watermark(value):
    result = subprocess.check_output(['sudo', '-n', 'sysctl', '-w',
                                     'vm.watermark_scale_factor=' + str(value)], text=True)
    assert int(Path('/proc/sys/vm/watermark_scale_factor').read_text()) == value
    return result.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True)
    parser.add_argument('--thread-id', required=True)
    parser.add_argument('--mode', choices=('screen', 'full'), required=True)
    args = parser.parse_args()
    work = Path(args.work).resolve()
    assert read(work / 'evidence/owner.json')['thread_id'] == args.thread_id
    assert (work / 'evidence/ceiling-post.exit').read_text().strip() == '0'
    assert subprocess.run(['systemctl', 'is-active', '--quiet', 'scaleup-reference-release.timer']).returncode != 0
    for port in (19077, 19080):
        with socket.socket() as connection:
            connection.settimeout(1)
            assert connection.connect_ex((socket.gethostbyname(socket.gethostname()), port)) != 0
    gpu = subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader'], text=True)
    assert not gpu.strip(), 'A GPU process is already active'
    directory = work / 'pressure-followup' / args.mode
    directory.mkdir(parents=True, exist_ok=False)
    output = directory / 'state.json'
    selection_path = work / 'exports/validated-261126/selected-cases-before-q11.json'
    selection = read(selection_path)
    assert hashlib.sha256((HERE.parents[1] / 'configs/aws48x.json').read_bytes()).hexdigest() == selection['common_configuration_sha256']
    available = {(case.query, case.name): case for case in catalog(include_followups=True)}
    variants = {row['query']: row['variant'] for row in selection['candidates']}
    queries = [9, 10, 11, 12, 13, 18] if args.mode == 'screen' else ORDER
    cases = []
    for query in queries:
        case = (available[(query, variants[query])] if query in variants else
                Variant(query, 'baseline', original(query), 'Unchanged reference SQL'))
        assert hashlib.sha256(case.sql.encode()).hexdigest() == selection['sql_sha256_by_query'][str(query)]
        cases.append(replace(case, overrides=selection['runtime_overrides_by_query'].get(str(query), {})))
    before = kernel_state()
    original_value = int(before['settings']['watermark_scale_factor'])
    assert original_value == 10, 'Historical control value differs'
    report = {'status': 'running', 'classification': 'Controlled host-reclaim experiment, not a suite score',
              'queries': queries, 'sequence': 'ABBA', 'control': 10, 'candidate': 100,
              'groups': [], 'before': before, 'selection': str(selection_path),
              'selection_sha256': hashlib.sha256(selection_path.read_bytes()).hexdigest(),
              'source_revision': subprocess.check_output(['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip(),
              'limits': ['No physical file-cache flush; first and second actions are both retained',
                         'Memory pressure must occur in controls before claiming a remedy for long tails'],
              'instance_policy': 'Retain until explicit user release'}
    controller = {'pid': os.getpid(), 'owner_uid': os.getuid(), 'start_time': services.start_time(os.getpid())}
    write(directory / 'controller.json', controller)
    write(output, report)
    def interrupted(signum, frame):
        raise InterruptedError('Controller received signal ' + str(signum))
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    children = []
    log_files = []
    try:
        for name, command in [
            ('host', [sys.executable, str(HERE.parent / 'runtime_prefilter/monitor_host.py'),
                      '--work', str(work), '--output', str(directory / 'host-monitor.jsonl')]),
            ('vm', [sys.executable, str(HERE / 'monitor_vm.py'), '--work', str(work),
                    '--thread-id', args.thread_id, '--controller', str(directory / 'controller.json'),
                    '--output', str(directory / 'vm-monitor.jsonl')])]:
            log = (directory / (name + '-monitor.log')).open('w')
            log_files.append(log)
            children.append(subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT))
        time.sleep(2)
        assert all(child.poll() is None for child in children), 'Sampling process failed'
        for index, arm in enumerate('ABBA'):
            value = original_value if arm == 'A' else 100
            mutation = set_watermark(value)
            current = kernel_state()
            assert {k: v for k, v in current['settings'].items() if k != 'watermark_scale_factor'} == {
                k: v for k, v in before['settings'].items() if k != 'watermark_scale_factor'}
            report.update(active_arm=arm + str(index), active_setting=value)
            write(output, report)
            run, result = launch_group(work, 'pressure-' + args.mode + '-' + arm + str(index), 0, cases, {})
            report['groups'].append({'arm': arm, 'index': index, 'run': str(run),
                'watermark_scale_factor': value, 'sysctl_command_output': mutation,
                'before': current, 'after': kernel_state(), 'status': result['status'],
                'driver_exit_code': result['driver_exit_code'], 'samples': result['samples']})
            write(output, report)
            assert result['status'] == 'completed' and result['driver_exit_code'] == 0
            assert report['groups'][-1]['after']['settings']['watermark_scale_factor'] == str(value)
            assert len(result['samples']) == 2 * len(queries)
            assert all(s['validation']['status'] == 'passed' for s in result['samples'])
            assert all(child.poll() is None for child in children)
        report['status'] = 'completed'
    except BaseException as error:
        report.update(status='failed', error=repr(error))
        raise
    finally:
        try:
            report['restore_command_output'] = set_watermark(original_value)
            report['restored'] = kernel_state()
        finally:
            for child in children:
                if child.poll() is None:
                    child.terminate()
            for child in children:
                child.wait(timeout=20)
            for log in log_files:
                log.close()
            report['monitor_exit_codes'] = [child.returncode for child in children]
            write(output, report)


if __name__ == '__main__':
    main()
