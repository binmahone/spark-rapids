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
"""Compare domain-proven int32 join keys with the same selected SQL and original reader."""
import argparse
from dataclasses import replace
import hashlib
import json
import re
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
import services
from critical_variants import cases as candidates
from run_screen import launch_group
from variants import ORDER, Variant, catalog, original


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', required=True)
    p.add_argument('--thread-id', required=True)
    p.add_argument('--phase', choices=['A0', 'B1', 'B2', 'A3'], required=True)
    a = p.parse_args()
    work = Path(a.work).resolve()
    owner = read(work / 'evidence/owner.json')
    assert owner['thread_id'] == a.thread_id
    assert Path('/proc/sys/vm/watermark_scale_factor').read_text().strip() == '10'
    assert subprocess.run(['systemctl', 'is-active', '--quiet', 'scaleup-reference-release.timer']).returncode != 0
    assert not subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader'], text=True).strip()
    for port in (19077, 19080):
        with socket.socket() as s:
            assert s.connect_ex((socket.gethostbyname(socket.gethostname()), port)) != 0
    directory = work / 'narrow-keys' / a.phase
    directory.mkdir(parents=True, exist_ok=False)
    report = {'status': 'running', 'phase': a.phase, 'groups': [], 'profilers': [],
              'thread_id': a.thread_id, 'instance_id': owner['instance_id'],
              'source_revision': subprocess.check_output(['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip(),
              'classification': 'Internal join-key representation pilot; frozen engine and original reader',
              'tuning_key': 'spark.rapids.sql.reader.kvikio.numThreads',
              'kvikio_threads': 32,
              'cache_policy': 'No cache flush; two actions per query',
              'instance_policy': 'Release session-created instance after useful investigations and evidence retention'}
    write(directory / 'controller.json', {'pid': os.getpid(), 'owner_uid': os.getuid(),
                                         'start_time': services.start_time(os.getpid())})
    write(directory / 'state.json', report)
    children, logs = [], []
    def interrupt(signum, frame):
        raise InterruptedError('Signal ' + str(signum))
    signal.signal(signal.SIGTERM, interrupt)
    signal.signal(signal.SIGINT, interrupt)
    def start(name, command, privileged=False):
        log = (directory / (name + '.log')).open('x'); logs.append(log)
        if privileged:
            command = ['sudo', '-n'] + command
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        receipt = {'name': name, 'pid': process.pid, 'start_time': services.start_time(process.pid),
                   'command': command, 'privileged': privileged}
        report['profilers'].append(receipt)
        children.append((process, receipt))
        write(directory / 'state.json', report)
        return process
    try:
        start('host', [sys.executable, str(HERE.parent / 'runtime_prefilter/monitor_host.py'),
                       '--work', str(work), '--output', str(directory / 'host.jsonl')])
        start('vm', [sys.executable, str(HERE / 'monitor_vm.py'), '--work', str(work),
                     '--thread-id', a.thread_id, '--controller', str(directory / 'controller.json'),
                     '--output', str(directory / 'vm.jsonl')])
        start('pool', [sys.executable, str(HERE / 'monitor_read_pool.py'),
                       '--work', str(work), '--output', str(directory / 'pool.jsonl')])
        start('gpm', ['/usr/bin/python3', str(HERE / 'monitor_gpm.py'),
                      '--directory', str(directory)], privileged=True)
        deadline = time.monotonic() + 30
        while not (directory / 'gpm-ready.json').exists():
            assert all(p.poll() is None for p, _ in children), 'Monitor startup failed; inspect logs'
            assert time.monotonic() < deadline, 'GPM startup timeout'
            time.sleep(0.5)
        selection_path = work / 'exports/validated-261126/selected-cases-before-q11.json'
        selection = read(selection_path)
        assert selection['common_configuration_sha256'] == hashlib.sha256((HERE.parents[1] / 'configs/aws48x.json').read_bytes()).hexdigest()
        available = {(c.query, c.name): c for c in catalog(include_followups=True)}
        names = {c['query']: c['variant'] for c in selection['candidates']}
        controls = {}
        for q in ORDER:
            case = available[(q, names[q])] if q in names else Variant(q, 'baseline', original(q), 'Retained reference')
            assert hashlib.sha256(case.sql.encode()).hexdigest() == selection['sql_sha256_by_query'][str(q)]
            controls[q] = replace(case, overrides=selection['runtime_overrides_by_query'].get(str(q), {}))
        choice = read(work / 'critical/selection.json')
        assert choice['confirmed'] and choice['queries'] == [8, 21]
        for case in candidates():
            if case.query in choice['queries'] and case.name != 'critical_witness_wide':
                controls[case.query] = replace(case, overrides={**controls[case.query].overrides, **case.overrides})
        queries = [9]
        report['queries'] = queries
        report['start_unix_ns'] = time.time_ns()
        report['start_monotonic_ns'] = time.monotonic_ns()
        write(directory / 'state.json', report)
        proof = read(work / 'narrow-keys/oracle.json')
        assert proof['status'] == 'passed'
        report['domain_proof_sha256'] = hashlib.sha256((work / 'narrow-keys/oracle.json').read_bytes()).hexdigest()
        report['key_type'] = 'bigint' if a.phase.startswith('A') else 'int'
        key_columns = {'lineitem': ['l_partkey', 'l_suppkey'], 'part': ['p_partkey'],
                       'partsupp': ['ps_partkey', 'ps_suppkey'], 'supplier': ['s_suppkey']}
        report['narrowed_columns'] = [key for keys in key_columns.values() for key in keys
                                      if report['key_type'] == 'int' and
                                      (a.phase == 'B1' or key.endswith('suppkey'))]
        view_casts = {'9': {table: {key: 'int' if key in report['narrowed_columns'] else 'bigint'
                                  for key in keys}
                           for table, keys in key_columns.items()}}
        path, result = launch_group(work, 'narrow-keys-' + a.phase, 0,
                                    [controls[q] for q in queries], {report['tuning_key']: '32'}, view_casts=view_casts, domain_proof=work / 'narrow-keys/oracle.json')
        report['groups'].append({'run': str(path), 'status': result['status'],
                                 'samples': result['samples'], 'error': result.get('error'),
                                 'driver_exit_code': result['driver_exit_code']})
        assert result['status'] == 'completed' and result['driver_exit_code'] == 0
        assert len(result['samples']) == 2 * len(queries)
        assert all(s['validation']['status'] == 'passed' for s in result['samples'])
        pool_rows = [json.loads(line) for line in (directory / 'pool.jsonl').open()]
        observed = {}
        for row in pool_rows:
            for record in row['executors']:
                observed.setdefault(record['pid'], set()).update(record['thread_names'])
        assert len(observed) == 8, observed
        expected = {'kvikio-' + str(i) for i in range(report['kvikio_threads'])}
        assert all(names == expected for names in observed.values()), observed
        scratch = Path(read(path / 'config.json')['deployment']['scratch_root']) / path.name
        messages = []
        for stderr in scratch.rglob('stderr'):
            for line in stderr.read_text(errors='replace').splitlines():
                if 'KvikIO reads with compatibility mode' in line:
                    messages.append({'path': str(stderr), 'line': line})
        assert len(messages) == 8, messages
        assert all(re.search(r'and ' + str(report['kvikio_threads']) + r' threads', m['line']) for m in messages)
        report['pool_verification'] = {'observed_thread_names': {str(pid): sorted(names) for pid, names in observed.items()},
                                       'initialization_messages': messages, 'passed': True}
        assert all(p.poll() is None for p, _ in children), 'Profiler exited before experiment completion'
        report['status'] = 'completed'
    except BaseException as error:
        report.update(status='failed', error=repr(error))
        raise
    finally:
        for process, receipt in reversed(children):
            if process.poll() is None:
                assert services.start_time(process.pid) == receipt['start_time']
                process.send_signal(signal.SIGINT if receipt['name'] in ('cpu', 'reclaim') else signal.SIGTERM)
        for process, receipt in children:
            try:
                receipt['exit_code'] = process.wait(timeout=25)
            except subprocess.TimeoutExpired:
                report['status'] = 'failed'
                receipt['cleanup_error'] = 'Process did not exit after signal; investigate recorded PID'
        for log in logs:
            log.close()
        report['end_unix_ns'] = time.time_ns()
        if (directory / 'gpm-receipt.json').exists():
            restoration = read(directory / 'gpm-receipt.json')['restoration']
            report['gpm_restored'] = len(restoration) == 8 and all(r.get('passed') for r in restoration)
            if not report['gpm_restored']:
                report['status'] = 'failed'
        else:
            report['status'] = 'failed'
        write(directory / 'state.json', report)


if __name__ == '__main__':
    main()
