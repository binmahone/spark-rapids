"""Run bounded critical-query probes with frozen artifacts and retained ownership."""
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
from critical_variants import cases as candidates
from run_screen import launch_group
from variants import ORDER, Variant, catalog, original
import services


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', required=True)
    p.add_argument('--thread-id', required=True)
    p.add_argument('--phase', choices=['oracle', 'screen', 'confirm', 'suite'], required=True)
    a = p.parse_args()
    work = Path(a.work).resolve()
    assert read(work / 'evidence/owner.json')['thread_id'] == a.thread_id
    assert Path('/proc/sys/vm/watermark_scale_factor').read_text().strip() == '10'
    assert subprocess.run(['systemctl', 'is-active', '--quiet', 'scaleup-reference-release.timer']).returncode != 0
    assert not subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader'], text=True).strip()
    for port in (19077, 19080):
        with socket.socket() as connection:
            connection.settimeout(1)
            assert connection.connect_ex((socket.gethostbyname(socket.gethostname()), port)) != 0
    directory = work / 'critical'
    directory.mkdir(exist_ok=True)
    output = directory / (a.phase + '-state.json')
    assert not output.exists()
    selection = read(work / 'exports/validated-261126/selected-cases-before-q11.json')
    assert selection['common_configuration_sha256'] == hashlib.sha256((HERE.parents[1] / 'configs/aws48x.json').read_bytes()).hexdigest()
    available = {(c.query, c.name): c for c in catalog(include_followups=True)}
    names = {c['query']: c['variant'] for c in selection['candidates']}
    control = {}
    for q in ORDER:
        case = available[(q, names[q])] if q in names else Variant(q, 'baseline', original(q), 'Retained reference')
        assert hashlib.sha256(case.sql.encode()).hexdigest() == selection['sql_sha256_by_query'][str(q)]
        control[q] = replace(case, overrides=selection['runtime_overrides_by_query'].get(str(q), {}))
    probes = {c.query: replace(c, overrides={**control[c.query].overrides, **c.overrides})
              for c in candidates() if c.name != 'critical_witness_wide'}
    report = {'status': 'running', 'phase': a.phase, 'groups': [],
              'baseline_selection_sha256': hashlib.sha256((work / 'exports/validated-261126/selected-cases-before-q11.json').read_bytes()).hexdigest(),
              'source_revision': subprocess.check_output(['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip(),
              'cache_policy': 'No cache flush, two actions per query per fresh application',
              'instance_policy': 'Retain until explicit user release'}
    write(directory / 'controller.json', {'pid': os.getpid(), 'owner_uid': os.getuid(), 'start_time': services.start_time(os.getpid())})
    write(output, report)
    def interrupted(signum, frame):
        raise InterruptedError('Signal ' + str(signum))
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    monitors, logs = [], []
    def run(label, values):
        report['active_group'] = label
        write(output, report)
        path, result = launch_group(work, 'critical-' + a.phase + '-' + label, 0, values, {})
        report['groups'].append({'label': label, 'run': str(path), 'status': result['status'],
                                 'samples': result['samples'], 'error': result.get('error'),
                                 'error_kind': result.get('error_kind'), 'active_query': result.get('active_query'),
                                 'driver_exit_code': result['driver_exit_code']})
        write(output, report)
        return result
    try:
        if a.phase == 'oracle':
            with (directory / 'oracle.log').open('w') as log:
                subprocess.run([str(work / 'spark-venv/bin/python'), str(HERE / 'critical_oracle.py'),
                    '--output', str(directory / 'oracle.json'), '--dataset',
                    '/opt/dlami/nvme/daily-trend-hardened/dataset'], stdout=log, stderr=subprocess.STDOUT,
                    check=True, timeout=900)
            assert read(directory / 'oracle.json')['status'] == 'passed'
        else:
            assert read(directory / 'oracle.json')['status'] == 'passed'
            for name, command in [
                ('host', [sys.executable, str(HERE.parent / 'runtime_prefilter/monitor_host.py'), '--work', str(work),
                          '--output', str(directory / (a.phase + '-host.jsonl'))]),
                ('vm', [sys.executable, str(HERE / 'monitor_vm.py'), '--work', str(work), '--thread-id', a.thread_id,
                        '--controller', str(directory / 'controller.json'), '--output', str(directory / (a.phase + '-vm.jsonl'))])]:
                log = (directory / (a.phase + '-' + name + '.log')).open('w'); logs.append(log)
                monitors.append(subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT))
            time.sleep(2)
            assert all(m.poll() is None for m in monitors)
            if a.phase == 'screen':
                result = run('control', [control[q] for q in (9, 8, 21)])
                assert result['status'] == 'completed' and result['driver_exit_code'] == 0
                pending, attempt = [probes[q] for q in (9, 8, 21)], 0
                while pending:
                    result = run('candidate' + str(attempt), pending)
                    done = {q for q in probes if sum(s['query'] == q and s['validation']['status'] == 'passed'
                            for s in result['samples']) == 2}
                    pending = [c for c in pending if c.query not in done]
                    if result['status'] != 'completed':
                        assert result.get('error_kind') in ('MechanismNotReached', 'TimeoutError'), result.get('error')
                        active = result['active_query']
                        assert active in [c.query for c in pending]
                        pending = [c for c in pending if c.query != active]
                    else:
                        assert not pending and result['driver_exit_code'] == 0
                    attempt += 1
            else:
                choice = read(directory / 'selection.json')
                assert choice['baseline_selection_sha256'] == report['baseline_selection_sha256']
                queries = choice['queries']
                assert queries and set(queries) <= {8, 9, 21}
                if a.phase == 'confirm':
                    for index, arm in enumerate('ABBA'):
                        result = run(arm + str(index), [(control if arm == 'A' else probes)[q] for q in queries])
                        assert result['status'] == 'completed' and result['driver_exit_code'] == 0
                else:
                    assert choice['confirmed']
                    result = run('combined', [probes[q] if q in queries else control[q] for q in ORDER])
                    assert result['status'] == 'completed' and result['driver_exit_code'] == 0
        report['status'] = 'completed'
    except BaseException as error:
        report.update(status='failed', error=repr(error))
        raise
    finally:
        for m in monitors:
            if m.poll() is None:
                m.terminate()
        for m in monitors:
            m.wait(timeout=20)
        for log in logs:
            log.close()
        report['monitor_exit_codes'] = [m.returncode for m in monitors]
        write(output, report)


if __name__ == '__main__':
    main()
