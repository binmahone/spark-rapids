#!/usr/bin/env python3
"""Screen all queries, retain rejected probes, and leave the EC2 instance running."""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
SUITE = HERE.parents[1]
sys.path.insert(0, str(SUITE / 'scripts'))
from common import expand, load_config, read, sha, write
import services
from variants import ORDER, catalog, extra_cases, targeted


def launch_group(work, phase, index, cases, global_conf, collector=None,
                 view_casts=None, domain_proof=None):
    deployment = read(work / 'evidence/deployment.json')
    config = load_config('aws48x', work / 'evidence/deployment.json')
    config['spark_conf'].update(global_conf)
    queries = [case.query for case in cases]
    artifacts = work / 'artifacts/main'
    engine = artifacts / 'cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar'
    if sha(engine) != 'c267dafd65ae85b558d8c8df03a94c0947cbe42b79a1e29ddeaa63bfcadb8cce':
        raise AssertionError('Frozen engine differs')
    if sha(artifacts / 'benchmark-tools.jar') != '4d28e4919d16b6a607092f91db1e271afe837100550251976c330ee25a1a2b90':
        raise AssertionError('Frozen validation tools differ')
    config.update(engine=str(engine), engine_sha256=sha(engine),
        tools=str(artifacts / 'benchmark-tools.jar'), queries=queries,
        case_variants={str(c.query): c.name for c in cases},
        case_overrides={str(c.query): c.overrides for c in cases},
        experiment_source_revision=subprocess.check_output(
            ['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip(),
        experiment_source_sha256={str(p.relative_to(SUITE)): sha(p) for p in [
            HERE / 'run_screen.py', HERE / 'run_cases.py', HERE / 'run_ceiling.py',
            HERE / 'variants.py', HERE / 'ceiling_variants.py',
            HERE / 'critical_variants.py', HERE / 'critical_oracle.py',
            SUITE / 'scripts/common.py', SUITE / 'scripts/services.py',
            SUITE / 'scripts/catalog_statistics.py', SUITE / 'scripts/validation.py']},
        classification='Unqualified screening, not a Daily Trend score')
    helper = None
    if view_casts is not None:
        assert domain_proof is not None
        proof = read(domain_proof)
        assert proof['status'] == 'passed' and proof['dataset_bounds']['dataset'] == config['dataset']
        config['view_overrides'].update(view_casts)
        config['key_domain_proof'] = {'path': str(domain_proof), 'sha256': sha(domain_proof)}
    if collector is not None:
        assert collector in ('original', 'early-leaves')
        helper = work / 'artifacts/early-shuffle/early-shuffle.jar'
        assert helper.exists()
        config['experimental_collector'] = {'mode': collector, 'jar': str(helper),
                                            'sha256': sha(helper)}
    profiles = expand(config, queries)
    for case in cases:
        profiles[str(case.query)].update(case.overrides)
    run = work / 'screen' / f'{phase}-{index}'
    app = run / 'applications/suite'
    app.mkdir(parents=True)
    write(run / 'config.json', config)
    profile = dict(profiles[str(queries[0])])
    host = socket.gethostbyname(socket.gethostname())
    os.environ['SCALEUP_HEAD'] = socket.gethostname()
    scratch = Path(deployment['scratch_root']) / run.name
    for name in ['events', 'driver', 'java-tmp']:
        (scratch / name).mkdir(parents=True, exist_ok=True)
    native = str(Path(deployment['ucx_home']) / 'lib')
    native += ':/usr/local/cuda-13.1/lib64:/usr/local/cuda-13.1/targets/x86_64-linux/lib'
    classpath = str(engine) + ':' + config['tools']
    if helper:
        classpath += ':' + str(helper)
    runtime = {'spark.master': f'spark://{host}:19077', 'spark.cores.max': '64',
        'spark.driver.host': host, 'spark.driver.bindAddress': host, 'spark.ui.port': '0',
        'spark.eventLog.enabled': 'true', 'spark.eventLog.compress': 'false',
        'spark.eventLog.dir': 'file://' + str(scratch / 'events'),
        'spark.local.dir': str(scratch / 'driver'),
        'spark.driver.extraClassPath': classpath, 'spark.executor.extraClassPath': classpath,
        'spark.executorEnv.PYSPARK_PYTHON': '/usr/bin/python3',
        'spark.executorEnv.LD_LIBRARY_PATH': native,
        'spark.executorEnv.UCX_MODULE_DIR': str(Path(deployment['ucx_home']) / 'lib/ucx')}
    for component in ['driver', 'executor']:
        runtime[f'spark.{component}.extraJavaOptions'] = '-Djava.io.tmpdir=' + str(scratch / 'java-tmp')
    profile.update(runtime)
    for value in profiles.values():
        value.update(runtime)
    write(app / 'profile.json', profile)
    write(run / 'profiles.json', profiles)
    properties = app / 'spark-defaults.conf'
    properties.write_text(''.join(f'{k}={v}\n' for k, v in sorted(profile.items())))
    env = dict(os.environ, JAVA_HOME=deployment['java_home'], SPARK_HOME=deployment['spark_home'],
        PYSPARK_PYTHON='/usr/bin/python3', CUDA_VISIBLE_DEVICES='', LD_LIBRARY_PATH=native,
        UCX_MODULE_DIR=runtime['spark.executorEnv.UCX_MODULE_DIR'])
    env['PATH'] = deployment['java_home'] + '/bin:' + env['PATH']
    service_state = services.start(run, config, host, 19077, 19080, 'suite')
    driver_state = app / 'driver-service.json'
    command = [str(Path(deployment['spark_home']) / 'bin/spark-submit'),
        '--properties-file', str(properties), '--jars',
        'local:' + str(engine) + ',local:' + config['tools'] +
        (',local:' + str(helper) if helper else ''), str(HERE / 'run_cases.py'),
        '--run', str(run), '--app', str(app), '--queries', ','.join(map(str, queries))]
    write(app / 'command.json', {'argv': command})
    try:
        process, receipt = services.launch(command, app / 'driver.log', env)
        write(driver_state, [receipt])
        deadline = time.monotonic() + 14400
        while process.poll() is None:
            if time.monotonic() > deadline:
                raise TimeoutError('Screening group exceeded its deadline')
            stat = os.statvfs(scratch)
            if stat.f_bavail * stat.f_frsize < 200 * 2**30:
                raise RuntimeError('NVMe free-space guard reached')
            time.sleep(3)
        result_path = app / 'result.json'
        if not result_path.exists():
            raise RuntimeError('Application exited without a result record')
        result = read(result_path)
        result['driver_exit_code'] = process.returncode
        if process.returncode and result['status'] == 'completed':
            raise RuntimeError('Driver reported completed outputs but exited unsuccessfully')
        if result['status'] not in ('completed', 'failed'):
            result.update(status='failed', error='Driver exited before recording its outcome',
                          error_kind='AbruptDriverExit')
        return run, result
    finally:
        services.stop(driver_state)
        services.stop(service_state)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True)
    args = parser.parse_args()
    work = Path(args.work).resolve()
    baseline = read(work / 'baseline/summary.json')
    assert baseline['status'] == 'completed' and len(baseline['samples']) == 44
    assert all(s['validation']['status'] == 'passed' for s in baseline['samples'])
    output = work / 'screen/state.json'
    if output.exists():
        raise RuntimeError('Screen state exists; inspect it before resuming')
    state = {'status': 'running', 'score_eligible': False, 'groups': [], 'rejected': [],
        'source_revision': subprocess.check_output(['git', '-C', str(SUITE), 'rev-parse', 'HEAD'],
                                                  text=True).strip(),
        'instance_policy': 'Retain until explicit user release',
        'screening_limit': 'Timing candidates require isolated confirmation before selection'}
    write(output, state)
    write(work / 'screen/candidates.json', [vars(c) for c in catalog()])
    write(work / 'screen/controller.json', {'pid': os.getpid(),
        'start_time': services.start_time(os.getpid()), 'owner_uid': os.getuid()})
    try:
        oracle_command = [str(work / 'spark-venv/bin/python'), str(HERE / 'toy_oracle.py'),
                          '--output', str(work / 'screen/toy-oracle.json')]
        env = dict(os.environ, JAVA_HOME=str(work / 'jdk17'))
        with (work / 'screen/toy-oracle.log').open('w') as log:
            subprocess.run(oracle_command, env=env, stdout=log, stderr=subprocess.STDOUT,
                           check=True, timeout=900)
        assert read(work / 'screen/toy-oracle.json')['status'] == 'passed'
        phases = [('sql', [targeted(q) for q in ORDER if targeted(q)], {}),
                  ('sql-extra', extra_cases(), {}),
                  ('split512', [c for c in catalog() if c.name == 'scan_split_512m'], {}),
                  ('batch512', [c for c in catalog() if c.name == 'batch_512m'],
                   {'spark.rapids.sql.batchSizeBytes': '536870912',
                    'spark.rapids.sql.reader.batchSizeBytes': '536870912'})]
        for phase, pending, overrides in phases:
            index = 0
            while pending:
                state.update(phase=phase, pending=[{'query': c.query, 'variant': c.name} for c in pending])
                write(output, state)
                run, result = launch_group(work, phase, index, pending, overrides)
                state['groups'].append({'phase': phase, 'run': str(run), 'status': result['status'],
                    'samples': [{key: s[key] for key in ['query', 'variant', 'iteration',
                        'sql_seconds', 'cleanup_seconds', 'validation']} for s in result['samples']],
                    'error': result.get('error'), 'error_kind': result.get('error_kind')})
                completed = {c.query for c in pending if sum(
                    s['query'] == c.query and s['validation']['status'] == 'passed'
                    for s in result['samples']) == 2}
                pending = [c for c in pending if c.query not in completed]
                if result['status'] != 'completed':
                    active = result.get('active_query')
                    if active is None or active not in [c.query for c in pending]:
                        raise RuntimeError('Failure cannot be attributed to one candidate; stop screening')
                    rejected = next(c for c in pending if c.query == active)
                    state['rejected'].append({'query': active, 'variant': rejected.name,
                        'error': result.get('error'), 'kind': result.get('error_kind'), 'run': str(run)})
                    pending = [c for c in pending if c.query != active]
                elif pending:
                    raise AssertionError('Completed group lacks two validated samples per case')
                index += 1
                write(output, state)
        state.update(status='screened', pending=[])
    except BaseException as error:
        state.update(status='failed', error=repr(error))
        raise
    finally:
        write(output, state)


if __name__ == '__main__':
    main()
