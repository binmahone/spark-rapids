#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
"""Prepare dependencies without GPUs and run Paul's dedicated Scala GDS execution."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import xml.etree.ElementTree as ET

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, sha, write
import services


def command(*args):
    return subprocess.check_output(list(map(str, args)), text=True).strip()


def submit(args):
    assert command('id', '-un') == 'mahonem'
    assert not command('git', '-C', REPO, 'status', '--porcelain')
    assert not command('git', '-C', REPO, 'diff',
                       'b3e85674bad145871c4a23da0c932fb28e4d80c4', '--', 'tests')
    assert 'No reservations' in command('scontrol', 'show', 'reservation')
    assert command('sinfo', '-N', '-h', '-n', args.node, '-o', '%t') == 'idle'
    run = args.run.resolve()
    if args.phase == 'prepare':
        repository = REPO / '.build/maven-repository'
        if args.retry:
            config = read(run / 'config.json')
            assert config['thread_id'] == os.environ['CODEX_THREAD_ID']
            assert config['maven_repository'] == str(repository)
            assert read(run / 'prepare-status.json')['status'] == 'failed'
            previous = read(run / 'prepare-submission.json')['job_id']
            state = command('sacct', '-j', previous, '-X', '-n', '-o', 'State')
            assert state in ('FAILED', 'TIMEOUT') or state.startswith('CANCELLED'), state
            archive = run / 'attempts' / str(previous)
            archive.mkdir(parents=True, exist_ok=False)
            shutil.copyfile(run / 'config.json', archive / 'config.json')
            for path in run.glob('prepare*'):
                if path.is_file():
                    shutil.move(str(path), archive / path.name)
            config['source_revision'] = command('git', '-C', REPO, 'rev-parse', 'HEAD')
        else:
            run.mkdir(parents=True, exist_ok=False)
            assert not repository.exists(), 'Inspect the existing session repository before reuse'
            config = {'deployment': read(args.deployment),
                'thread_id': os.environ['CODEX_THREAD_ID'],
                'source_revision': command('git', '-C', REPO, 'rev-parse', 'HEAD'),
                'paul_head': 'b3e85674bad145871c4a23da0c932fb28e4d80c4',
                'maven_repository': str(repository),
                'engine_sha256_before': sha(next((REPO / '.build/artifacts/arm64').glob('cudf-spark_*.jar'))),
                'author_tests_unchanged': True}
        write(run / 'config.json', config)
    else:
        assert read(run / 'prepare-status.json')['status'] == 'completed'
        config = read(run / 'config.json')
        revision = command('git', '-C', REPO, 'rev-parse', 'HEAD')
        assert not command('git', '-C', REPO, 'diff', config['source_revision'], revision,
                           '--', '.', ':(exclude)benchmarks/scaleup_reference/experiments/paul_gds')
        config['compiled_source_revision'] = config.get('compiled_source_revision', config['source_revision'])
        config['source_revision'] = revision
        write(run / 'config.json', config)
        assert not command('ssh', '-oBatchMode=yes', '-oConnectTimeout=8', args.node,
                           'nvidia-smi --query-compute-apps=pid,gpu_uuid --format=csv,noheader')
    deployment = read(run / 'config.json')['deployment']
    argv = ['sbatch', '--parsable', '--export=ALL', '--nodes=1', '--ntasks=1',
        '--cpus-per-task=16', '--gres=' + ('gpu:0' if args.phase == 'prepare' else 'gpu:b200:1'),
        '--mem=64G', '--time=' + ('01:00:00' if args.phase == 'prepare' else '00:15:00'),
        '--no-requeue', '--partition=' + deployment.get('slurm_partition', 'nvidia-part'),
        '--nodelist=' + args.node, '--job-name=paul-scala-gds-' + args.phase,
        '--comment=codex-thread-' + os.environ['CODEX_THREAD_ID'],
        '--output=' + str(run / (args.phase + '-slurm-%j.log')),
        str(HERE / 'scripts/scala_tests.slurm')]
    env = dict(os.environ, PAUL_SCALA_RUN=str(run), PAUL_SCALA_SUITE=str(HERE),
               PAUL_SCALA_PHASE=args.phase)
    job = int(subprocess.check_output(argv, env=env, text=True).strip().split(';')[0])
    write(run / (args.phase + '-submission.json'), {'job_id': job, 'argv': argv})
    print('SUBMITTED', args.phase, job, flush=True)


def node(args):
    run = args.run.resolve()
    config = read(run / 'config.json')
    assert command('git', '-C', REPO, 'rev-parse', 'HEAD') == config['source_revision']
    assert not command('git', '-C', REPO, 'status', '--porcelain')
    env = dict(os.environ)
    if args.phase == 'prepare':
        argv = ['bash', str(HERE / 'scripts/prepare_scala_tests.sh')]
    else:
        tmp = Path('/gds-tests') / run.name
        tmp.mkdir(mode=0o700, parents=True, exist_ok=False)
        mount = command('findmnt', '-T', tmp, '-o', 'TARGET,SOURCE,FSTYPE,OPTIONS')
        assert 'gpfs' in mount
        default_tmp = REPO / 'tests/target/spark353/tmp'
        if default_tmp.exists():
            assert default_tmp.is_dir() and not any(default_tmp.iterdir())
            default_tmp.rmdir()
        default_tmp.symlink_to(tmp, target_is_directory=True)
        settings = json.loads(re.sub(r'//[^\n]*', '', Path('/etc/cufile.json').read_text()))
        settings['properties']['allow_compat_mode'] = False
        settings['profile']['cufile_stats'] = 0
        logs = run / 'cufile-logs'
        logs.mkdir()
        settings['logging']['dir'] = str(logs)
        settings['logging']['level'] = 'INFO'
        write(run / 'cufile.json', settings)
        env.update(CUFILE_ALLOW_COMPAT_MODE='false', CUFILE_STATS_LEVEL='0',
                   CUFILE_LOGGING_LEVEL='INFO', CUFILE_ENV_PATH_JSON=str(run / 'cufile.json'),
                   LD_LIBRARY_PATH='/usr/local/cuda-13.1/targets/sbsa-linux/lib:/usr/local/cuda-13.0/targets/sbsa-linux/lib:' + env.get('LD_LIBRARY_PATH', ''))
        write(run / 'preflight.json', {'host': socket.gethostname(), 'mount': mount,
            'tmp_target': str(default_tmp.resolve()), 'cuda_visible_devices': env.get('CUDA_VISIBLE_DEVICES'),
            'environment': {k: v for k, v in env.items() if k.startswith('CUFILE_')}})
        argv = ['mvn', '-B', '-ntp', '-nsu', '--settings', str(HERE / 'maven-settings.xml'), 'test-compile',
            'scalatest:test@hybrid-scan-gds-test', '-pl', 'tests', '-Dbuildver=353',
            '-Dgds.required=true', '-Parm64', '-Dcuda.version=cuda13',
            '-Dcudf-spark-jni.version=26.12.0-20261122.042214-52',
            '-Dmaven.repo.local=' + config['maven_repository']]
    write(run / (args.phase + '-command.json'), {'argv': argv, 'cwd': str(REPO)})
    write(run / (args.phase + '-status.json'), {'status': 'running', 'host': socket.gethostname()})
    state = run / (args.phase + '-service.json')
    process = None
    def terminate(signum, frame):
        raise SystemExit('Slurm task interrupted')
    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    try:
        process, receipt = services.launch(['bash', '-c', 'cd "$1"; shift; exec "$@"',
                                            'scala-gds', str(REPO), *argv],
                                           run / (args.phase + '.log'), env)
        write(state, [receipt])
        code = process.wait(timeout=3300 if args.phase == 'prepare' else 700)
        assert code == 0, 'Maven exited ' + str(code)
        if args.phase == 'test':
            reports = REPO / 'tests/target/spark353/surefire-reports'
            suites = []
            for path in reports.glob('TEST-*.xml'):
                document = ET.parse(path).getroot()
                if document.get('name') == 'com.nvidia.spark.rapids.parquet.HybridScanGdsSuite':
                    suites.append((path, document))
            assert len(suites) == 1
            path, suite = suites[0]
            cases = list(suite.iter('testcase'))
            assert len(cases) == 14, 'The inherited native reader tests and dedicated GDS test must run'
            assert not any(c.find('failure') is not None or c.find('error') is not None for c in cases)
            required = [c for c in cases if c.get('name') == 'a local file is opened for cuFile']
            assert len(required) == 1 and required[0].find('skipped') is None
            (run / path.name).write_bytes(path.read_bytes())
            output = reports / 'scala-test-output.txt'
            if output.exists():
                shutil.copyfile(output, run / output.name)
            write(run / 'test-summary.json', {'suite': suite.attrib, 'cases': [
                {'name': c.get('name'), 'status': 'canceled' if c.find('skipped') is not None else 'passed'}
                for c in cases], 'required_gds_check_passed': True})
        assert sha(next((REPO / '.build/artifacts/arm64').glob('cudf-spark_*.jar'))) == config['engine_sha256_before']
        write(run / (args.phase + '-status.json'), {'status': 'completed'})
    except BaseException as error:
        write(run / (args.phase + '-status.json'), {'status': 'failed', 'error': repr(error)})
        raise
    finally:
        if state.exists():
            services.stop(state)
        if process is not None:
            process.wait(timeout=20)
        if args.phase == 'test' and default_tmp.is_symlink() and default_tmp.resolve() == tmp:
            default_tmp.unlink()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('submit', 'node'))
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--phase', choices=('prepare', 'test'), required=True)
    parser.add_argument('--deployment', type=Path)
    parser.add_argument('--node', default='presto-gb200-gcn-04')
    parser.add_argument('--retry', action='store_true')
    arguments = parser.parse_args()
    submit(arguments) if arguments.action == 'submit' else node(arguments)
