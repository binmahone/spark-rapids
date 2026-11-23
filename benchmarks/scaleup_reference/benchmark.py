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

"""Plan, validate, submit and inspect a fixed Scale-Up cuDF-Spark reference run."""
import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent / 'scripts'))
from common import SUITE, TABLES, expand, load_config, read, sha, write


def prepare(args):
    config = load_config(args.platform, args.deployment)
    if args.queries:
        config['queries'] = [int(q) for q in args.queries.split(',')]
    if args.no_validate:
        config['validation'] = False
    if args.iterations:
        config['iterations'] = args.iterations
    if args.diagnostics:
        config['spark_conf']['spark.rapids.sql.metrics.level'] = 'DEBUG'
    profiles = expand(config, config['queries'])
    arch = config['architecture']
    root = SUITE.parents[1]
    artifact_dir = root / '.build/artifacts' / arch
    engine = Path(args.engine).resolve() if args.engine else next(artifact_dir.glob('cudf-spark_*.jar'), None)
    if engine is None or not engine.is_file():
        raise ValueError('Build the engine or provide --engine')
    tools = Path(args.tools).resolve() if args.tools else engine.parent / 'benchmark-tools.jar'
    manifest_path = engine.parent / 'manifest.json'
    manifest = read(manifest_path)
    if manifest['architecture'] != arch:
        raise ValueError('Artifact architecture differs from platform')
    for path in (engine, tools):
        if path.name not in manifest['files'] or sha(path) != manifest['files'][path.name]['sha256']:
            raise ValueError('Artifact SHA-256 differs: ' + str(path))
    config.update(engine=str(engine), tools=str(tools), engine_sha256=sha(engine),
                  tools_sha256=sha(tools), artifact_manifest=manifest,
                  diagnostic_metrics_enabled=args.diagnostics,
                  runtime_source_revision=subprocess.check_output(
                      ['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip(),
                  runtime_source_dirty=bool(subprocess.check_output(
                      ['git', '-C', str(root), 'status', '--porcelain'], text=True).strip()))
    return config, profiles


def validate_target(config):
    deployment = config['deployment']
    for key in ('spark_home', 'java_home', 'ucx_home'):
        if not deployment[key] or not Path(deployment[key]).is_dir():
            raise ValueError('Provide an existing deployment.' + key)
    for table in TABLES:
        if not (Path(config['dataset']) / table).is_dir():
            raise ValueError('Dataset table is unavailable: ' + table)
    for path in (Path(deployment['spark_home']) / 'bin/spark-submit',
                 Path(deployment['java_home']) / 'bin/java',
                 Path(deployment['ucx_home']) / 'lib/libucs.so'):
        if not path.exists():
            raise ValueError('Runtime dependency is unavailable: ' + str(path))
    for path in (Path(deployment['spark_home']) / 'jars').glob('*.jar'):
        if any(name in path.name.lower() for name in ('rapids', 'cudf', 'gluten', 'flux')):
            raise ValueError('Unexpected engine dependency in the Spark installation: ' + str(path))
    if shutil.which('numactl') is None:
        raise ValueError('numactl is required')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('plan', 'validate', 'submit', 'status', 'collect', 'stop'))
    parser.add_argument('--platform', choices=('nvl72', 'aws48x'))
    parser.add_argument('--deployment', help='Machine paths only; does not alter the workload profile')
    parser.add_argument('--engine')
    parser.add_argument('--tools')
    parser.add_argument('--queries', help='Comma-separated query numbers; a subset is diagnostic only')
    parser.add_argument('--iterations', type=int, choices=(1, 2))
    parser.add_argument('--no-validate', action='store_true')
    parser.add_argument('--diagnostics', action='store_true')
    parser.add_argument('--run', help='New output directory for submit; existing directory for other actions')
    args = parser.parse_args()
    if args.action in ('status', 'collect', 'stop'):
        if not args.run:
            parser.error('--run is required')
        run = Path(args.run).resolve()
        owner = read(run / 'owner.json')
        if args.action == 'stop':
            if owner['uid'] != os.getuid():
                raise ValueError('Run belongs to another user')
            if owner['platform'] == 'nvl72':
                job = read(run / 'submission.json')['job_id']
                user = subprocess.check_output(['squeue', '-h', '-j', str(job), '-o', '%u'],
                                               text=True).strip()
                if user and user != owner['user']:
                    raise ValueError('Slurm job belongs to another user')
                if user:
                    subprocess.run(['scancel', str(job)], check=True)
            else:
                import services
                import signal
                receipt = read(run / 'submission.json')
                try:
                    if services.start_time(receipt['pid']) == receipt['start_time']:
                        os.killpg(receipt['pid'], signal.SIGTERM)
                except (ProcessLookupError, FileNotFoundError):
                    pass
                for state in (run / 'applications').glob('*/driver-service.json'):
                    services.stop(state)
                for state in (run / 'services').glob('*/*.json'):
                    if not state.name.endswith('.stopped.json'):
                        services.stop(state)
        if (run / 'summary.json').exists():
            print(__import__('json').dumps(read(run / 'summary.json'), indent=2))
        else:
            print(__import__('json').dumps(read(run / 'submission.json'), indent=2))
        return
    if not args.platform:
        parser.error('--platform is required')
    config, profiles = prepare(args)
    if args.action == 'plan':
        print(__import__('json').dumps({'config': config, 'profiles': profiles}, indent=2))
        return
    validate_target(config)
    if args.action == 'validate':
        print('Target paths, artifact hashes and configuration passed preflight; no GPU query ran.')
        return
    label = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8]
    run = Path(args.run).resolve() if args.run else SUITE / 'runs' / label
    run.mkdir(parents=True, exist_ok=False)
    write(run / 'config.json', config)
    write(run / 'profiles.json', profiles)
    write(run / 'owner.json', {'uid': os.getuid(), 'user': __import__('getpass').getuser(),
          'platform': args.platform, 'thread_id': os.environ.get('CODEX_THREAD_ID'),
          'created_utc': datetime.now(timezone.utc).isoformat(), 'run_id': run.name})
    env = dict(os.environ, SCALEUP_RUN=str(run), SCALEUP_SUITE=str(SUITE))
    if args.platform == 'nvl72':
        deployment = config['deployment']
        if not deployment.get('container_mounts'):
            raise ValueError('The NVL deployment file must list container_mounts explicitly')
        command = ['sbatch', '--parsable', '--export=ALL', '--job-name=scaleup-reference',
                   '--nodes=' + str(deployment['nodes']), '--ntasks-per-node=1',
                   '--cpus-per-task=144', '--gres=gpu:b200:4', '--mem=0', '--exclusive',
                   '--no-requeue', '--time=00:45:00', '--partition=' + deployment['slurm_partition'],
                   '--comment=codex-thread-' + os.environ.get('CODEX_THREAD_ID', 'manual'),
                   '--output=' + str(run / 'slurm-%j.log')]
        if deployment.get('node_list'):
            command.append('--nodelist=' + deployment['node_list'])
        command.append(str(SUITE / 'scripts/run_nvl.slurm'))
        response = subprocess.check_output(command, env=env, text=True).strip()
        write(run / 'submission.json', {'job_id': int(response.split(';')[0]), 'command': command})
        print('Submitted Slurm job ' + response + '; results: ' + str(run))
    else:
        env.update(SCALEUP_HEAD=socket.gethostname(), SCALEUP_MASTER_PORT='17744',
                   SCALEUP_MASTER_UI_PORT='18190')
        command = ['bash', str(SUITE / 'scripts/run_node.sh')]
        with (run / 'console.log').open('w') as log:
            process = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=True)
        import services
        write(run / 'submission.json', {'pid': process.pid, 'start_time': services.start_time(process.pid),
                                        'command': command})
        print('Started run ' + str(run) + '; controller PID ' + str(process.pid))


if __name__ == '__main__':
    main()
