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

"""Run the configured application lifecycle and require every requested sample to complete."""
import argparse
import os
from pathlib import Path
import subprocess
import time

from common import SUITE, read, write
import services


def run_cohort(run):
    run = Path(run)
    config, profiles = read(run / 'config.json'), read(run / 'profiles.json')
    queries = config['queries']
    deployment = config['deployment']
    master_host = os.environ['SCALEUP_HEAD']
    port, ui_port = int(os.environ['SCALEUP_MASTER_PORT']), int(os.environ['SCALEUP_MASTER_UI_PORT'])
    env = dict(os.environ, JAVA_HOME=deployment['java_home'], SPARK_HOME=deployment['spark_home'],
               PYSPARK_PYTHON=os.environ.get('PYSPARK_PYTHON', '/usr/bin/python3'),
               CUDA_VISIBLE_DEVICES='')
    env['PATH'] = deployment['java_home'] + '/bin:' + env.get('PATH', '')
    groups = [(f'q{q:02d}', [q], False) for q in queries] if config['application_mode'] == 'query' else [
        ('suite', queries, False)]
    if config['platform'] == 'nvl72' and config['validation'] and 11 in queries:
        groups.insert(0, ('q11-reference', [11], True))
    summary = {'status': 'running', 'applications': [], 'samples': [],
               'validation_enabled': config['validation'], 'queries': queries}
    write(run / 'summary.json', summary)
    try:
        for tag, group, reference in groups:
            app = run / 'applications' / tag
            app.mkdir(parents=True)
            profile = dict(profiles[str(group[0])])
            if reference:
                profile.update({'spark.rapids.sql.format.parquet.hybridScan.enabled': 'false',
                                'spark.rapids.sql.reader.chunked': 'true',
                                'spark.rapids.sql.reader.gpuDirectStorage.mode': 'OFF'})
            scratch = Path(deployment['scratch_root']) / run.name
            (scratch / tag / 'events').mkdir(parents=True, exist_ok=True)
            native = str(Path(deployment['ucx_home']) / 'lib')
            cuda = deployment.get('cuda_home', '/usr/local/cuda-13.1')
            target = 'sbsa-linux' if config['architecture'] == 'arm64' else 'x86_64-linux'
            native += ':' + cuda + '/lib64:' + cuda + '/targets/' + target + '/lib'
            if config['platform'] == 'nvl72':
                if config.get('deployment_experiment', {}).get('bundle_cufile', False):
                    native += ':' + str(scratch / 'cufile-runtime')
                else:
                    native += ':/usr/local/cuda-13.0/lib64:/usr/local/cuda-13.0/targets/sbsa-linux/lib'
            native += ':/usr/local/nvidia/lib:/usr/local/nvidia/lib64'
            classpath = config['engine'] + ':' + config['tools']
            profile.update({'spark.master': f'spark://{master_host}:{port}',
                'spark.cores.max': str(int(profile['spark.executor.instances']) * int(profile['spark.executor.cores'])),
                'spark.driver.host': master_host, 'spark.driver.bindAddress': master_host,
                'spark.ui.port': '0', 'spark.eventLog.enabled': 'true', 'spark.eventLog.compress': 'false',
                'spark.eventLog.dir': 'file://' + str(scratch / tag / 'events'),
                'spark.local.dir': str(scratch / tag / 'driver'),
                'spark.driver.extraClassPath': classpath, 'spark.executor.extraClassPath': classpath,
                'spark.executorEnv.PYSPARK_PYTHON': env['PYSPARK_PYTHON'],
                'spark.executorEnv.LD_LIBRARY_PATH': native,
                'spark.executorEnv.UCX_MODULE_DIR': str(Path(deployment['ucx_home']) / 'lib/ucx')})
            if config['platform'] == 'nvl72':
                profile['spark.executorEnv.CUFILE_ENV_PATH_JSON'] = str(scratch / 'cufile.json')
            for component in ('driver', 'executor'):
                profile[f'spark.{component}.extraJavaOptions'] = '-Djava.io.tmpdir=' + str(scratch / 'java-tmp')
            (scratch / 'java-tmp').mkdir(exist_ok=True)
            write(app / 'profile.json', profile)
            properties = app / 'spark-defaults.conf'
            properties.write_text(''.join(f'{k}={v}\n' for k, v in sorted(profile.items())))
            state = None
            if config['application_mode'] == 'query':
                state = services.start(run, config, master_host, port, ui_port, tag)
            command = [str(Path(deployment['spark_home']) / 'bin/spark-submit'),
                       '--master', profile['spark.master'], '--deploy-mode', 'client',
                       '--properties-file', str(properties), '--jars',
                       'local:' + config['engine'] + ',local:' + config['tools'],
                       str(SUITE / 'scripts/run_sql.py'), '--run', str(run), '--app', str(app),
                       '--queries', ','.join(map(str, group))]
            if reference:
                command.append('--reference-only')
            write(app / 'command.json', {'argv': command, 'profile': str(properties)})
            app_env = dict(env, LD_LIBRARY_PATH=native,
                           UCX_MODULE_DIR=str(Path(deployment['ucx_home']) / 'lib/ucx'))
            try:
                process, receipt = services.launch(command, app / 'driver.log', app_env)
                driver_state = app / 'driver-service.json'
                write(driver_state, [receipt])
                try:
                    code = process.wait(timeout=1800 if len(group) > 1 else 800)
                finally:
                    services.stop(driver_state)
                    process.wait(timeout=10)
                result = read(app / 'result.json')
                if code != 0 or result['status'] != 'completed':
                    raise RuntimeError('Application failed: ' + tag)
                if len(result['samples']) != len(group) * (1 if reference else config['iterations']):
                    raise AssertionError('Application sample count differs')
                log_text = (app / 'driver.log').read_text(errors='replace')
                if 'UCX shuffle reclaim failed on executor' in log_text:
                    raise RuntimeError('An executor reported a failed UCX reclaim')
                summary['applications'].append({'name': tag, 'id': result['application_id'],
                                                'reference_only': reference})
                if not reference:
                    summary['samples'].extend(result['samples'])
                write(run / 'summary.json', summary)
            finally:
                if state:
                    services.stop(state)
        if len(summary['samples']) != len(queries) * config['iterations']:
            raise AssertionError('Full requested sample count differs')
        if config['validation'] and any(s['validation']['status'] != 'passed' for s in summary['samples']):
            raise AssertionError('A requested sample was not validated')
        summary['status'] = 'completed'
        summary['score_eligible'] = (config['validation'] and config['iterations'] == 2 and
                                     not config['artifact_manifest']['source_dirty'] and
                                     not config['runtime_source_dirty'] and
                                     not config['diagnostic_metrics_enabled'] and
                                     config['runtime_source_revision'] ==
                                     config['artifact_manifest']['source_revision'] and
                                     set(queries) == set(range(1, 23)))
        summary['per_query'] = {}
        for query in queries:
            samples = [s for s in summary['samples'] if s['query'] == query]
            chosen = min(samples, key=lambda s: s['sql_seconds'])
            summary['per_query'][str(query)] = {'best_sql_seconds': chosen['sql_seconds'],
                'selected_cleanup_seconds': chosen['cleanup_seconds'],
                'selected_sql_plus_cleanup_seconds': chosen['sql_plus_cleanup_seconds'],
                'second_pass_sql_seconds': samples[1]['sql_seconds'] if len(samples) > 1 else None}
        summary['best_sql_sum_seconds'] = sum(s['best_sql_seconds'] for s in summary['per_query'].values())
        summary['selected_sql_plus_cleanup_sum_seconds'] = sum(s['selected_sql_plus_cleanup_seconds']
                                                               for s in summary['per_query'].values())
    except BaseException as error:
        summary.update(status='failed', error=repr(error), score_eligible=False)
        raise
    finally:
        write(run / 'summary.json', summary)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', required=True)
    run_cohort(parser.parse_args().run)
