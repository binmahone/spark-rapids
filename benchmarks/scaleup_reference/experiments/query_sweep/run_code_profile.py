#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Profile frozen SQL with CPU and wall stacks, preserving unprofiled controls."""
import argparse
from dataclasses import replace
import json
import os
import re
from pathlib import Path
import signal
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, sha, write
from critical_path_variants import controls
from run_screen import launch_group
import services


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', type=Path, required=True)
    p.add_argument('--thread-id', required=True)
    p.add_argument('--label', required=True)
    p.add_argument('--mode', choices=('control', 'stacks'), required=True)
    p.add_argument('--queries', default='8,18,5')
    p.add_argument('--nvcomp-policy', choices=('STABLE', 'OFF'))
    p.add_argument('--concat-implementation', choices=('control', 'candidate'))
    a = p.parse_args()
    assert a.label.replace('-', '').replace('_', '').isalnum()
    owner = read(a.work / 'evidence/owner.json')
    assert owner['thread_id'] == a.thread_id
    assert (a.work / 'evidence/prepare.exit').read_text().strip() == '0'
    assert not subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader'], text=True).strip()
    assert Path('/proc/sys/vm/watermark_scale_factor').read_text().strip() == '10'
    assert int(Path('/sys/kernel/mm/lru_gen/enabled').read_text().strip(), 0) == 7
    directory = a.work / 'code-profile' / a.label
    directory.mkdir(parents=True, exist_ok=False)
    scratch = Path(read(a.work / 'evidence/deployment.json')['scratch_root']) / 'code-profiles' / a.label
    scratch.mkdir(parents=True, exist_ok=False)
    assert subprocess.check_output(['findmnt', '-no', 'TARGET', '-T', str(scratch)], text=True).strip() != '/'
    options = None
    profiler = None
    if a.mode == 'stacks':
        profiler = read(a.work / 'profilers/manifest.json')
        assert sha(Path(profiler['library'])) == profiler['library_sha256']
        options = (f"-agentpath:{profiler['library']}=start,event=cpu,interval=10ms,wall=20ms,"
                   f"cstack=dwarf,jstackdepth=256,timeout=900,file={scratch}/profile-%p.jfr,log=stderr")
    concat = None
    if a.concat_implementation:
        native = a.work / 'native-concat'
        assert (native / 'build.exit').read_text().strip() == '0'
        subprocess.run(['sha256sum', '--check', str(native / 'agent.sha256')], check=True)
        concat = {'implementation': a.concat_implementation,
                  'agent_sha256': sha(native / 'concat-offset-probe.jar'),
                  'library_sha256': sha(native / 'lib/libconcat_offsets_probe.so'),
                  'sdk': read(native / 'sdk-manifest.json')}
        agent = (f'-Dai.rapids.cudf.lib-native-dir={native}/lib '
                 f'-javaagent:{native}/concat-offset-probe.jar={a.concat_implementation},'
                 f'{native}/lib/libconcat_offsets_probe.so')
        options = (options + ' ' if options else '') + agent
    selected = controls()
    queries = [int(q) for q in a.queries.split(',')]
    assert queries and len(queries) == len(set(queries)) and set(queries) <= {5, 8, 18, 20, 10}
    selected = [replace(selected[q], overrides={**selected[q].overrides,
                'spark.rapids.sql.join.hashTable.reuse': 'false'}) for q in queries]
    state = {'status': 'running', 'owner': owner, 'mode': a.mode, 'profiler': profiler,
             'concat_probe': concat,
             'queries': queries, 'nvcomp_policy': a.nvcomp_policy,
             'scratch': str(scratch), 'started_unix_ns': time.time_ns(),
             'classification': 'Code-path diagnosis; profiler times are not benchmark scores',
             'source_revision': subprocess.check_output(['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip()}
    write(directory / 'controller.json', {'pid': os.getpid(), 'start_time': services.start_time(os.getpid()), 'owner_uid': os.getuid()})
    write(directory / 'state.json', state)
    def interrupt(signum, frame):
        raise InterruptedError('Signal ' + str(signum))
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGALRM):
        signal.signal(sig, interrupt)
    signal.alarm(1200)
    children = []
    try:
        for name, script, extra in [
            ('host', HERE.parent / 'runtime_prefilter/monitor_host.py', []),
            ('vm', HERE / 'monitor_vm.py', ['--thread-id', a.thread_id, '--controller', str(directory / 'controller.json')]),
            ('pool', HERE / 'monitor_read_pool.py', [])]:
            log = (directory / (name + '.log')).open('x')
            process = subprocess.Popen([sys.executable, str(script), '--work', str(a.work),
                '--output', str(directory / (name + '.jsonl')), *extra], stdout=log, stderr=subprocess.STDOUT)
            children.append((process, log))
        decoder = ({'spark.executorEnv.LIBCUDF_NVCOMP_POLICY': a.nvcomp_policy}
                   if a.nvcomp_policy else {})
        run, result = launch_group(a.work, 'code-profile-' + a.label, 0, selected, {
            **decoder,
            'spark.rapids.sql.reader.kvikio.numThreads': '32',
            'spark.rapids.sql.metrics.level': 'DEBUG',
            'spark.rapids.shuffle.compression.codec': 'none',
            'spark.task.resource.gpu.amount': '0.25',
            'spark.rapids.sql.format.parquet.hybridScan.maxH2DPackSize': '32m',
            'spark.rapids.shuffle.ucx.bounceBuffers.size': '4m'},
            experimental_engine=a.work / 'artifacts/decimal-null-copy',
            diagnostic_jvm_options=options)
        state.update(run=str(run), result=result)
        assert result['status'] == 'completed' and result['driver_exit_code'] == 0
        assert len(result['samples']) == 2 * len(queries)
        assert all(s['validation']['status'] == 'passed' for s in result['samples'])
        if concat:
            logs = Path(read(a.work / 'evidence/deployment.json')['scratch_root']) / run.name / 'suite'
            proof = []
            for stderr in logs.glob('worker-*/work/*/*/stderr'):
                match = re.search(r'CONCAT_OFFSET_PROBE_COUNTS mode=(\w+) calls=(\d+) '
                    r'candidate_string_columns=(\d+) input_tables=(\d+)', stderr.read_text())
                assert match, 'Missing native execution counters: ' + str(stderr)
                assert match[1] == a.concat_implementation and int(match[2]) > 0
                proof.append({'stderr': str(stderr), 'calls': int(match[2]),
                    'candidate_string_columns': int(match[3]), 'input_tables': int(match[4])})
            assert len(proof) == 8
            if a.concat_implementation == 'candidate' and 5 in queries:
                assert all(v['candidate_string_columns'] > 0 for v in proof)
            concat['runtime_proof'] = proof
        assert all(child.poll() is None for child, _ in children), 'An observer exited during the workload'
        pools = {}
        decoder_policies = {}
        for line in (directory / 'pool.jsonl').read_text().splitlines():
            for executor in json.loads(line)['executors']:
                pools.setdefault(str(executor['pid']), set()).update(executor['thread_names'])
                decoder_policies.setdefault(str(executor['pid']), set()).add(
                    executor.get('nvcomp_policy'))
        assert len(pools) == 8 and all(names == {'kvikio-' + str(i) for i in range(32)}
                                     for names in pools.values())
        state['native_threads'] = {pid: sorted(names) for pid, names in pools.items()}
        if a.nvcomp_policy:
            assert len(decoder_policies) == 8
            assert all(values == {a.nvcomp_policy} for values in decoder_policies.values())
        state['observed_nvcomp_policies'] = {pid: list(v) for pid, v in decoder_policies.items()}
        profiles = sorted(scratch.glob('*.jfr'))
        if a.mode == 'stacks':
            assert len(profiles) == 9 and all(p.stat().st_size > 1024 for p in profiles)
        state['profiles'] = [{'path': str(p), 'bytes': p.stat().st_size, 'sha256': sha(p)} for p in profiles]
        state['status'] = 'completed'
    except BaseException as error:
        state.update(status='failed', error=repr(error))
        raise
    finally:
        signal.alarm(0)
        for child, log in children:
            if child.poll() is None:
                child.terminate()
            if child.wait(timeout=30) != 0:
                state['status'] = 'failed'
            log.close()
        state['finished_unix_ns'] = time.time_ns()
        write(directory / 'state.json', state)


if __name__ == '__main__':
    main()
