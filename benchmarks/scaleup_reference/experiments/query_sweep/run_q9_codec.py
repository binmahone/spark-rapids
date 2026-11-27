#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Compare LZ4 and uncompressed shuffle for Q9 on the same statistics-reuse engine."""
import argparse
import json
from dataclasses import replace
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write
from critical_path_variants import controls, cases
from run_screen import launch_group
from variants import ORDER
import services


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--thread-id', required=True)
    parser.add_argument('--phase', choices=('C0', 'N1', 'N2', 'C3', 'full-N1', 'full-C2'), required=True)
    args = parser.parse_args()
    work = args.work
    owner = read(work / 'evidence/owner.json')
    assert owner['thread_id'] == args.thread_id
    assert (work / 'join-stats-build/build.exit').read_text().strip() == '0'
    assert not subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader'], text=True).strip()
    assert Path('/proc/sys/vm/watermark_scale_factor').read_text().strip() == '10'
    assert int(Path('/sys/kernel/mm/lru_gen/enabled').read_text().strip(), 0) == 7
    directory = work / 'q9-codec' / args.phase
    directory.mkdir(parents=True, exist_ok=False)
    write(directory / 'controller.json', {'pid': os.getpid(), 'owner_uid': os.getuid(),
                                         'start_time': services.start_time(os.getpid())})
    queries = list(ORDER) if args.phase.startswith('full-') else [9]
    assert queries and len(set(queries)) == len(queries) and set(queries) <= set(ORDER)
    selected = controls()
    for q in queries:
        selected[q] = replace(selected[q], overrides={**selected[q].overrides,
            'spark.rapids.sql.join.hashTable.reuse': 'false'})
    if 7 in queries:
        q7 = cases()[0]
        selected[7] = replace(q7, overrides={**selected[7].overrides, **q7.overrides})
    candidate = 'N' in args.phase
    codec = 'none' if candidate else 'lz4'
    engine = work / 'join-stats-engine/.build/artifacts/amd64'
    state = {'status': 'running', 'owner': owner, 'candidate': candidate, 'codec': codec, 'queries': queries,
             'source_revision': subprocess.check_output(['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip(),
             'start_unix_ns': time.time_ns(), 'observers': [],
             'classification': 'LZ4 versus uncompressed shuffle; identical engine, SQL and 32 native read threads',
             'gate': 'Worst candidate second-action SQL+cleanup must beat best control by at least 3%; full suite required for total score'}
    write(directory / 'state.json', state)
    def interrupt(signum, frame):
        raise InterruptedError('Signal ' + str(signum))
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGALRM):
        signal.signal(sig, interrupt)
    signal.alarm(3600)
    children = []
    try:
        commands = {
            'host': [str(HERE.parent / 'runtime_prefilter/monitor_host.py'), '--work', str(work), '--output', str(directory / 'host.jsonl')],
            'vm': [str(HERE / 'monitor_vm.py'), '--work', str(work), '--thread-id', args.thread_id,
                   '--controller', str(directory / 'controller.json'), '--output', str(directory / 'vm.jsonl')],
            'pool': [str(HERE / 'monitor_read_pool.py'), '--work', str(work), '--output', str(directory / 'pool.jsonl')]}
        for name, command in commands.items():
            log = (directory / (name + '.log')).open('x')
            child = subprocess.Popen([sys.executable, *command], stdout=log, stderr=subprocess.STDOUT)
            record = {'name': name, 'pid': child.pid, 'start_time': services.start_time(child.pid)}
            children.append((child, log, record))
            state['observers'].append(record)
        write(directory / 'state.json', state)
        run, result = launch_group(work, 'q9-codec-' + args.phase, 0,
            [selected[q] for q in queries], {'spark.rapids.sql.reader.kvikio.numThreads': '32',
                                           'spark.rapids.sql.metrics.level': 'DEBUG',
                                           'spark.rapids.shuffle.compression.codec': codec},
            runtime_hash_reuse_queries=[9], experimental_engine=engine)
        state.update(run=str(run), result=result)
        assert result['status'] == 'completed' and result['driver_exit_code'] == 0
        assert len(result['samples']) == 2 * len(queries)
        assert all(s['validation']['status'] == 'passed' for s in result['samples'])
        pools = {}
        for line in (directory / 'pool.jsonl').open():
            for executor in json.loads(line)['executors']:
                pools.setdefault(str(executor['pid']), set()).update(executor['thread_names'])
        assert len(pools) == 8 and all(names == {'kvikio-' + str(i) for i in range(32)}
                                     for names in pools.values())
        state['native_threads'] = {pid: sorted(names) for pid, names in pools.items()}
        assert all(child.poll() is None for child, _, _ in children)
        evidence = []
        for sample in result['samples']:
            query, iteration = sample['query'], sample['iteration']
            metrics = read(run / 'applications/suite' / f'q{query}-iteration{iteration}.metrics.json')
            joins = [n['metrics'] for n in metrics if n['node'] == 'GpuDriverlessBroadcastHashJoin']
            counts = {key: sum(n.get(key, 0) for n in joins)
                      for key in ('buildStatsComputations', 'buildStatsReuses')}
            if query == 9:
                assert counts['buildStatsComputations'] > 0, counts
                assert counts['buildStatsReuses'] > counts['buildStatsComputations'], counts
            evidence.append({'query': query, 'iteration': iteration, **counts})
        for sample in result['samples']:
            assert sample['runtime_hash_reuse_proof']['physical_plan_unchanged']
            assert sum(n['metrics'].get('hashTableReuses', 0) for n in sample['plan_metrics']) > 0
        state['statistics_reuse_evidence'] = evidence
        state['status'] = 'completed'
    except BaseException as error:
        state.update(status='failed', error=repr(error))
        raise
    finally:
        signal.alarm(0)
        for child, log, record in children:
            if child.poll() is None:
                child.terminate()
            record['exit_code'] = child.wait(timeout=30)
            if record['exit_code'] != 0:
                state['status'] = 'failed'
            log.close()
        state['end_unix_ns'] = time.time_ns()
        write(directory / 'state.json', state)


if __name__ == '__main__':
    main()
