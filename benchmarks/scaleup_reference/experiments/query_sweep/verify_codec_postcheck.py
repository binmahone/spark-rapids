#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Verify retained complete applications after the known Q9-only postcheck scope error."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write
from variants import ORDER
import services


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verified_state(directory):
    state = read(directory / 'state.json')
    if state['status'] == 'completed':
        return state
    proof = read(directory / 'postcheck-verification.json')
    assert state['status'] == 'failed' and state['error'] == "KeyError('runtime_hash_reuse_proof')"
    assert proof['status'] == 'passed'
    assert proof['original_state_sha256'] == digest(directory / 'state.json')
    for path, expected in proof['evidence_sha256'].items():
        assert digest(Path(path)) == expected, path
    return {**state, 'status': 'completed', 'postcheck_verification': proof}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', type=Path, required=True)
    p.add_argument('--phase', required=True)
    p.add_argument('--thread-id', required=True)
    a = p.parse_args()
    directory = a.work / 'q9-codec' / a.phase
    state = read(directory / 'state.json')
    assert state['owner']['thread_id'] == a.thread_id
    assert state['status'] == 'failed' and state['error'] == "KeyError('runtime_hash_reuse_proof')"
    assert state['result']['status'] == 'completed' and state['result']['driver_exit_code'] == 0
    assert len(state['native_threads']) == 8
    expected_threads = {'kvikio-' + str(i) for i in range(32)}
    assert all(set(v) == expected_threads for v in state['native_threads'].values())
    assert all(r['exit_code'] == 0 for r in state['observers'])
    for record in [read(directory / 'controller.json'), *state['observers']]:
        stat = Path('/proc') / str(record['pid']) / 'stat'
        assert not stat.exists() or services.start_time(record['pid']) != record['start_time']
    run = Path(state['run'])
    app = run / 'applications/suite'
    result = read(app / 'result.json')
    assert result['status'] == 'completed' and result['samples'] == state['result']['samples']
    samples = result['samples']
    assert [(s['query'], s['iteration']) for s in samples] == [(q, i) for q in ORDER for i in (1, 2)]
    evidence = [run / 'config.json', run / 'profiles.json', app / 'result.json',
                app / 'profile.json', app / 'command.json', directory / 'config-parity.json']
    assert read(directory / 'config-parity.json')['effective_profiles_equal_after_codec_and_scratch_normalization']
    for sample in samples:
        assert sample['validation']['status'] == 'passed'
        token = f'q{sample["query"]}-iteration{sample["iteration"]}'
        assert digest(app / (token + '.sql')) == sample['sql_sha256']
        cleanup = app / (token + '.cleanup.json')
        assert len(read(cleanup)) == 8
        metrics = read(app / (token + '.metrics.json'))
        if sample['query'] == 9:
            proof = sample['runtime_hash_reuse_proof']
            assert proof['physical_plan_unchanged'] and proof['restored_value'] == 'false'
            assert sum(n['metrics'].get('hashTableReuses', 0) for n in metrics) > 0
            joins = [n['metrics'] for n in metrics if n['node'] == 'GpuDriverlessBroadcastHashJoin']
            computed = sum(n.get('buildStatsComputations', 0) for n in joins)
            reused = sum(n.get('buildStatsReuses', 0) for n in joins)
            assert reused > computed > 0
        else:
            assert 'runtime_hash_reuse_proof' not in sample
        evidence.extend([cleanup, app / (token + '.metrics.json'), app / (token + '.executed.txt'),
                         app / (token + '.sql')])
    for path in run.rglob('*.json'):
        if path.name == 'driver-service.json' or ('services' in path.parts and not path.name.endswith('.stopped.json')):
            for record in read(path):
                assert not services.group_members(record), path
    target = directory / 'postcheck-verification.json'
    assert not target.exists()
    proof = {'status': 'passed', 'validated_actions': len(samples),
             'original_controller_status': state['status'], 'original_error': state['error'],
             'original_state_sha256': digest(directory / 'state.json'),
             'evidence_sha256': {str(path): digest(path) for path in evidence},
             'verifier_revision': subprocess.check_output(['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip(),
             'scope': 'Offline verification of completed query actions, cleanup, native pools, '
                      'Q9-only runtime reuse, source artifacts and process exit. Original failed state is preserved.'}
    write(target, proof)
    print(json.dumps({k: v for k, v in proof.items() if k != 'evidence_sha256'}))


if __name__ == '__main__':
    main()
