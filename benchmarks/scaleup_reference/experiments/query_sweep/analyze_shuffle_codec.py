#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Evaluate the Q8 codec probe using the predeclared second-action ABBA gate."""
import argparse
import hashlib
import json
from pathlib import Path

from finalize_sol_reuse import normalize


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    args = parser.parse_args()
    rows = []
    for phase in ('K0', 'K1', 'K2', 'K3'):
        state = json.loads((args.work / f'critical-path/{phase}/state.json').read_text())
        assert state['status'] == 'completed' and state['pool_verified']
        group, = state['groups']
        assert len(group['samples']) == 2
        assert all(s['validation']['status'] == 'passed' for s in group['samples'])
        sample = group['samples'][1]
        assert sample['query'] == 8 and sample['iteration'] == 2
        app = Path(group['run']) / 'applications/suite'
        profile = json.loads((app / 'profile.json').read_text())
        expected_codec = 'lz4' if phase in ('K0', 'K3') else 'none'
        assert profile['spark.rapids.shuffle.compression.codec'] == expected_codec
        nodes = sample['plan_metrics']
        exchanges = [n for n in nodes if n['node'] == 'GpuColumnarExchange']
        rows.append({'phase': phase, 'codec': expected_codec,
                     'sql_seconds': sample['sql_seconds'],
                     'cleanup_seconds': sample['cleanup_seconds'],
                     'sql_plus_cleanup_seconds': sample['sql_plus_cleanup_seconds'],
                     'sql_sha256': sample['sql_sha256'],
                     'plan_sha256': hashlib.sha256(normalize(
                         (app / 'q8-iteration2.executed.txt').read_text()).encode()).hexdigest(),
                     'reader_metrics': sample['reader_metrics'],
                     'shuffle_bytes_written': sum(n['metrics'].get('shuffleBytesWritten', 0) for n in exchanges),
                     'shuffle_records_written': sum(n['metrics'].get('shuffleRecordsWritten', 0) for n in exchanges),
                     'shuffle_partition_task_seconds': sum(n['metrics'].get('rapidsShufflePartitionTime', 0) for n in exchanges)/1e9,
                     'spill_metrics': {k: sum(n['metrics'].get(k, 0) for n in nodes)
                                      for k in sorted({k for n in nodes for k in n['metrics'] if 'spill' in k.lower()})}})
    for key in ('sql_sha256', 'plan_sha256', 'reader_metrics', 'shuffle_records_written'):
        assert all(row[key] == rows[0][key] for row in rows), key
    baseline = min(row['sql_plus_cleanup_seconds'] for row in rows if row['codec'] == 'lz4')
    candidate = max(row['sql_plus_cleanup_seconds'] for row in rows if row['codec'] == 'none')
    result = {'rows': rows, 'gate': 'worst none <= 0.97 * best lz4, second actions including cleanup',
              'accepted': candidate <= 0.97 * baseline,
              'conservative_reduction_fraction': 1-candidate/baseline,
              'classification': 'Focused Q8 experiment; not a full-suite score',
              'limits': ['First actions excluded; K0 first action overlapped trace archiving.',
                         'Task timing sums are not query wall time.',
                         'Byte counts exclude transport metadata and are not link throughput.']}
    output = args.work / 'critical-path/sol-codec-decision.json'
    output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
