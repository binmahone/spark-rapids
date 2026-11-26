#!/usr/bin/env python3
"""Run the selected SQL rewrites as one fully validated AWS48x TPC-H suite."""
import argparse
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
import re
import socket
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, sha, write
from run_screen import launch_group
from variants import ORDER, Variant, catalog, original

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--work', required=True, help='Prepared host directory with deployment, artifacts and data')
parser.add_argument('--selection', default=str(HERE / 'selected-cases.json'))
parser.add_argument('--name', default=datetime.now(timezone(timedelta(hours=8))).strftime('selected-%y%m%d-%H%M%S'))
parser.add_argument('--verify-only', action='store_true', help='Check every SQL fingerprint without starting services')
args = parser.parse_args()
work = Path(args.work).resolve()
selection = read(args.selection)
if selection['platform'] != 'aws48x' or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]*', args.name):
    raise ValueError('Unsupported platform or run name')
if (selection['engine_sha256'] != 'c267dafd65ae85b558d8c8df03a94c0947cbe42b79a1e29ddeaa63bfcadb8cce'
        or selection['query_order'] != ORDER):
    raise AssertionError('Engine identity or query order differs from the tested suite')
if selection.get('common_configuration_sha256') and selection['common_configuration_sha256'] != sha(
        HERE.parents[1] / 'configs/aws48x.json'):
    raise AssertionError('Common configuration differs from the measured selection')
overrides = selection.get('runtime_overrides_by_query', {})
allowed = {'spark.sql.shuffle.partitions', 'spark.rapids.sql.batchSizeBytes',
           'spark.sql.files.maxPartitionBytes'}
if any(q not in {str(q) for q in ORDER} or set(values) - allowed
       or any(not isinstance(value, str) for value in values.values())
       for q, values in overrides.items()):
    raise ValueError('Selection contains unsupported runtime overrides')
available = {(c.query, c.name): c for c in catalog(include_followups=True)}
selected = {}
for record in selection['candidates']:
    case = available[(record['query'], record['variant'])]
    if case.query in selected or case.overrides:
        raise ValueError('Selected SQL rewrites must be unique and keep the common configuration')
    selected[case.query] = case
cases = [selected.get(q, Variant(q, 'baseline', original(q), 'Unchanged reference SQL')) for q in ORDER]
cases = [replace(case, overrides=overrides.get(str(case.query), {})) for case in cases]
for case in cases:
    if hashlib.sha256(case.sql.encode()).hexdigest() != selection['sql_sha256_by_query'][str(case.query)]:
        raise AssertionError('Selected SQL differs from the tested query: ' + str(case.query))
if args.verify_only:
    print('Verified all 22 SQL fingerprints and runtime override contracts; no services were started')
    raise SystemExit(0)
host = socket.gethostbyname(socket.gethostname())
for port in (19077, 19080):
    with socket.socket() as connection:
        connection.settimeout(1)
        if connection.connect_ex((host, port)) == 0:
            raise RuntimeError('Another reference run is active on port ' + str(port))
run, result = launch_group(work, args.name, 0, cases, {})
if result['status'] != 'completed' or result['driver_exit_code'] or len(result['samples']) != 44:
    raise RuntimeError('Selected suite failed: ' + str(result.get('error')))
assert all(s['validation']['status'] == 'passed' for s in result['samples'])
summary = {'status': 'completed', 'run': str(run), 'score_eligible': False,
    'classification': 'Experimental SQL mechanisms; not an official Daily Trend main score',
    'best_sql_sum_seconds': sum(min(s['sql_seconds'] for s in result['samples'] if s['query'] == q)
                              for q in ORDER),
    'best_total_sum_seconds': sum(min(s['sql_plus_cleanup_seconds'] for s in result['samples']
                                     if s['query'] == q) for q in ORDER),
    'validated_actions': 44, 'instance_policy': 'Retain until explicit user release'}
write(run / 'summary.json', summary)
print(summary)
