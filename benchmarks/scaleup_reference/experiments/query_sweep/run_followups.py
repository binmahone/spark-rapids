#!/usr/bin/env python3
"""Run bounded follow-ups for residual bottlenecks observed in the broad screen."""
import argparse
import os
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from common import read, write
from run_screen import launch_group
import services
from variants import catalog, targeted

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--work', required=True)
args = parser.parse_args()
work = Path(args.work).resolve()
assert read(work / 'screen/state.json')['status'] == 'screened'
assert read(work / 'followups/toy-oracle.json')['status'] == 'passed'
output = work / 'followups/state.json'
if output.exists():
    raise RuntimeError('Follow-up state exists; inspect before resuming')
available = {(c.query, c.name): c for c in catalog(include_followups=True)}
cases = [targeted(3)] + [available[key] for key in [
    (3, 'ordinary_customer_no_reuse'), (3, 'ordinary_customer_reuse'),
    (5, 'prefilter_lineitem_order_keys'), (21, 'preserve_witness_broadcast_suppliers')]]
report = {'status': 'running', 'score_eligible': False, 'cases': [],
    'budget': 'Five cases, two actions each; no automatic iterative tuning',
    'intent': 'Separate transport/reuse costs; target a larger remaining fact exchange',
    'instance_policy': 'Retain until explicit user release'}
write(output, report)
write(work / 'followups/controller.json', {'pid': os.getpid(),
    'start_time': services.start_time(os.getpid()), 'owner_uid': os.getuid()})
try:
    for index, case in enumerate(cases):
        metrics = 'DEBUG' if case.query == 3 else 'MODERATE'
        run, result = launch_group(work, 'followup', index, [case],
                                  {'spark.rapids.sql.metrics.level': metrics})
        report['cases'].append({'query': case.query, 'variant': case.name, 'run': str(run),
            'status': result['status'], 'error': result.get('error'),
            'error_kind': result.get('error_kind'), 'samples': result['samples']})
        write(output, report)
    report['status'] = 'screened'
except BaseException as error:
    report.update(status='failed', error=repr(error))
    raise
finally:
    write(output, report)
