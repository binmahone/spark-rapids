#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""Select unexecuted controls while retaining the original immutable timed runner."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[4]


def load(harness):
    path = harness / 'benchmarks/scaleup_reference/experiments/deployment/experiment.py'
    spec = importlib.util.spec_from_file_location('original_deployment_experiment', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'node'))
    parser.add_argument('--run', required=True, type=Path)
    parser.add_argument('--harness-checkout', type=Path)
    parser.add_argument('--reference-checkout', type=Path)
    parser.add_argument('--deployment', type=Path)
    parser.add_argument('--select', default='07-auto-tls,08-unbound,09-cpu-only,10-baseline')
    parser.add_argument('--queries', default='9,3,6')
    parser.add_argument('--predecessor', type=int, default=25472)
    args = parser.parse_args()
    root = args.run.resolve()
    if args.action == 'node':
        plan = json.loads((root / 'plan.json').read_text())
        original = load(Path(plan['harness_checkout']))
        live = subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid,process_name',
                                        '--format=csv,noheader'], text=True).strip()
        import socket
        original.write(root / 'preflight' / (socket.gethostname() + '.json'),
                       {'existing_compute_processes': live})
        if live:
            raise RuntimeError('Allocated GPUs have unexpected compute processes: ' + live)
        original.node(args)
        return
    original = load(args.harness_checkout.resolve())
    original.prepare(SimpleNamespace(reference_checkout=args.reference_checkout,
        run=root, deployment=args.deployment, queries=args.queries, submit=False))
    plan = original.read(root / 'plan.json')
    selected = args.select.split(',')
    by_label = {case['label']: case for case in plan['cases']}
    plan['cases'] = [by_label[label] for label in selected]
    plan.update(harness_checkout=str(args.harness_checkout.resolve()), predecessor_job=args.predecessor,
        selection_source_revision=subprocess.check_output(
            ['git', '-C', str(REPO), 'rev-parse', 'HEAD'], text=True).strip(),
        selection='Explicit unexecuted cases; the original benchmark runner remains unchanged')
    for path in [Path(__file__), HERE / 'scripts/run_node.sh']:
        plan['runtime_input_sha256'][str(path)] = original.sha(path)
    original.write(root / 'plan.json', plan)
    d = original.read(root / 'config.json')['deployment']
    command = ['sbatch', '--parsable', '--export=ALL', '--job-name=nvl-deployment-resume',
        '--nodes=' + str(plan['nodes']), '--ntasks-per-node=1', '--cpus-per-task=144',
        '--gres=gpu:b200:4', '--mem=0', '--exclusive', '--no-requeue', '--time=00:25:00',
        '--partition=' + d['slurm_partition'], '--comment=codex-thread-' + str(plan['thread_id']),
        '--output=' + str(root / 'slurm-%j.log'), '--nodelist=' + d['node_list'],
        str(original.SUITE / 'scripts/run_nvl.slurm')]
    env = dict(os.environ, SCALEUP_RUN=str(root), SCALEUP_SUITE=str(HERE))
    response = subprocess.check_output(command, env=env, text=True).strip()
    original.write(root / 'submission.json', {'job_id': int(response.split(';')[0]), 'argv': command})
    print('Submitted', response, 'results:', root, flush=True)


if __name__ == '__main__':
    main()
