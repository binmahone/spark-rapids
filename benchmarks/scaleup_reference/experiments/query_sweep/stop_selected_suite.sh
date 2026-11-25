#!/usr/bin/env bash
set -euo pipefail
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly work=${1:?Prepared host directory is required}
readonly name=${2:?Run name is required}
[[ "$name" =~ ^[A-Za-z0-9][A-Za-z0-9_-]*$ ]] || exit 2
python3 - "$here" "$work" "$name" <<'PY'
from pathlib import Path
import sys
here, work = Path(sys.argv[1]), Path(sys.argv[2]).resolve()
sys.path.insert(0, str(here.parents[1] / 'scripts'))
import services
run = work / 'screen' / (sys.argv[3] + '-0')
states = [run / 'applications/suite/driver-service.json']
states += [p for p in run.glob('services/*/*.json') if not p.name.endswith('.stopped.json')]
for state in states:
    if state.exists() and not Path(str(state) + '.stopped.json').exists():
        services.stop(state)
print('Recorded services for this selected run stopped; EC2 instance retained')
PY
