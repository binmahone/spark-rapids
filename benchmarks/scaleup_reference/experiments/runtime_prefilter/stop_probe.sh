#!/usr/bin/env bash
set -euo pipefail
readonly work=${1:?Absolute owned work directory is required}
readonly suite=$(cd "$(dirname "$0")/../.." && pwd)
python3 - "$work" "$suite" <<'PY'
from pathlib import Path
import sys
sys.path.insert(0, str(Path(sys.argv[2]) / 'scripts'))
import services
work = Path(sys.argv[1]).resolve()
for state in sorted(work.glob('results*/*/applications/suite/driver-service.json')):
    services.stop(state)
for state in sorted(work.glob('results*/*/services/*/*.json')):
    if not state.name.endswith('.stopped.json'):
        services.stop(state)
PY
