#!/usr/bin/env bash
set -euo pipefail
readonly work=${1:?Owned absolute directory is required}
readonly here=$(cd "$(dirname "$0")" && pwd)
python3 - "$work" "$here" <<'PY'
import json
from pathlib import Path
import sys
work=Path(sys.argv[1]).resolve()
sys.path.insert(0,sys.argv[2])
sys.path.insert(0,str(Path(sys.argv[2]).parents[1]/'scripts'))
import b200_services as services
owner=json.loads((work/'evidence/owner.json').read_text())
assert owner['thread_id']=='01a21183-bc74-7a30-be37-7b3b0d57c292'
for state in sorted((work/'results').glob('*/*/applications/suite/driver-service.json')):
    services.stop(state)
for state in sorted((work/'results').glob('*/*/services/*/*.json')):
    if not state.name.endswith('.stopped.json'):
        services.stop(state)
PY
