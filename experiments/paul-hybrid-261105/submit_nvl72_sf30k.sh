#!/usr/bin/env bash
set -euo pipefail
readonly query=${1:?Specify one query number}
readonly smoke_job=${2:?Specify the successful build and smoke Slurm job}
readonly nodes=${3:-5}
readonly scan_profile=${4:-baseline}
case "$scan_profile" in
  baseline|split512m|split1g|split2g) ;;
  *) echo 'Supported scan profiles: baseline, split512m, split1g, split2g' >&2; exit 2 ;;
esac
case "$nodes" in
  5) readonly nodelist='presto-gb200-gcn-[06-07,12-14]' ;;
  8) readonly nodelist='presto-gb200-gcn-[01-02,04,06-07,12-14]' ;;
  *) echo 'Supported node counts: 5 or 8' >&2; exit 2 ;;
esac
readonly harness_revision=$(git -C /home/nvidia/mahone_gluten/wild_spark_rapids/261105_paul_hybrid_multithreaded/src/cudf-spark rev-parse HEAD)
[[ "$query" =~ ^([1-9]|1[0-9]|2[0-2])(,([1-9]|1[0-9]|2[0-2]))*$ ]]
[[ "$smoke_job" =~ ^[0-9]+$ ]]
ssh -F /home/nvidia/mahone_gluten/cluster_ssh_config \
  -o ConnectTimeout=10 -o ServerAliveInterval=15 -o ServerAliveCountMax=2 \
  presto-cp-02-via-mac bash -s -- "$query" "$smoke_job" "$harness_revision" "$nodes" "$nodelist" "$scan_profile" <<'SH'
set -euo pipefail
readonly query=$1
readonly smoke_job=$2
readonly harness_revision=$3
readonly nodes=$4
readonly nodelist=$5
readonly scan_profile=$6
readonly own=/home/mahonem/paul_hybrid_multithreaded_261105
export SLURM_CONF=/cm/shared/apps/slurm/etc/slurm/slurm.conf
readonly slurm=/cm/local/apps/slurm/current/bin
test "$("$slurm/sacct" -j "$smoke_job" -X -n -o State | xargs)" = COMPLETED
grep -q '^PAUL_HYBRID_READER_SMOKE_PASS$' "$own/logs/job-$smoke_job/reader-smoke.log"
test -z "$(git -C "$own/src/cudf-spark" status --short)"
git -C "$own/src/cudf-spark" pull --ff-only
test "$(git -C "$own/src/cudf-spark" rev-parse HEAD)" = "$harness_revision"
mkdir -p "$own/submissions"
export TPCH_POLICY_MANIFEST_PATH
TPCH_POLICY_MANIFEST_PATH=$(mktemp "$own/submissions/policy-${nodes}node-${scan_profile}-XXXXXXXX.json")
python3 - "$own/runtime-policy.json" "$smoke_job" "$nodes" "$nodelist" "$TPCH_POLICY_MANIFEST_PATH" "$scan_profile" <<'PY'
import json
import sys
from pathlib import Path
policy = json.loads(Path(sys.argv[1]).read_text())
assert policy['artifact_identity']['build_job'] == int(sys.argv[2])
policy['name'] = f'paul-hybrid-multithreaded-sf30k-{sys.argv[3]}node'
policy['benchmark_contract']['nodes'] = sys.argv[4]
policy['benchmark_contract']['executors'] = int(sys.argv[3]) * 4
split_sizes = {'split512m': '512m', 'split1g': '1g', 'split2g': '2g'}
if sys.argv[6] in split_sizes:
    policy['name'] += '-' + sys.argv[6]
    conf = policy['uniform_configuration']
    conf['spark.sql.files.maxPartitionBytes'] = split_sizes[sys.argv[6]]
    # A high partition-count ceiling preserves the requested byte-size splits.
    conf['spark.sql.files.maxPartitionNum'] = '65536'
Path(sys.argv[5]).write_text(json.dumps(policy, indent=2) + '\n')
PY
chmod 444 "$TPCH_POLICY_MANIFEST_PATH"
if [[ ! -e "$own/scripts" ]]; then
  ln -s "$own/src/cudf-spark/experiments/paul-hybrid-261105/suite" "$own/scripts"
fi
test "$(readlink "$own/scripts")" = "$own/src/cudf-spark/experiments/paul-hybrid-261105/suite"
test -s "$TPCH_POLICY_MANIFEST_PATH"
export TPCH_QUERIES=$query
export TPCH_EXPERIMENT_PROFILE="paul-hybrid-multithreaded-sf30k-${nodes}node-${scan_profile}"
"$slurm/sbatch" --parsable --nodes="$nodes" \
  --nodelist="$nodelist" --no-requeue \
  --job-name="mahonem-paul-hybrid-${nodes}n-${scan_profile}-q$query" \
  --output="$own/logs/slurm-%j.out" --error="$own/logs/slurm-%j.err" \
  --export=ALL,TPCH_QUERIES,TPCH_POLICY_MANIFEST_PATH,TPCH_EXPERIMENT_PROFILE \
  "$own/scripts/run_sf30k.slurm"
SH
