#!/usr/bin/env bash
set -euo pipefail
readonly here=$(cd "$(dirname "$0")" && pwd)
readonly revision=$(git -C "$here" rev-parse HEAD)
readonly node=${1:-presto-gb200-gcn-02}
[[ "$node" =~ ^presto-gb200-gcn-[0-9]{2}$ ]]
test -z "$(git -C "$here" status --short)"
ssh -F /home/nvidia/mahone_gluten/cluster_ssh_config \
  -o ConnectTimeout=12 -o ServerAliveInterval=15 -o ServerAliveCountMax=2 \
  presto-cp-02-via-mac bash -s -- "$revision" "$node" <<'REMOTE'
set -euo pipefail
readonly revision=$1
readonly node=$2
readonly root=/home/mahonem/kvikio_range_validation_261110
readonly source=$root/src/cudf-spark
readonly branch=test/kvikio-range-validation-261110
export SLURM_CONF=/cm/shared/apps/slurm/etc/slurm/slurm.conf
test "$(df -PB1 /home/mahonem | awk 'NR==2 {print $4}')" -gt 10737418240
mkdir -p "$root/src" "$root/logs"
if [[ ! -d "$source/.git" ]]; then
  git clone --filter=blob:none --no-checkout --single-branch --branch "$branch" \
    https://github.com/binmahone/spark-rapids.git "$source"
  git -C "$source" sparse-checkout set experiments/kvikio-range-261110
  git -C "$source" checkout "$branch"
else
  test -z "$(git -C "$source" status --short)"
  git -C "$source" pull --ff-only
fi
test "$(git -C "$source" rev-parse HEAD)" = "$revision"
export KVIKIO_STUDY_ROOT=$root
export KVIKIO_SOURCE_REVISION=$revision
/cm/local/apps/slurm/current/bin/sbatch --parsable --nodelist="$node" \
  --output="$root/logs/slurm-%j.out" --error="$root/logs/slurm-%j.err" \
  --export=ALL,KVIKIO_STUDY_ROOT,KVIKIO_SOURCE_REVISION \
  "$source/experiments/kvikio-range-261110/run_nvl72_host.slurm"
REMOTE
