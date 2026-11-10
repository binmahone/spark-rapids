#!/usr/bin/env bash
set -euo pipefail
readonly job=${1:?Specify the recorded Slurm job ID from this study}
[[ "$job" =~ ^[0-9]+$ ]]
readonly local_root=/home/nvidia/mahone_gluten/wild_spark_rapids/261110_kvikio_range_validation/evidence
readonly remote_root=/home/mahonem/kvikio_range_validation_261110/evidence
readonly config=/home/nvidia/mahone_gluten/cluster_ssh_config
mkdir -p "$local_root"
expected=$(ssh -F "$config" -o ConnectTimeout=12 presto-cp-02-via-mac \
  bash -s -- "$job" <<'REMOTE'
set -euo pipefail
readonly job=$1
readonly root=/home/mahonem/kvikio_range_validation_261110/evidence
export SLURM_CONF=/cm/shared/apps/slurm/etc/slurm/slurm.conf
active=$(/cm/local/apps/slurm/current/bin/squeue -j "$job" -h)
test -z "$active"
test -d "$root/job-$job"
tar -czf "$root/job-$job.tar.gz" -C "$root" "job-$job"
sha256sum "$root/job-$job.tar.gz" | cut -d ' ' -f 1
REMOTE
)
[[ "$expected" =~ ^[0-9a-f]{64}$ ]]
scp -F "$config" "presto-cp-02-via-mac:$remote_root/job-$job.tar.gz" \
  "$local_root/job-$job.tar.gz"
printf '%s  %s\n' "$expected" "$local_root/job-$job.tar.gz" | sha256sum --check
tar -xzf "$local_root/job-$job.tar.gz" -C "$local_root"
printf '%s\n' "$expected" > "$local_root/job-$job.tar.gz.sha256"
