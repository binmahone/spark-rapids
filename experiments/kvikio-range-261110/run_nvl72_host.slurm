#!/usr/bin/env bash
#SBATCH --job-name=mahonem-kvikio-host-ranges
#SBATCH --partition=nvidia-part
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=40
#SBATCH --mem=48G
#SBATCH --time=00:15:00
set -euo pipefail
readonly root=${KVIKIO_STUDY_ROOT:?Set the isolated personal-home study directory}
readonly revision=${KVIKIO_SOURCE_REVISION:?Pin the source revision}
readonly source=$root/src/cudf-spark
readonly scripts=$source/experiments/kvikio-range-261110
readonly python=$root/host-only-venv/bin/python
readonly uv=$root/tools/uv
readonly table=/scratch/prestouser/tpch-rs-float-no-delta/scale-30000/lineitem
readonly output=$root/evidence/job-${SLURM_JOB_ID:?}
mkdir -p "$output"
exec > >(tee "$output/full.log") 2>&1
test "$(git -C "$source" rev-parse HEAD)" = "$revision"
test -z "$(git -C "$source" status --short)"
hostname
id -un
df -hT /home/mahonem /scratch
test -d "$table"
export PATH="$root/tools:$PATH"
"$uv" --version
"$uv" venv --allow-existing --python /usr/bin/python3 "$root/host-only-venv"
"$uv" pip install --python "$python" --no-deps --only-binary=:all: \
  -r "$scripts/requirements-host.txt"
"$python" -c 'import kvikio, numpy, pyarrow; print(kvikio.__version__)'
g++ --version
for count in 1 4; do
  files=()
  for ((i=0; i<count; i++)); do
    files+=("$table/part.$i.parquet")
  done
  "$python" "$scripts/range_benchmark.py" plan --files "${files[@]}" \
    --output "$output/ranges-$count-files.json"
  for cache in warm advise-drop; do
    /cm/local/apps/slurm/current/bin/srun --ntasks=1 \
      --cpus-per-task="${SLURM_CPUS_PER_TASK:-40}" \
      bash "$scripts/run_host_matrix.sh" "$python" "$output/ranges-$count-files.json" \
      "$output/$count-files-$cache" "$count" "$cache"
  done
done
printf '%s\n' KVIKIO_HOST_RANGE_MATRIX_COMPLETE
