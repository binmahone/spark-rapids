#!/usr/bin/env bash
#SBATCH --job-name=mahonem-dictionary-prototype
#SBATCH --output=/home/prestouser/mahonem/260908_nvl72_ucx_compression/logs/dictionary-prototype/slurm-%j.out
#SBATCH --error=/home/prestouser/mahonem/260908_nvl72_ucx_compression/logs/dictionary-prototype/slurm-%j.err
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --gres=gpu:1
#SBATCH --nodelist=presto-gb200-gcn-12
#SBATCH --time=00:30:00

set -euo pipefail

readonly WORK_ROOT=/home/prestouser/mahonem/260908_nvl72_ucx_compression
readonly IMAGE=${PROTOTYPE_CONTAINER_IMAGE:-nvcr.io#nvidia/cuda:13.1.0-base-ubuntu24.04}
readonly MOUNTS=/scratch:/scratch,/home/prestouser:/home/prestouser,/raid:/raid

mkdir -p "${WORK_ROOT}/logs/dictionary-prototype"

df -hT / /home/prestouser /raid | tee \
  "${WORK_ROOT}/logs/dictionary-prototype/storage-${SLURM_JOB_ID}.txt"
readonly ROOT_FREE_BYTES=$(df -PB1 / | awk 'NR == 2 {print $4}')
readonly HOME_FREE_BYTES=$(df -PB1 /home/prestouser | awk 'NR == 2 {print $4}')
if ((ROOT_FREE_BYTES < 20 * 1024 * 1024 * 1024)); then
  echo "Node root filesystem has less than 20 GiB free" >&2
  exit 1
fi
if ((HOME_FREE_BYTES < 20 * 1024 * 1024 * 1024)); then
  echo "Shared home filesystem has less than 20 GiB free" >&2
  exit 1
fi

srun --nodes=1 --ntasks=1 \
  --container-image="${IMAGE}" \
  --container-mounts="${MOUNTS}" \
  --container-remap-root \
  --container-writable \
  bash -lc 'set -euo pipefail; export DEBIAN_FRONTEND=noninteractive; apt-get update; apt-get install -y --no-install-recommends openjdk-17-jdk-headless; bash /home/prestouser/mahonem/260908_nvl72_ucx_compression/src/cudf-spark-main/dev/dictionary-prototype/run_dictionary_encoding_prototype.sh'
