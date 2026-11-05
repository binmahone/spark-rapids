#!/usr/bin/env bash
#SBATCH --job-name=mahonem-paul-hybrid-build-smoke
#SBATCH --partition=nvidia-part
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=24
#SBATCH --mem=64G
#SBATCH --gres=gpu:1
#SBATCH --time=00:45:00
set -euo pipefail
export PUBLIC_REVISION=${PUBLIC_REVISION:?}
export PRIVATE_REVISION=${PRIVATE_REVISION:?}
/cm/local/apps/slurm/current/bin/srun --kill-on-bad-exit=1 \
  --container-image=nvcr.io#nvidia/cuda:13.1.0-base-ubuntu24.04 \
  --container-mounts=/home/mahonem:/home/mahonem,/home/prestouser:/home/prestouser,/scratch:/scratch \
  --container-remap-root --container-writable \
  bash /home/mahonem/paul_hybrid_multithreaded_261105/src/cudf-spark/experiments/paul-hybrid-261105/build_and_smoke.sh
