#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#SBATCH --job-name=mahonem-paul-h2d-public-build
#SBATCH --partition=nvidia-part
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=24
#SBATCH --mem=64G
#SBATCH --gres=gpu:1
#SBATCH --time=00:45:00
set -euo pipefail
export PUBLIC_REVISION=${PUBLIC_REVISION:?}
export JNI_REVISION=${JNI_REVISION:?}
export NATIVE_JOB=${NATIVE_JOB:?}
/cm/local/apps/slurm/current/bin/srun --kill-on-bad-exit=1 \
  --container-image=nvcr.io#nvidia/cuda:13.1.0-base-ubuntu24.04 \
  --container-mounts=/home/mahonem:/home/mahonem,/home/prestouser:/home/prestouser \
  --container-remap-root --container-writable \
  bash /home/mahonem/paul_hybrid_multithreaded_261105/src/cudf-spark-h2d/experiments/paul-hybrid-261105/h2d/build_public.sh
