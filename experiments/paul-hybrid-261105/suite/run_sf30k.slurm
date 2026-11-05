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
#SBATCH --job-name=mahonem-paul-hybrid-sf30k
#SBATCH --partition=nvidia-part
#SBATCH --nodes=5
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=144
#SBATCH --mem=0
#SBATCH --gres=gpu:4
#SBATCH --exclusive
#SBATCH --time=00:20:00
set -euo pipefail
readonly own=/home/mahonem/paul_hybrid_multithreaded_261105
readonly legacy=/home/prestouser/mahonem/wild_spark_rapids
export SLURM_CONF=/cm/shared/apps/slurm/etc/slurm/slurm.conf
export PATH=/cm/local/apps/slurm/current/bin:$PATH
export TPCH_DATA_DIR=/scratch/prestouser/tpch-rs-float-no-delta/scale-30000
export TPCH_QUERY_DIR=$legacy/inputs/tpch-queries-sf30000-canonical
export TPCH_QUERIES=${TPCH_QUERIES:?}
export TPCH_QUERY_ITERATIONS=2
export TPCH_QUERY_TIMEOUT_SECONDS=120
export TPCH_ABORT_SUITE_AFTER_FAILURE=true
export TPCH_STOP_QUERY_AFTER_FAILURE=true
export TPCH_EXPECTED_EXECUTORS=$((SLURM_JOB_NUM_NODES * 4))
export TPCH_EXECUTORS_PER_NODE=4
export TPCH_PARQUET_READER_TYPE=MULTITHREADED
export TPCH_PARQUET_DICTIONARY_FILTER_ENABLED=false
export TPCH_POLICY_MANIFEST_PATH=${TPCH_POLICY_MANIFEST_PATH:?Specify the submission policy snapshot}
export TPCH_APPLICATION=$own/scripts/run_tpch_suite.py
export TPCH_RAPIDS_JAR=$own/src/cudf-spark/dist/target/rapids-4-spark_2.12-26.10.0-SNAPSHOT-cuda13-arm64.jar
export TPCH_RAPIDS_UCX_RUNTIME=$legacy/artifacts/ucx-5025d2a2-cuda13-aarch64/runtime/usr
export TPCH_TRUSTED_METADATA_PATH=$legacy/manifests/tpch-sf30000-presto-analyze-trusted-constraints.properties
export TPCH_RESULT_DIGEST_JAR=$legacy/artifacts/result-digest-helper/job-14629/result-digest-helper.jar
export TPCH_RESULT_DIGEST_ENABLED=true
export TPCH_PERSIST_FULL_RESULT=true
export TPCH_RESULT_SINK_POLICY=jvm-driver-collect-all
export TPCH_OFFLINE_APT_CACHE=$legacy/artifacts/apt-cache/ubuntu2404-arm64-cuda13.1
export TPCH_SUBMIT_SCRIPT=$own/scripts/run_sf30k.slurm
export TPCH_EXPERIMENT_PROFILE=${TPCH_EXPERIMENT_PROFILE:-paul-hybrid-multithreaded-sf30k-${SLURM_JOB_NUM_NODES}node}
export TPCH_RAPIDS_VERSION=26.10.0-SNAPSHOT-paul-hybrid-experiment
export TPCH_ENABLE_NODE_TELEMETRY=true
export HEAD_NODE
HEAD_NODE=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1)
mkdir -p "$own/logs"
scontrol show job "$SLURM_JOB_ID" > "$own/logs/slurm-$SLURM_JOB_ID.job.txt"
srun --kill-on-bad-exit=1 --nodes="$SLURM_JOB_NUM_NODES" --ntasks-per-node=1 \
  --container-image=nvcr.io#nvidia/cuda:13.1.0-base-ubuntu24.04 \
  --container-mounts=/home/mahonem:/home/mahonem,/home/prestouser:/home/prestouser,/scratch:/scratch,/raid:/raid,/dev/infiniband:/dev/infiniband \
  --container-remap-root --container-writable \
  bash "$own/scripts/run_cudf_spark_cluster.sh"
