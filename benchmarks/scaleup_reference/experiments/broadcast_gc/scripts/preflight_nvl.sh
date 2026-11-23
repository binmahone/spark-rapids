#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
set -eo pipefail
source /etc/profile.d/modules.sh
module load slurm/slurm/25.05
set -u
hostname
id -un
sinfo -N -n 'presto-gb200-gcn-[02,04,06,08-10,12-13]' -o '%N %T %G'
squeue -o '%.18i %.16u %.24j %.8T %.10M %.6D %R'
scontrol show reservation
ls /home/prestouser/mahonem/wild_spark_rapids/software/spark-3.5.3-bin-hadoop3/jars/*xbean*
