#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
readonly work=${1:?Owned work directory required}
exec > >(tee "$work/evidence/prepare-observers.log") 2>&1
sudo apt-get install -y bpftrace linux-tools-common "linux-tools-$(uname -r)"
uname -a
perf --version
bpftrace --version
sudo bpftrace -lv 'tracepoint:vmscan:mm_vmscan_direct_reclaim_*'
for file in /proc/sys/vm/min_free_kbytes /proc/sys/vm/watermark_scale_factor \
    /proc/sys/vm/zone_reclaim_mode /sys/kernel/mm/lru_gen/enabled \
    /sys/kernel/mm/transparent_hugepage/enabled /sys/kernel/mm/transparent_hugepage/defrag; do
    echo "$file"
    cat "$file"
done
cat /proc/zoneinfo > "$work/evidence/initial-zoneinfo.txt"
findmnt -T /opt/dlami/nvme
nvidia-smi --query-gpu=index,pci.bus_id,driver_version,name --format=csv
nvidia-smi topo -m
sudo dmesg > "$work/evidence/initial-dmesg.txt"
printf '0\n' > "$work/evidence/prepare-observers.exit"
