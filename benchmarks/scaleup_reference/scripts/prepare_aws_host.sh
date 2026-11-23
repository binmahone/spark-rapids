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

set -euo pipefail
# Run explicitly as root on an owned benchmark instance, before submitting GPU work.
test "$(id -u)" = 0
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends openjdk-17-jdk-headless python3-venv python3-pip \
  numactl build-essential autoconf automake libtool pkg-config libnuma-dev \
  libnvjitlink-13-1 cuda-nvrtc-13-1
cat > /etc/modprobe.d/scaleup-reference-p2p.conf <<'EOF'
options nvidia NVreg_RegistryDwords="ForceP2P=17;RMForceP2PType=1;RMPcieP2PType=1" NVreg_DmaRemapPeerMmio=0
EOF
echo 'P2P configuration written. Reboot this owned instance, then run benchmark validate.'
