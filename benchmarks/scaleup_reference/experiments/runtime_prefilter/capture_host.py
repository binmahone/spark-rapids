#!/usr/bin/env python3
"""Record the owned host's effective prerequisites without credentials."""
from datetime import datetime,timezone
import hashlib,json,subprocess,sys
from pathlib import Path
work=Path(sys.argv[1]).resolve()
def command(*args):return subprocess.check_output(args,text=True,stderr=subprocess.STDOUT)
params=Path('/proc/driver/nvidia/params').read_text()
assert 'ForceP2P=17;RMForceP2PType=1;RMPcieP2PType=1' in params and 'DmaRemapPeerMmio: 0' in params
matrices={}
for mode in ('r','w'):
 value=command('nvidia-smi','topo','-p2p',mode)
 assert sum(line.split().count('OK') for line in value.splitlines() if line.lstrip().startswith('GPU'))==56
 matrices[mode]=value
libraries={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((work/'ucx/lib').glob('libuc?.so.0.0.0'))}
report={'time_utc':datetime.now(timezone.utc).isoformat(),'driver_params':params,'p2p':matrices,'gpu_inventory':command('nvidia-smi','--query-gpu=index,name,uuid,pci.bus_id,driver_version,memory.total','--format=csv,noheader'),
 'java_version':command(str(work/'jdk17/bin/java'),'-version'),'ucx_version':command(str(work/'ucx/bin/ucx_info'),'-v'),'ucx_library_hashes':libraries,
 'runtime_packages':command('dpkg-query','-W','-f=${Package}\t${Version}\n','libnvjitlink-13-1','cuda-nvrtc-13-1','openjdk-17-jdk-headless'),
 'mount':command('findmnt','-T','/opt/dlami/nvme'),'numa':command('numactl','--hardware')}
(work/'evidence/host-prerequisites.json').write_text(json.dumps(report,indent=2)+'\n')
print('Verified P2P read/write 56 pairs each; recorded Java, UCX and CUDA prerequisites')
