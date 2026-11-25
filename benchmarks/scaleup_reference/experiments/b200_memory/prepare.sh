#!/usr/bin/env bash
set -euo pipefail
readonly work=${1:?Owned absolute directory is required}
readonly suite=$(cd "$(dirname "$0")/../.." && pwd)
mkdir -p "$work/evidence" "$work/runtime/packages" "$work/runtime/native"
exec > >(tee -a "$work/evidence/prepare.log") 2>&1
trap 'code=$?; printf "%s\n" "$code" > "$work/evidence/prepare.exit"' EXIT
test "$(id -un)" = mahonem
test "$(hostname)" = umb-b200-220
test "$(df --output=avail -B1 "$work" | tail -1)" -gt 274877906944
cd "$work/runtime/packages"
apt-get download openjdk-17-jdk-headless openjdk-17-jre-headless libtool libtool-bin libnuma-dev libnuma1
for package in ./*.deb; do dpkg-deb -x "$package" "$work/runtime/sdk"; done
python3 - "$work" <<'PY'
from pathlib import Path
import os,sys
w=Path(sys.argv[1]);sdk=w/'runtime/sdk';jdk=sdk/'usr/lib/jvm/java-17-openjdk-amd64'
for base,dirs,files in os.walk(jdk):
    for name in files+dirs:
        p=Path(base)/name
        if p.is_symlink():
            link=os.readlink(p)
            target=sdk/link.lstrip('/') if link.startswith('/') else None
            if target is not None and target.exists():
                p.unlink();p.symlink_to(target)
(w/'jdk17').symlink_to(jdk)
PY
"$work/jdk17/bin/java" -version
bash "$suite/scripts/prepare_spark.sh" "$work/spark-venv"
for library in libnvrtc.so.13.1.115 libnvrtc-builtins.so.13.1.115 libnvJitLink.so.13.1.115; do
    cp -L "/raid/mahonem/artifacts/$library" "$work/runtime/native/$library"
done
ln -s libnvrtc.so.13.1.115 "$work/runtime/native/libnvrtc.so.13"
ln -s libnvrtc-builtins.so.13.1.115 "$work/runtime/native/libnvrtc-builtins.so.13.1"
ln -s libnvJitLink.so.13.1.115 "$work/runtime/native/libnvJitLink.so.13"
export PATH="$work/runtime/sdk/usr/bin:$PATH"
export LIBTOOL_DATADIR="$work/runtime/sdk/usr/share/libtool"
export ACLOCAL_PATH="$work/runtime/sdk/usr/share/aclocal"
export CPPFLAGS="-I$work/runtime/sdk/usr/include"
export LDFLAGS="-L$work/runtime/sdk/usr/lib/x86_64-linux-gnu"
BUILD_JOBS=16 bash "$suite/scripts/build_ucx.sh" "$work/ucx" /usr/local/cuda-13.0
python3 - "$work" <<'PY'
import hashlib,json,os,sys
from pathlib import Path
w=Path(sys.argv[1]);props=Path('/raid/alfxu/runs/cudf-spark-sf10000-8gpu-p64-b4g-cpu-stats-20261015/inputs/tpch-sf10000.properties')
raw=props.read_bytes();assert hashlib.sha256(raw).hexdigest()=='6076215127dc3610bf8a4b6af4ad2c2ba1719adcf9388ff29a28bd4e1ccdc20c'
d=dict(line.split('=',1) for line in raw.decode().splitlines() if line and not line.startswith('#') and '=' in line)
dataset=Path('/raid/knataraj/datasets/tpch/sf10k_v2_float');tables={}
for table in ['customer','lineitem','nation','orders','part','partsupp','region','supplier']:
    total=sum(e.stat().st_size for e in os.scandir(dataset/table) if e.is_file() and e.name.endswith('.parquet'))
    assert total==int(d['spark.table.'+table+'.sizeInBytes']),table
    prefix='spark.column.'+table+'.';columns={}
    for key,value in d.items():
        if not key.startswith(prefix):continue
        column,kind=key[len(prefix):].rsplit('.',1)
        mapping={'distinctCount':'distinct_count','nullCount':'null_count','min':'minimum','max':'maximum'}
        if kind in mapping:columns.setdefault(column,{})[mapping[kind]]=int(value) if kind in ['distinctCount','nullCount'] else value
    tables[table]={'row_count':int(d['spark.table.'+table+'.rowCount']),'columns':columns,'parquet_bytes':total}
snapshot={'dataset':str(dataset),'source':{'properties':str(props),'sha256':hashlib.sha256(raw).hexdigest()},'tables':tables}
(w/'evidence/catalog-statistics.json').write_text(json.dumps(snapshot,indent=2)+'\n')
(w/'evidence/tpch-sf10000.properties').write_bytes(raw)
homes=list((w/'spark-venv/lib').glob('python*/site-packages/pyspark'));assert len(homes)==1
deployment={'spark_home':str(homes[0]),'java_home':str(w/'jdk17'),'ucx_home':str(w/'ucx'),
            'scratch_root':str(w/'scratch'),'nodes':1,'gpus_per_node':4}
(w/'evidence/deployment.json').write_text(json.dumps(deployment,indent=2)+'\n')
PY
sha256sum "$work"/runtime/native/* "$work"/artifacts/* > "$work/evidence/runtime-hashes.txt"
