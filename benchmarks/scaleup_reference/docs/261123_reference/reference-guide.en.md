# NVL72 and AWS 48x Reference Configurations and Reproduction Guide (2026-11-23)

This package provides one launcher for two cuDF-Spark experiments: NVL72 with TPC-H SF30000 and one AWS g7e.48xlarge with TPC-H SF10000. The repository contains the launch scripts, complete configurations, SQL, frozen optimizer statistics and result-validation references. The experiments use the existing datasets listed below.

The workloads differ in scale, numeric types, storage and reader mode. Their timings cannot be divided directly to obtain a hardware speedup ratio. These are engineering reference results, not official TPC benchmark results.

The package supports the two existing environments. Deployment paths and infrastructure settings require adjustment when moving to another environment. It does not depend on scripts from earlier experiment directories or manual JAR modifications.

## Start here

The tested executable source is [binmahone/spark-rapids at `faa47fe51f998ae97385e2b218be4a261006bfa6`](https://github.com/binmahone/spark-rapids/tree/faa47fe51f998ae97385e2b218be4a261006bfa6). The shared entry point is [benchmarks/scaleup_reference/benchmark.py](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/benchmark.py), with an accompanying [README](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/README.md).

| Item | NVL72 | AWS |
| --- | --- | --- |
| Hardware | Eight GB200 nodes, four GPUs per node, 32 GPUs total | One g7e.48xlarge, eight RTX PRO 6000 Blackwell Server Edition GPUs |
| Workload | TPC-H SF30000 | TPC-H SF10000 |
| Existing dataset | `/scratch/prestouser/tpch-rs-float-no-delta/scale-30000` | `/opt/dlami/nvme/daily-trend-hardened/dataset` |
| Numeric columns | Native double | Native decimal(15,2) |
| Spark | Vanilla 3.5.3 | Vanilla 3.5.3 |
| Reader | Hybrid MULTITHREADED, GPUDirect Storage (GDS) | Hybrid MULTITHREADED, native host reads |
| Application lifecycle | One application for all timed queries | One application per query |
| Repetitions | Two complete executions per query | Two complete executions per query |

GDS reads file data into GPU buffers. The AWS profile reads compressed data into host memory before GPU processing. The retained reader metrics confirm that the intended paths executed.

## Pinned software and source locations

Every component below has a source location and a fixed revision. The executable source commit differs from the documentation commit: check out the executable revision above when running the benchmark with the published artifacts.

| Component | Source at the tested revision | Role |
| --- | --- | --- |
| Final public engine and benchmark package | [binmahone/spark-rapids, `faa47fe51f998ae97385e2b218be4a261006bfa6`](https://github.com/binmahone/spark-rapids/tree/faa47fe51f998ae97385e2b218be4a261006bfa6) | Contains the complete public source, benchmark scripts, configurations, SQL and validation helpers |
| Public source base | [Paul's hybrid_scaleup, `d64ace4eba1f0a49c2cd4ea89644dfca8892140e`](https://github.com/pmattione-nvidia/cudf-spark/tree/d64ace4eba1f0a49c2cd4ea89644dfca8892140e) | Base for the reference engine; latest public main was not merged |
| Driverless-broadcast cleanup | [NVIDIA/cudf-spark, `9a854c8d312eb36c2fc0a0760eaa80a0217f8243`](https://github.com/NVIDIA/cudf-spark/commit/9a854c8d312eb36c2fc0a0760eaa80a0217f8243), [PR #17016](https://github.com/NVIDIA/cudf-spark/pull/17016) | Applied separately to the base; included in the final public source above |
| Ordinary-broadcast cleanup | [IndependentQueryBoundary.scala](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/sql-plugin/src/main/scala/org/apache/spark/sql/rapids/IndependentQueryBoundary.scala) and [ReferenceBroadcastSnapshot.scala](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/sql-plugin/src/main/scala/org/apache/spark/broadcast/ReferenceBroadcastSnapshot.scala) at the final public commit | Restricted independent-query cleanup, related to [issue #17019](https://github.com/NVIDIA/cudf-spark/issues/17019) |
| Private optimizer | [Upstream nvspark/cudf-spark-private, `61e9e96803771371dec116593f0c4eced9ac44d8`](https://gitlab-master.nvidia.com/nvspark/cudf-spark-private/-/tree/61e9e96803771371dec116593f0c4eced9ac44d8) | Fixed optimizer source included in the engine; NVIDIA GitLab access is needed for a source rebuild |
| Java/native bindings (JNI) | [NVIDIA/cudf-spark-jni, `bf63ec567eabc0bcac5366f6afba0b0bba40f30a`](https://github.com/NVIDIA/cudf-spark-jni/tree/bf63ec567eabc0bcac5366f6afba0b0bba40f30a) | Official Maven artifact `26.12.0-20261122.042214-52`; ARM uses `cuda13-arm64`, x86 uses `cuda13` |
| cuDF | [NVIDIA/cudf, `3c92c2094dae1931ef7c2c19ee932cc26822a000`](https://github.com/NVIDIA/cudf/tree/3c92c2094dae1931ef7c2c19ee932cc26822a000) | cuDF revision embedded in the official JNI artifact |
| Shared RAPIDS source submodule | [NVIDIA/spark-rapids-common, `5b570402a63ce8feeb97494c9998076a1908c0ae`](https://github.com/NVIDIA/spark-rapids-common/tree/5b570402a63ce8feeb97494c9998076a1908c0ae) | Git submodule initialized by the public build script |
| UCX | [openucx/ucx v1.22.0, `8a6b06fb880accbb933a79cda893883872c68d9d`](https://github.com/openucx/ucx/tree/8a6b06fb880accbb933a79cda893883872c68d9d), plus the checked-in [1.22 interval-tree patch](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/patches/ucx-1.22-interval-search.patch) | Built as a complete installation; no separate replacement libucs overlay |
| Apache Spark | [apache/spark 3.5.3, `32232e9ed33bb16b93ad58cfde8b82e0f07c0970`](https://github.com/apache/spark/tree/32232e9ed33bb16b93ad58cfde8b82e0f07c0970) | Vanilla Spark runtime, installed from the 3.5.3 distribution or PySpark package |
| Statistics, result validation and native-read qualification tools | [tools/src/main/java](https://github.com/binmahone/spark-rapids/tree/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/tools/src/main/java) at the final public commit | Source for benchmark-tools.jar; separate from the official JNI implementation |

The machine-readable dependency record is [configs/sources.lock.json](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/configs/sources.lock.json). The per-architecture manifests retain the component revisions and hashes of the actual tested JARs.

### Does the private optimizer require the mahonem fork?

**No. The requirement is the fixed source commit, not the personal fork.** The exact commit `61e9e96803771371dec116593f0c4eced9ac44d8` is available in the upstream `nvspark/cudf-spark-private` repository and is an ancestor of upstream main. It is the merge of [MR !446](https://gitlab-master.nvidia.com/nvspark/cudf-spark-private/-/merge_requests/446). Upstream availability and ancestry were verified through the GitLab API; see `private-optimizer-provenance.json`.

The tested build cloned [mahonem/cudf-spark-private](https://gitlab-master.nvidia.com/mahonem/cudf-spark-private/-/tree/61e9e96803771371dec116593f0c4eced9ac44d8) at that same commit with no local private-source modifications. The fork URL remains in the historical build manifest and in the tested build script's default clone location. There are no additional private patches unique to that fork in this build.

The reference configurations use the private optimizer, and the complete engine JAR already contains its built component. **Running a published complete JAR does not require cloning the private repository.** Rebuilding the engine from source requires access to the pinned private source. An upstream checkout can be supplied through `PRIVATE_SOURCE`, as shown below. Do not substitute the current tip of main for the pinned commit.

This version also removes replacement NativeReaderBinding.class files, the auxiliary libforecast_native_fileio.so, JAR patching of native-reader bindings, and the separate broadcast-cleanup helper JAR. Ordinary-broadcast cleanup is compiled into the public engine. benchmark-tools.jar provides required catalog-statistics loading, optional result validation and native-read qualification.

The UCX patch targets the 1.22 tree. Its upstream equivalent is [PR #12367](https://github.com/openucx/ucx/pull/12367), commit [ffbb1aa4c8ffdf2a93628b222f1594a70f76625c](https://github.com/openucx/ucx/commit/ffbb1aa4c8ffdf2a93628b222f1594a70f76625c), which uses a newer generic red-black tree. This experiment retains UCX 1.22.0 with the included adaptation and regression tests. The patch SHA-256 is `80853be416a7193d91f856f6e4898f8e276d3d69e6ee06f9617f4b920ed47795`.

## Validated results

Both complete runs used the pinned executable source and newly built artifacts on November 23, 2026, Beijing time.

| Metric | NVL72 SF30000 | AWS SF10000 |
| --- | --- | --- |
| Complete validated samples | 22 queries x 2 executions, 44/44 passed | 22 queries x 2 executions, 44/44 passed |
| Sum of per-query best-of-two SQL times | **154.665 s** | **447.041 s** |
| SQL plus cleanup for those same selected samples | **171.848 s** | **467.840 s** |
| Sum of second-execution SQL times | 156.726 s | 447.041 s |
| Reader evidence | Positive GDS device bytes in 44/44 samples; zero fallback files | Positive native host bytes in 44/44 samples; zero fallback files |
| Executor cleanup acknowledgements | 44 x 32 = 1,408 | 44 x 8 = 352 |
| Service teardown records | All eight nodes | All 22 application groups |

NVL Slurm job `25453` completed with exit code `0:0`. Its timed application was `app-20261123042758-0001`; the additional Q11 reference application was `app-20261123042724-0000`. AWS application IDs are listed in `qualification-summary.json`. The AWS validation instance was `i-0751e3edbfd4c95a6` in us-east-2c; it and its dedicated disk/access resources were released after evidence collection.

All entries below are seconds. Each SQL-plus-cleanup entry uses the same execution selected by the lower SQL time. Both raw execution times and the selected iteration are retained in `per-query-results.csv`.

| Query | NVL SQL | NVL SQL + cleanup | AWS SQL | AWS SQL + cleanup |
| --- | --- | --- | --- | --- |
| Q1 | 3.568 | 4.608 | 14.872 | 15.902 |
| Q2 | 2.075 | 2.919 | 6.369 | 6.999 |
| Q3 | 9.641 | 10.738 | 24.085 | 25.429 |
| Q4 | 6.373 | 7.465 | 11.334 | 12.266 |
| Q5 | 12.830 | 13.835 | 28.465 | 29.410 |
| Q6 | 1.733 | 2.723 | 7.200 | 8.135 |
| Q7 | 6.650 | 7.653 | 24.242 | 25.381 |
| Q8 | 5.016 | 6.009 | 33.835 | 35.243 |
| Q9 | 24.900 | 26.326 | 43.315 | 44.563 |
| Q10 | 8.301 | 9.294 | 26.018 | 27.522 |
| Q11 | 6.098 | 6.148 | 6.729 | 6.759 |
| Q12 | 3.630 | 4.022 | 14.067 | 15.253 |
| Q13 | 4.557 | 5.000 | 13.004 | 14.186 |
| Q14 | 3.123 | 4.016 | 13.537 | 14.522 |
| Q15 | 6.658 | 7.055 | 25.985 | 27.474 |
| Q16 | 2.517 | 2.907 | 6.773 | 6.801 |
| Q17 | 3.946 | 4.989 | 17.807 | 18.254 |
| Q18 | 20.395 | 21.388 | 62.237 | 62.274 |
| Q19 | 5.626 | 6.036 | 12.973 | 13.965 |
| Q20 | 6.346 | 6.393 | 21.506 | 22.389 |
| Q21 | 7.852 | 8.852 | 25.999 | 27.440 |
| Q22 | 2.830 | 3.470 | 6.689 | 7.672 |

The formal driver-log audit covered two NVL applications and 22 AWS applications, with no matches for ERROR, task failure, UCX reclaim failure or native fatal errors. Every NVL GPU additionally passed both host and strict GDS native-read qualification against a complete 16 MiB CPU-read buffer: 64 records across 32 GPUs.

Ordinary-broadcast cleanup retired 836 objects on NVL and 244 on AWS. None of the cleanup receipts showed an increase in the RMM allocation counter. These records demonstrate execution of the cleanup path in this experiment; its supported lifecycle is defined below.

## Complete configuration and deliberate exceptions

The full profiles are [nvl72.json](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/configs/nvl72.json) and [aws48x.json](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/configs/aws48x.json). The `plan` action expands the final configuration for every query, including its overrides.

| Setting | NVL72 | AWS |
| --- | --- | --- |
| Executors / cores per executor | 32 / 8 | 8 / 8 |
| spark.task.resource.gpu.amount | 0.25: four Spark task slots per GPU | Normally 0.25; Q18 uses 0.5: two slots |
| spark.rapids.sql.concurrentGpuTasks | 5, dynamic configured true | 2, dynamic configured true |
| Pinned host pool per executor | 64 GiB | 16 GiB |
| Default scan split | 4 GiB | 2 GiB |
| Scan partition cap | Unset | 4096 |
| Shuffle partitions | 128 | 256 |
| Shuffle | UCX, no compression: none | UCX, LZ4 |
| GDS mode | WAIT_IF_GPU_BUSY | OFF |
| Metrics level | MODERATE | MODERATE |

Spark task slots limit simultaneously scheduled Spark tasks. concurrentGpuTasks controls entry into GPU work. These are separate controls: NVL's configured value of five does not provide five Spark task slots per GPU; the task-slot limit remains four.

The retained query exceptions are explicit:

- Both platforms use 1 GiB splits for Q13 to avoid the retained large-string overflow case. AWS Q13 also uses a scan partition cap of 8192.
- AWS Q18 uses two Spark task slots per GPU, a retained query tuning choice. Task resources are determined at application startup, so Q18 needs an application with its own resource setting. The AWS reference consistently starts one application per query and executes that query twice, cleaning between executions. Application startup is excluded from SQL time.
- NVL Q15 uses a temporary view to cast l_extendedprice and l_discount to decimal(15,2), preserving the query definition's exact aggregate equality. The casts run inside the timed SQL and the view is removed afterward. AWS already stores those columns as decimal.

The default query order is retained because cache and execution order can affect the experiment:

- NVL: 9, 21, 3, 18, 17, 1, 2, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 19, 20, 22.
- AWS: 3, 1, 2, 4, 5, 6, 9, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22.

AQE, speculation and dynamic executor allocation are disabled. Use the full JSON files for the remaining optimizer, memory, reader, shuffle and executor environment settings.

## Data, required statistics and optional result validation

Use the existing dataset paths above with the same schema, files and row-group layout. Regenerating data at the same scale factor constitutes a different experiment.

For a fresh owned AWS instance, `scripts/prepare_aws_data.sh` can download the existing dataset from `s3://haozhu-nf-us-east-2/TPC-H-sf10k-decimal/`, using an authorized AWS identity and sufficient local NVMe capacity. The dataset contains 1,538 Parquet files totaling 3,712,454,195,832 bytes.

The download helper writes to a separate staging directory, verifies it, and then publishes the complete directory. Existing data is verified without being overwritten. Preflight rejects visible temporary files that Spark could read as input, and checks file counts, sizes and Parquet header/footer magic. It does not hash every byte of the dataset.

Frozen catalog statistics are stored under `inputs/<platform>/catalog-statistics.json`. Loading them is required: the launcher installs row counts, distinct counts, null counts and available bounds into Spark's catalog, then reads back the installed statistics. They support optimizer estimates. NVL's snapshot originated from retained Presto ANALYZE. AWS uses measured Parquet footers and approximate distinct counts with measured null counts. No primary-key or foreign-key constraints are injected.

Result validation defaults ON:

- Small results are compared completely. Large results use multiset fingerprints over every output row.
- AWS Q3 and Q18 handle valid ties at a LIMIT boundary. Q18 validates against the complete eligible-row reference and its ordering conditions.
- NVL Q11 first runs an additional GPU reference with Hybrid disabled, then compares every output key/value using the documented numeric tolerance. This reference shares the GPU software stack and is not an independent CPU correctness oracle.

The `--no-validate` option supports diagnostics. Disabled validation, a single execution, a query subset, DEBUG metrics, dirty source or mismatched source/artifact revisions makes a run ineligible for a formal reference score.

## Get the source and validated artifacts

```bash
git clone --branch benchmark/261123-scaleup-reference https://github.com/binmahone/spark-rapids.git
cd spark-rapids
git checkout faa47fe51f998ae97385e2b218be4a261006bfa6
```

The validated binaries are stored in private S3. Each architecture has an engine JAR, benchmark-tools.jar and manifest.json; obtain the complete set.

Access prerequisite: an AWS identity authorized for s3:GetObject on the exact objects in `artifact-locations.json`, in us-east-1. Access to the dataset bucket does not imply access to the artifact bucket; the validation instance's default role could not list this artifact bucket. Download through an authorized identity and stage the files on the runtime hosts, or rebuild from the source below. No IAM changes or public bucket access are required by these instructions.

| Architecture / file | SHA-256 |
| --- | --- |
| `arm64 / benchmark-tools.jar` | `4ee01fa57115eeb6e59b52eb27ef26c3a5994dd57947f1bc72be38828c52fa90` |
| `arm64 / cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13-arm64.jar` | `f99002a23ffc885d8593039d3c6ccb8359c288a730cf2cf65db10d71c5952ded` |
| `arm64 / manifest.json` | `a1958779b5d4bc27c9d86936d46f4dcd0b3b481aa2c5cab12aa44cf944842e7a` |
| `amd64 / benchmark-tools.jar` | `ba687e884685d6a2438910b161562e9d8dc35cd583e27339f8fc76243cd1eb9f` |
| `amd64 / cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar` | `02058b8a21475075ea77ede99487b89e36424ee6518856685ca5dcaf2c98cb9e` |
| `amd64 / manifest.json` | `a312330ebaf5d8fda4f34e7f1ccdc29ed19cd8446089c7598fa3d7f2a46f9826` |

Exact artifact prefixes:

- `arm64`: `s3://mahonem-wild-sf10k-261012-503960270539-use1/scaleup-reference/261123/01a20622-0ca8-74d0-9805-6bc842a9fdb3/faa47fe51f99/arm64/a1958779b5d4/`
- `amd64`: `s3://mahonem-wild-sf10k-261012-503960270539-use1/scaleup-reference/261123/01a20622-0ca8-74d0-9805-6bc842a9fdb3/faa47fe51f99/amd64/a312330ebaf5/`

Download `artifact-locations.json` from the evidence links, then run the following at the executable source checkout root. Use arm64 for NVL or amd64 for AWS. It addresses exact objects without requiring bucket listing and verifies every downloaded SHA-256.

```bash
python3 - arm64 <<'PY'
import hashlib, json, subprocess, sys
from pathlib import Path
architecture = sys.argv[1]
record = next(r for r in json.loads(Path('artifact-locations.json').read_text())
              if r['architecture'] == architecture)
destination = Path('.build/artifacts') / architecture
destination.mkdir(parents=True, exist_ok=True)
for item in record['files']:
    target = destination / item['name']
    subprocess.run(['aws', 's3', 'cp', item['uri'], str(target),
                    '--region', 'us-east-1', '--only-show-errors'], check=True)
    assert hashlib.sha256(target.read_bytes()).hexdigest() == item['sha256']
PY
```

### Rebuild using the upstream private source

Use Java 17 and NVIDIA GitLab credentials authorized for the upstream private repository. From the executable public checkout root:

```bash
git clone https://gitlab-master.nvidia.com/nvspark/cudf-spark-private.git ../cudf-spark-private
git -C ../cudf-spark-private checkout --detach 61e9e96803771371dec116593f0c4eced9ac44d8
test -z "$(git -C ../cudf-spark-private status --porcelain)"
PRIVATE_SOURCE="$PWD/../cudf-spark-private" bash benchmarks/scaleup_reference/scripts/build.sh arm64
PRIVATE_SOURCE="$PWD/../cudf-spark-private" bash benchmarks/scaleup_reference/scripts/build.sh amd64
```

The tested script defaults to cloning the mahonem fork when PRIVATE_SOURCE is absent. Supplying the clean upstream checkout above selects the identical pinned source without editing the tested public code. The clone URL embedded in a newly built component can differ from the historical manifest; new artifacts have their own hashes and require their own runtime qualification.

The build uses one physically independent Maven local repository, defaulting to `.build/maven-repository`. Do not share a writable repository with other builds. BUILD_JAVA_HOME, MAVEN_REPOSITORY and PRIVATE_SOURCE select explicit locations. The script initializes the pinned spark-rapids-common submodule automatically.

Outputs are under `.build/artifacts/<architecture>/`. Preserve the engine, tools and manifest together. The build runs private optimizer tests, compiles the public test sources and executes selected CPU lifecycle tests. GPU tests run separately on allocated GPUs.

## Prepare the runtime

Spark must be vanilla 3.5.3, without preinstalled RAPIDS, cuDF, Gluten or Flux JARs in its jars directory. `scripts/prepare_spark.sh` installs an independent Spark 3.5.3 Python environment.

Build UCX using `scripts/build_ucx.sh INSTALL_PREFIX CUDA_HOME`, with an explicit complete CUDA SDK such as `/usr/local/cuda-13.0`. Installing CUDA 13.1 runtime packages can redirect `/usr/local/cuda` to a directory without development headers. The helper applies the included patch, builds a complete UCX installation and runs the interval-tree tests.

The deployment JSON selects machine paths and Slurm/container placement. Workload settings remain in the reference profiles. An example path configuration is:

```json
{
  "spark_home": "/your/shared/path/to/pyspark",
  "java_home": "/usr/lib/jvm/java-17-openjdk-arm64",
  "ucx_home": "/your/shared/path/to/ucx",
  "scratch_root": "/var/tmp/scaleup-reference"
}
```

Use the amd64 Java path and local NVMe scratch on AWS. NVL also requires container_image, container_mounts and optionally container_setup. The actual tested deployment objects, including NVL mounts and node selection, are in `tested-deployments.json`. Personal directories there document the validation deployment; replace them with accessible paths visible on every runtime node.

The saved startup chain has behavior that affects performance:

- Each GPU has one Spark worker, with CUDA_VISIBLE_DEVICES set by GPU UUID and logical GPU 0 exposed inside that worker. Barrier assignment records confirm 32 unique GPU UUIDs on NVL and eight on AWS.
- Workers bind CPUs to the GPU's NUMA node. NVL uses numactl --cpunodebind and --membind; AWS uses --cpunodebind and --preferred. The node is read from the GPU PCI device's sysfs entry.
- Workers advertise 32 cores/350g on NVL and 24 cores/200g on AWS. Each executor still uses its configured eight cores and memory; spark.cores.max limits the application total.
- LD_LIBRARY_PATH selects the built UCX installation first, then CUDA 13.1 library directories; NVL also adds host CUDA 13.0 for cuFile. UCX_MODULE_DIR selects that installation's lib/ucx. Each application's profile.json preserves the effective paths.
- NVL configures UCX_TLS=^posix and UCX_NET_DEVICES=bond0. AWS uses self,sm,tcp,cuda_copy,cuda_ipc. Other executor environment settings are explicit in the full profile JSON.

Environment bindings that still require review when moving hosts:

- The optional AWS allocator defaults to this environment's AMI, subnet and IAM instance profile, which have command-line overrides. Its Owner label is fixed to mahonem; another owner should adjust that label before creating resources.
- NVL native-read qualification uses `/usr/local/cuda-13.0`. Review both qualification and application library paths if changing the SDK layout.
- The AWS launcher uses master port 17744 and UI port 18190. Concurrent experiments on the same host require distinct ports.

### AWS host preparation

The validation AMI had local NVMe mounted at `/opt/dlami/nvme`; the dataset helper requires that path to be a separate mount point. Other AMIs require the host owner to prepare sufficient local storage first. The helper does not format disks automatically.

On an owned benchmark instance, run `scripts/prepare_aws_host.sh` as root. It installs dependencies and writes `/etc/modprobe.d/scaleup-reference-p2p.conf`:

```text
options nvidia NVreg_RegistryDwords="ForceP2P=17;RMForceP2PType=1;RMPcieP2PType=1" NVreg_DmaRemapPeerMmio=0
```

Reboot, then read `/proc/driver/nvidia/params` to verify the three RegistryDwords values and DmaRemapPeerMmio: 0 are effective. Both `nvidia-smi topo -p2p r` and `nvidia-smi topo -p2p w` must report OK for all 56 directed GPU pairs. The subsequent Spark queries verify their actual execution paths.

The validation driver was 595.91.07. Both cuda-nvrtc-13-1 and libnvjitlink-13-1 were version 13.1.115-1. The effective parameters, P2P matrices and package versions are recorded in `aws-host-prerequisites.json`.

The optional `scripts/aws_instance.py` supports allocate, status and terminate for one owned host. Its defaults belong to the retained us-east-2 environment and require appropriate AWS permissions. Allocation applies the saved setup, reboots and establishes an absolute shutdown deadline. Provisioning and termination are separate from benchmark submission. Preserve ownership receipts and release the instance, dedicated security group and imported public key after collecting evidence.

### NVL preparation

Use an authorized personal account on the Slurm controller and check current maintenance and reservations. The controller does not run Spark or compile native libraries. UCX can be prepared in a CPU-only allocation, from the executable source checkout root:

```bash
module load slurm/slurm/25.05
sbatch --partition=nvidia-part --nodes=1 --ntasks=1 --cpus-per-task=16 --mem=32G --time=00:20:00 \
  --wrap='bash "$SLURM_SUBMIT_DIR/benchmarks/scaleup_reference/scripts/build_ucx.sh" "$SLURM_SUBMIT_DIR/benchmarks/scaleup_reference/dependencies/ucx-arm64" /usr/local/cuda-13.0'
```

Set ucx_home in the deployment JSON to the resulting installation.

Container mounts must expose shared source/artifacts, the dataset, local working storage, `/dev/infiniband`, `/run/udev`, host CUDA/cuFile, `/etc/cufile.json` and the required `/dev/nvidia-fs*` devices. The validation used driver 580.105.08, GPFS at `/scratch`, and `nvcr.io#nvidia/cuda:13.1.0-base-ubuntu24.04`. `prepare_nvl_container.sh` lists the required userspace packages.

The launcher creates a per-run cuFile configuration, disables compatibility mode and cuFile I/O diagnostic statistics, and leaves the host file intact. cufile_stats=0 is unrelated to the catalog statistics required by the optimizer. Before Spark startup, each GPU executes both native host and strict GDS reads, compares all 16 MiB against a CPU read and checks cuFile runtime logs. All 32 GPUs must pass before the timed suite starts.

The controller checks artifacts and submission configuration; data, Java and native-library paths are checked inside the allocated compute nodes/container. The controller does not need the compute nodes' `/scratch` mount.

## Submit, inspect and stop

On the NVL controller, first load the site's Slurm module. On AWS, run on the prepared benchmark host.

```bash
python3 benchmarks/scaleup_reference/benchmark.py plan --platform nvl72 --deployment deployment.json
python3 benchmarks/scaleup_reference/benchmark.py validate --platform nvl72 --deployment deployment.json
python3 benchmarks/scaleup_reference/benchmark.py submit --platform nvl72 --deployment deployment.json --queries 2,9 --iterations 2
# Run the complete suite after focused validation passes.
python3 benchmarks/scaleup_reference/benchmark.py submit --platform nvl72 --deployment deployment.json
```

For AWS, replace nvl72 with aws48x. For binaries outside the build directory, provide `--engine /absolute/path/engine.jar --tools /absolute/path/benchmark-tools.jar`, keeping manifest.json beside the engine.

```bash
python3 benchmarks/scaleup_reference/benchmark.py status --run RUN_DIRECTORY
python3 benchmarks/scaleup_reference/benchmark.py collect --run RUN_DIRECTORY
python3 benchmarks/scaleup_reference/benchmark.py stop --run RUN_DIRECTORY
```

Each submission creates a fresh output directory. Stop uses the saved Slurm/PID ownership records for that run, rather than a broad process-name match. Terminate EC2 instances separately.

## Timing and cleanup contract

Each SQL timer starts at SQL submission/planning and ends after complete JVM collect. Application startup, table/statistics registration, validation and cleanup are outside that timer; validation and cleanup are separately recorded.

For each query, select the faster SQL execution and sum across 22 queries. The companion total adds cleanup from those same selected executions. It does not independently minimize cleanup. These sums are not application wall-clock time. No OS cache reset is performed.

Cleanup covers two resource owners:

- Driverless broadcasts: the separately applied upstream fix releases cached broadcast builds when normal UCX query-end reclaim frees their shuffle.
- Ordinary broadcasts: after complete SQL collection, no active Spark jobs and normal UCX reclaim acknowledgements, a barrier runs across all executors. It removes host broadcast blocks and Spark cache entries before releasing GPU/host resources, preventing later cache eviction from serializing a retired object.

This contract requires a fresh SQL action per execution and complete result collection. Concurrent queries, retained/reused DataFrames, cached RDDs and streaming iterators are outside the supported lifecycle. Retired payload reuse is rejected. This is a restricted experimental implementation related to #17019; a general product cleanup strategy remains separate work.

Every boundary retains executor acknowledgements, object counts, owned GPU-batch bytes and RMM allocation counters before/after cleanup. Cleanup seconds are retained and included in the companion total.

## Evidence and source entry points

| Evidence file | Contents |
| --- | --- |
| `qualification-summary.json` | Validation/GPU/cleanup checks, application IDs and totals for both complete runs |
| `per-query-results.csv` | Both raw execution times, selected iteration, SQL and corresponding cleanup |
| `nvl72-evidence.tar.gz` and `aws48x-evidence.tar.gz` | Complete configuration, per-query profiles, commands, results, logical/executed plans, installed statistics, GPU assignments, cleanup receipts, node records and service teardown; ephemeral service ownership tokens are omitted |
| `tested-deployments.json` | Actual validation paths, NVL container mounts and node selection |
| `artifact-locations.json`, `arm64-manifest.json`, `amd64-manifest.json` | Exact S3 objects, byte sizes, SHA-256 and embedded component/source revisions |
| `private-optimizer-provenance.json` | Verification that the pinned private commit exists in upstream and is an ancestor of upstream main |
| `aws-host-prerequisites.json` | Effective driver settings, P2P matrices and runtime package versions |
| `build-and-log-validation.json` and `verification-logs.tar.gz` | Build/regression checks and formal driver-log audit |
| `attachment-checksums.json` | SHA-256 of the compact evidence files |

| Executable source entry point | Purpose |
| --- | --- |
| [benchmark.py](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/benchmark.py) | Plan, validate, submit, inspect, collect and stop |
| [configs/](https://github.com/binmahone/spark-rapids/tree/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/configs) | Complete profiles, explicit query overrides and dependency lock |
| [inputs/](https://github.com/binmahone/spark-rapids/tree/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/inputs) | SQL, frozen statistics and validation references |
| [scripts/build.sh](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/build.sh) | Build the public engine, private optimizer and tools; selected regression checks |
| [scripts/artifact_manifest.py](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/artifact_manifest.py) | Artifact hashing and embedded revision inspection |
| [scripts/build_ucx.sh](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/build_ucx.sh) | Complete patched UCX installation and interval-tree tests |
| [scripts/prepare_spark.sh](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/prepare_spark.sh) | Dedicated vanilla Spark 3.5.3 installation |
| [scripts/prepare_aws_host.sh](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/prepare_aws_host.sh) | AWS driver and userspace prerequisites |
| [scripts/prepare_aws_data.sh](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/prepare_aws_data.sh) | AWS data staging and verification |
| [scripts/aws_instance.py](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/aws_instance.py) | Optional owned AWS resource lifecycle |
| [scripts/run_nvl.slurm](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/run_nvl.slurm) | NVL Slurm and container launch |
| [scripts/run_node.sh](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/run_node.sh) | Container setup and per-node execution |
| [scripts/prepare_nvl_container.sh](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/prepare_nvl_container.sh) | NVL container userspace preparation |
| [scripts/node.py](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/node.py) | Node preflight and per-run cuFile configuration |
| [scripts/qualify_nvl.py](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/qualify_nvl.py) | Per-GPU native host and strict GDS byte comparison |
| [scripts/services.py](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/services.py) | NUMA/GPU worker placement and ownership-based service lifecycle |
| [scripts/cohort.py](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/cohort.py) | Application lifecycle, native library selection and score collection |
| [scripts/run_sql.py](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/run_sql.py) | SQL execution, statistics, validation, timing and cleanup |
| [tools/src/main/java/](https://github.com/binmahone/spark-rapids/tree/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/tools/src/main/java) | Source of all benchmark helper classes |

Validation included 427 passing private optimizer tests, five canceled cases requiring Spark 4/4.1 and one ignored case; 38 CPU lifecycle tests on each architecture; ten Python contract tests; one real GPU broadcast-retirement test on AWS; and 27 interval-tree tests per native UCX build. The supported runtime is vanilla Spark 3.5.3. Qualification of other Spark shims, latest main or every upstream test combination is outside this record.

A formal score requires all 44 requested samples to complete and validate, positive bytes on the intended Hybrid path, zero unexpected GDS fallback files, acknowledgements from all executors, no reported UCX reclaim failures and successful service teardown. Slurm COMPLETED or a successful Spark application alone is insufficient.

The compact evidence contains the configuration, commands, results, plans and acknowledgements needed to assess these runs. It excludes raw event logs and large temporary reference files. Audited AWS driver logs are retained under the same private S3 source prefix at `evidence/aws-retained-driver-logs.tar.gz`, SHA-256 `70459fe76ed4ceb9f4db5b58400d00fe33cbe52e4147ffb922f532d82c7eb8f8`. NVL driver logs remain under the validation run directory on its controller. Build caches were removed after preserving artifacts and compact evidence; they are not reproduction prerequisites.
