# Scale-Up cuDF-Spark reference benchmark

This directory contains the source, configuration, SQL, statistics, validation
references and launch scripts for two fixed engineering experiments:

| Platform | Workload | GPUs | Reader | Application lifecycle |
| --- | --- | --- | --- | --- |
| NVL72 | TPC-H SF30000 | 8 nodes, 32 B200 | Hybrid MULTITHREADED, GDS | One application for all timed queries |
| AWS | TPC-H SF10000 | One g7e.48xlarge, 8 GPUs | Hybrid MULTITHREADED, native host reads | One application for all timed queries |

These are experimental reference configurations, not default product settings or
official TPC benchmark results. New artifacts must receive their own validation
and timings. The earlier 157.224 s and 454.450 s results do not qualify this build.

## Source and dependency contract

Use the exact source commit linked by the accompanying validation record. Start
with a clean checkout of `binmahone/spark-rapids`, not an old experiment directory.
The AWS source base is upstream main `bcbaa76b5c02313fe36ead400c2949834396ec49`,
with the restricted ordinary-broadcast retirement API from the reference benchmark.
Driverless cache reclaim #17016 is already in this upstream base. No optimizer or
driverless-admission prototype is added. The retained NVL profile and inputs remain
reference material; its GDS run uses the separately pinned November 23 source,
because MULTITHREADED GDS is not in this upstream main.

`configs/sources.lock.json` records component revisions. JNI is the official
26.12 snapshot `26.12.0-20261124.043801-53`, with `cuda13-arm64` for NVL and `cuda13`
for AWS. No `NativeReaderBinding` class or auxiliary native file-reader library
is added to the engine. Private optimizer source remains an external prerequisite:
NVIDIA GitLab access to the pinned repository and commit is required.

Each build must have one physically independent Maven repository. The build
defaults to `.build/maven-repository`; do not share it with another build session.
Use Java 17. `BUILD_JAVA_HOME`, `MAVEN_REPOSITORY` and `PRIVATE_SOURCE` can select
explicit local locations; `PRIVATE_SOURCE` must be the clean pinned checkout.

```bash
git clone --branch benchmark/aws-main-single-session-261125 https://github.com/binmahone/spark-rapids.git
cd spark-rapids
# Checkout the exact commit linked by the validation record before building.
bash benchmarks/scaleup_reference/scripts/build.sh arm64
# For the AWS artifact, build separately with the same isolated Maven repository:
bash benchmarks/scaleup_reference/scripts/build.sh amd64
```

The final public build compiles the test sources and runs the selected CPU lifecycle
tests; it does not automatically schedule native/GPU test suites on the builder.
Private optimizer tests run
before it. GPU test execution is separate and must use an available allocated
GPU. `IndependentBroadcastRetirementGpuSuite` verifies that a real GPU broadcast
allocation is released. `IndependentBroadcastRetirementSuite` rejects reuse of a
retired object and checks idempotence. Removing the retirement or its reuse guards
must make these regressions fail.

Build output is `.build/artifacts/<architecture>/`, containing the engine,
`benchmark-tools.jar`, and `manifest.json`. The manifest includes SHA-256, embedded
component revisions and source state. A shared validated artifact must come from
a clean committed checkout.

## Existing data

The experiments assume access to these existing datasets. They do not generate
replacement data or claim equivalence with another dataset at the same scale.

| Platform | Path | Numeric representation |
| --- | --- | --- |
| NVL | `/scratch/prestouser/tpch-rs-float-no-delta/scale-30000/` | Native double columns |
| AWS | `/opt/dlami/nvme/daily-trend-hardened/dataset/` | Native decimal(15,2) columns |

All eight tables must be readable on the runtime hosts. For an empty owned AWS
instance, `scripts/prepare_aws_data.sh` downloads the existing source from
`s3://haozhu-nf-us-east-2/TPC-H-sf10k-decimal/`; it requires an authorized AWS role
and enough local NVMe space. Downloads use a staging directory and publish the
complete directory only after verification. Existing data is verified without
being overwritten. Preflight rejects visible download leftovers in table directories
and checks file counts, sizes and Parquet magic. The source dataset contains 1,538 Parquet files,
3,712,454,195,832 bytes. Existing file layout and lineitem order-key clustering are
prerequisites of these fixed experiments; arbitrary regenerated data is outside
this reference. Runtime records retain table schemas and file counts.

Statistics are required. `inputs/<platform>/catalog-statistics.json` is read-only
and installed in the Spark catalog, then row counts, NDVs and null counts are
read back. NVL statistics originated from retained Presto ANALYZE; AWS statistics
come from measured Parquet footers and approximate NDVs, including the measured
null counts used in the full-stats experiment. No key constraints are injected.

## Explicit query exceptions

| Platform/query | Setting | Reason |
| --- | --- | --- |
| Both, Q13 | 1 GiB scan splits | Avoid the retained large-string overflow case |
| AWS Q13 | Scan partition cap 8192 | Paired with its smaller split |
| NVL Q15 | Cast `l_extendedprice` and `l_discount` to decimal(15,2) in a temporary view | Preserve the retained Q15 definition's exact aggregate equality |

AWS uses four Spark task slots per executor for every query. Scan settings are
mutable SQL settings: Q13 uses 1 GiB/8192 partitions, then the runner restores
2 GiB/4096 for the following query. The historical November 23 AWS run used
separate query applications and a two-slot Q18 exception; its timings are not
an isolated software comparison with this single-session protocol.

NVL Q15 casts execute in the timed SQL and the view is removed afterward. AWS
already stores those columns as decimal, so it needs no such view. These are
configuration/workload exceptions, not query-number branches in optimizer code.

## Prepare the runtime

Provide a deployment JSON containing explicit machine paths for `spark_home`,
`java_home`, `ucx_home`, and `scratch_root`. It changes deployment locations only;
it cannot override the reference workload settings. Spark must be vanilla 3.5.3,
with no cuDF/RAPIDS/Gluten/Flux JARs installed in its `jars` directory.
`scripts/prepare_spark.sh` installs a dedicated Spark 3.5.3 Python environment.

Build UCX 1.22.0 with the included interval-search patch, using
`scripts/build_ucx.sh INSTALL_PREFIX CUDA_HOME`. The build requires a C/C++
toolchain, autoconf, automake, libtool, libnuma development files and CUDA headers.
The included patch targets 1.22.0; the lock file identifies its upstream equivalent
and SHA-256. The build runs the interval-tree tests. No separate `libucs` overlay is used.
The runtime explicitly selects this installation's libraries and module directory.
Use a complete CUDA SDK path explicitly when building UCX, such as
`/usr/local/cuda-13.0` on the retained AWS AMI. Installing CUDA 13.1 runtime
packages can point `/usr/local/cuda` at a directory without development headers.

On NVL, the deployment also names `container_image`, `container_mounts`, and
optionally `container_setup`. Its mounts must expose the source/runtime directory,
the dataset, node-local storage, `/dev/infiniband`, `/run/udev`, `/etc/cufile.json`,
host CUDA/cuFile libraries, and the required `/dev/nvidia-fs*` devices. The saved
`scripts/prepare_nvl_container.sh` lists container userspace dependencies. cuFile
configuration is copied into a per-run file, compatibility mode is disabled and
the host configuration is not changed. Slurm access and the cluster's current
maintenance/reservation rules apply; run no Spark workload on the controller.

On an owned AWS instance, `scripts/prepare_aws_host.sh` installs dependencies and
writes the explicit P2P driver settings. It does not reboot automatically. Reboot
before validation. Both P2P read and write matrices must show all 56 directed
GPU pairs as supported. A capability matrix is not a SQL transfer measurement;
the subsequent Spark run must also execute and validate its actual data path.

## Plan, validate, run and stop

Use the architecture-specific engine and tools from the same manifest. The
following commands are run on the NVL controller or the AWS benchmark host:

```bash
python3 benchmarks/scaleup_reference/benchmark.py plan --platform nvl72 --deployment deployment.json
python3 benchmarks/scaleup_reference/benchmark.py validate --platform nvl72 --deployment deployment.json
python3 benchmarks/scaleup_reference/benchmark.py submit --platform nvl72 --deployment deployment.json
```

The NVL controller checks artifacts and submission configuration; dataset, Java,
UCX and other container paths are checked on the allocated nodes. The controller
does not need the compute nodes' `/scratch` mount or Java installation.

For AWS replace `nvl72` with `aws48x`. Use `--engine /absolute/path/engine.jar` and
`--tools /absolute/path/benchmark-tools.jar` when using a downloaded artifact
instead of local build output. Keep its `manifest.json` beside it.

Start qualification with `--queries 2,9 --iterations 2`, then run all 22 queries
without `--queries`. A focused run is diagnostic and cannot produce a full-suite
reference score. The runner saves a unique output directory and prints its path.

```bash
python3 benchmarks/scaleup_reference/benchmark.py status --run RUN_DIRECTORY
python3 benchmarks/scaleup_reference/benchmark.py collect --run RUN_DIRECTORY
python3 benchmarks/scaleup_reference/benchmark.py stop --run RUN_DIRECTORY
```

`stop` uses this run's owner/job/PID receipts. It does not kill processes by a
broad Spark name. Resource provisioning and instance termination are separate
explicit operations; submitting a query does not create or terminate EC2 instances.
`scripts/aws_instance.py` provides optional `allocate`, `status` and `terminate`
commands for one owned host. Its AMI, subnet and IAM profile defaults are specific
to the retained us-east-2 environment; colleagues must have authorized access.
Allocation sets an absolute shutdown deadline, installs the saved host preparation
script and reboots to apply the P2P settings. Keep its ownership receipts and
release its dedicated security group and imported public key after termination.

## Timing, validation and cleanup

Each timed action starts at `spark.sql` and ends after complete JVM `collect`.
Startup, schema/statistics registration, result validation and query-boundary
cleanup are outside that SQL timer. Every sample records SQL, validation and
cleanup time separately. Summaries report per-query best-of-two SQL and the
SQL-plus-cleanup time of that same selected sample. These sums are not application
wall time and do not represent a cold-cache experiment. No OS cache reset occurs.

Validation defaults ON. `--no-validate` is for diagnostics only and labels outputs
unvalidated and ineligible for a reference score. One-pass runs, DEBUG metrics,
dirty source and mismatched source/artifact revisions are also ineligible. Small outputs are compared in
full; large outputs use complete multiset fingerprints. AWS Q3/Q18 validate legal
LIMIT ties. NVL Q11 receives a separately executed non-hybrid GPU reference and
complete key/value comparison. This is not an independent CPU correctness oracle.

The only ordinary-broadcast cleanup API is explicitly enabled by the reference
profile. The runner creates a fresh SQL action each time, collects all results,
waits for normal UCX/driverless reclaim acknowledgements, then retires ordinary
broadcast resources across all executors. Its host broadcast blocks and cache entries
are removed before retirement, so later Spark eviction cannot serialize cleared data.
Resource cleanup uses typed calls and a
Spark 3.5.3 package bridge, with no reflective access to private resource fields.
The Python runner locates the Scala boundary through the plugin's standard shim
classloader. Concurrent queries,
retained/reused DataFrames, cached RDDs and iterator results are unsupported.
Retired broadcast reuse fails explicitly. This is a restricted experimental
implementation related to #17019, not a completed general-purpose product fix.

## Evidence to retain and publish

Keep `config.json`, `profiles.json`, `owner.json`, submission records,
`summary.json`, application `result.json`, launch commands, collected plans,
cleanup receipts, driver logs, node preflight and artifact manifests. Keep raw
event logs, complete Q11 binary references, build trees and dependency caches in
working storage. Publish only compact evidence required to interpret a result.

Before sharing a result, verify every requested sample completed, validation
passed, the intended Hybrid host/GDS path recorded positive bytes, no GDS fallback
file was reported, normal UCX reclaim reported no failure, and no owned services
remain after completion. Repeat-use GPU validation is mandatory for this new build.
