# NVL72 与 AWS 48x 标准配置及复现指南（2026-11-23）

这套目录提供同一个入口，分别运行 NVL72 SF30000 和 AWS g7e.48xlarge SF10000 两套 cuDF-Spark 实验。代码、SQL、配置、固定统计快照、结果校验参考和提交脚本都可以从固定提交取得；数据继续使用现有环境中的同一份文件。

两边的数据规模、数值类型、存储和读取方式不同。下面的总时间分别对应这两套实验，不能直接相除得到硬件加速比。这是工程参考配置，不是官方 TPC 成绩。

复用范围是下面列出的两套既有环境。统一入口已经移除了对旧 session 脚本和手工修改 JAR 的依赖；查询特例、统计加载和清理边界都明确写在配置及文档中。迁移到其他环境时，仍需调整部署路径和基础设施设置。

## 从哪里开始

代码入口：[benchmarks/scaleup_reference](https://github.com/binmahone/spark-rapids/tree/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference)，固定提交 `faa47fe51f998ae97385e2b218be4a261006bfa6`。目录内也有英文 [README](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/README.md)。

| 项目 | NVL72 | AWS |
| --- | --- | --- |
| 机器 | 8 个 GB200 节点，每节点 4 个 GPU，共 32 GPU | 一台 g7e.48xlarge，8 个 RTX PRO 6000 Blackwell Server Edition GPU |
| 数据 | TPC-H SF30000 | TPC-H SF10000 |
| 数据路径 | `/scratch/prestouser/tpch-rs-float-no-delta/scale-30000` | `/opt/dlami/nvme/daily-trend-hardened/dataset` |
| 数值列 | 原生 double | 原生 decimal(15,2) |
| Spark | 原版 3.5.3 | 原版 3.5.3 |
| 读取方式 | Hybrid MULTITHREADED，使用 GDS | Hybrid MULTITHREADED，使用原生主机内存读取 |
| Application | 计时查询共用一个 application | 每条查询一个 application |
| 每条查询次数 | 完整执行两遍 | 完整执行两遍 |

GDS 是 GPUDirect Storage，即把文件数据读入 GPU 缓冲区。AWS 配置把压缩数据读入主机内存后再交给 GPU 处理。运行时会检查读取指标，确认实际经过了对应路径。

## 实测验证结果

以下是 **2026-11-23 北京时间**使用本页固定提交、全新构建产物跑出的完整成绩。

| 指标 | NVL72 SF30000 | AWS SF10000 |
| --- | --- | --- |
| 完成并通过校验 | 22 条 × 2 轮，44/44 | 22 条 × 2 轮，44/44 |
| 每条查询取两轮较快 SQL 时间后求和 | **154.665 秒** | **447.041 秒** |
| 同一批所选样本的 SQL + 清理时间之和 | **171.848 秒** | **467.840 秒** |
| 固定第二轮 SQL 时间之和 | 156.726 秒 | 447.041 秒 |
| 实际读取指标 | 44/44 样本有正的 GDS device bytes，fallback files = 0 | 44/44 样本有正的 native host bytes，fallback files = 0 |
| 清理 executor 确认 | 44 × 32 = 1,408 | 44 × 8 = 352 |
| 查询服务退出记录 | 8/8 节点完成 | 22/22 application 完成 |

NVL Slurm job 为 `25453`，状态 `COMPLETED / 0:0`；计时 application 为 `app-20261123042758-0001`，额外 Q11 参考为 `app-20261123042724-0000`。AWS 的 22 个 application ID 见 `qualification-summary.json`，验证实例为 `i-0751e3edbfd4c95a6`（us-east-2c，运行后释放）。

下面单位均为秒；“含清理”采用该行较快 SQL 所在的同一次样本。完整两轮原始时间见 `per-query-results.csv`。

| 查询 | NVL SQL | NVL 含清理 | AWS SQL | AWS 含清理 |
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

正式 driver 日志审计覆盖 NVL 的 2 个 application 和 AWS 的 22 个 application，未发现 ERROR、任务失败、UCX reclaim 失败或原生致命错误匹配。NVL 的 32 个 GPU 还各自完成 host/GDS 两条原生读取路径的 16 MiB 全字节校验，共 64 条记录。

普通广播清理记录实际退休了 NVL 的 836 个对象、AWS 的 244 个对象；所有清理记录中的 RMM 分配计数均未增加。这证明本次实验执行了清理路径，不构成对所有并发或复用场景的支持证明。

## 固定的软件版本

| 组件 | 固定版本或提交 |
| --- | --- |
| 公共代码 base | Paul 的 `hybrid_scaleup@d64ace4eba1f0a49c2cd4ea89644dfca8892140e` |
| Driverless broadcast 清理 | 单独移植上游 [#17016](https://github.com/NVIDIA/cudf-spark/pull/17016)，提交 `9a854c8d312eb36c2fc0a0760eaa80a0217f8243` |
| 普通广播清理 | 此参考分支中的独立查询清理接口，问题范围对应 [#17019](https://github.com/NVIDIA/cudf-spark/issues/17019) |
| JNI：Java 与原生代码之间的绑定组件 | 官方 `26.12.0-20261122.042214-52`，提交 `bf63ec567eabc0bcac5366f6afba0b0bba40f30a` |
| cuDF | `3c92c2094dae1931ef7c2c19ee932cc26822a000` |
| 私有优化器 | [mahonem/cudf-spark-private](https://gitlab-master.nvidia.com/mahonem/cudf-spark-private)，提交 `61e9e96803771371dec116593f0c4eced9ac44d8` |
| UCX | 1.22.0，加目录中附带的 interval-tree 搜索补丁；上游对应 [#12367](https://github.com/openucx/ucx/pull/12367) |

这套版本没有合并最新 public main。NVL 使用 `cuda13-arm64` 原生依赖，AWS 使用 `cuda13`。构建私有优化器需要 NVIDIA GitLab 对应仓库的访问权限。

以下旧做法已移除：替换 `NativeReaderBinding.class`、附加 `libforecast_native_fileio.so`、修改 JAR 来替换 native-reader 绑定、依赖额外的广播清理 JAR，以及引用旧实验目录中的脚本。普通广播清理直接编入引擎。

小的 `benchmark-tools.jar` 单独提供统计加载、结果校验和原生读取预检工具。统计加载是必须的；结果校验可以显式关闭。它不替换官方 JNI 的实现。

UCX 补丁编入完整安装目录，不再单独覆盖 `libucs`。补丁及其 SHA-256 位于 `patches/` 和 `configs/sources.lock.json`。该补丁适配 1.22 的树结构，没有把 UCX 升级为 1.24。

## 配置，以及刻意保留的例外

完整配置：[nvl72.json](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/configs/nvl72.json)、[aws48x.json](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/configs/aws48x.json)。`plan` 会展开每条查询最终使用的配置。

| 配置 | NVL72 | AWS |
| --- | --- | --- |
| Executor 数量 / 每个 executor 核数 | 32 / 8 | 8 / 8 |
| `spark.task.resource.gpu.amount` | 0.25，即每 GPU 4 个 Spark task slot | 通常 0.25；Q18 为 0.5，即 2 个 slot |
| `spark.rapids.sql.concurrentGpuTasks` | 5，dynamic 开启 | 2，dynamic 开启 |
| 每 executor pinned pool | 64 GiB | 16 GiB |
| 默认 scan split | 4 GiB | 2 GiB |
| Scan 分区数量上限 | 不设置 | 4096 |
| Shuffle 分区数 | 128 | 256 |
| Shuffle | UCX，不压缩（`none`） | UCX，LZ4 |
| GDS mode | `WAIT_IF_GPU_BUSY` | `OFF` |
| Metrics level | `MODERATE` | `MODERATE` |

Spark task slot 控制同时存在的 Spark task；`concurrentGpuTasks` 控制进入 GPU 工作阶段的并发。这是两层控制。NVL 保留的值 5 不表示每 GPU 能同时调度 5 个 Spark task，Spark task slot 仍是 4。

以下例外直接写在配置中：

- 两边 Q13 都使用 1 GiB split，规避保留实验中出现的大字符串溢出；AWS Q13 的 scan 分区上限同时设为 8192。
- AWS Q18 使用两个 Spark task slot。这是保留的查询调优选择。Task 资源在 application 启动时确定，因此 Q18 必须使用对应的独立 application；AWS 参考流程统一为每条查询启动一个 application。每个 application 内仍执行两遍，并在两遍之间清理资源。Application 启动时间不计入 SQL 时间。
- NVL Q15 用临时 view 将 `l_extendedprice`、`l_discount` 转成 decimal(15,2)，保持该查询定义中的精确聚合相等比较。Cast 在计时 SQL 中执行，之后移除 view。AWS 文件本身已是 decimal，不需要这个 view。

为保持本次实验的缓存和执行顺序，默认查询顺序也固定在 JSON 中：

- NVL：9, 21, 3, 18, 17, 1, 2, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 19, 20, 22。
- AWS：3, 1, 2, 4, 5, 6, 9, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22。

两套配置都关闭 AQE、speculation 和动态 executor 分配。优化器、内存和各项读取/UCX 参数以完整 JSON 为准。

## 数据、统计和结果校验

这两套实验使用上面的既有数据路径，要求沿用相同的 schema、文件和 row-group 布局。重新生成相同 scale factor 的数据，应当作为另一次实验。

新建 AWS 实例没有数据时，可用 `scripts/prepare_aws_data.sh` 从 `s3://haozhu-nf-us-east-2/TPC-H-sf10k-decimal/` 下载，需要有权限的 AWS 身份和足够的本地 NVMe 空间。该数据集有 1,538 个 Parquet 文件，共 3,712,454,195,832 字节。

下载脚本先写入独立 staging 目录，验证完整后再切换为正式目录。已有数据只验证，不覆盖。预检会拒绝表目录中 Spark 可能误读的临时文件，检查文件数量、大小以及 Parquet 文件头尾标记；不计算整个数据集的完整内容哈希。

统计快照固定在 `inputs/<platform>/catalog-statistics.json`。入口把行数、不同值数量、空值数量和可用的上下界加载到 Spark catalog，随后读回行数和列统计进行核对。它们用于优化器估算。NVL 快照来自保留的 Presto ANALYZE；AWS 来自 Parquet footer 测量及近似不同值数量，并包含测得的空值数量。没有注入主键或外键约束。

结果校验默认开启：

- 小结果完整比较；大结果使用覆盖全部行的 multiset 指纹。
- AWS Q3/Q18 单独处理 LIMIT 边界上合法的并列结果；Q18 使用完整候选行参考校验成员和排序条件。
- NVL Q11 先额外运行一次关闭 Hybrid 的 GPU 参考执行，再按完整 key/value 比较所有结果，使用约定的数值容差。它与 GPU 软件栈共享实现，不能当成独立 CPU 正确性证明。

`--no-validate` 适合排查问题。关闭校验、只跑一遍、只跑查询子集、启用 DEBUG metrics、代码未提交或源码/产物版本不匹配，都不能列为正式参考成绩。

## 获取代码和 JAR

```bash
git clone --branch benchmark/261123-scaleup-reference https://github.com/binmahone/spark-rapids.git
cd spark-rapids
git checkout faa47fe51f998ae97385e2b218be4a261006bfa6
```

已经验证的两套产物保存在同一个 AWS 账户的私有 S3 路径中。每套包含引擎、`benchmark-tools.jar` 和 `manifest.json`，请整套获取。

访问前提：AWS 身份需要对证据文件 `artifact-locations.json` 中列出的对象拥有 `s3:GetObject` 权限，region 为 `us-east-1`。拥有数据桶权限不代表拥有这个产物桶的权限；本次 EC2 默认 instance role 就不能列举此产物桶。可用有权读取的身份下载，再放到运行环境的共享目录；也可按下文从源码构建。不需要修改 IAM 或公开 bucket。

| 架构 / 文件 | SHA-256 |
| --- | --- |
| `arm64 / benchmark-tools.jar` | `4ee01fa57115eeb6e59b52eb27ef26c3a5994dd57947f1bc72be38828c52fa90` |
| `arm64 / cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13-arm64.jar` | `f99002a23ffc885d8593039d3c6ccb8359c288a730cf2cf65db10d71c5952ded` |
| `arm64 / manifest.json` | `a1958779b5d4bc27c9d86936d46f4dcd0b3b481aa2c5cab12aa44cf944842e7a` |
| `amd64 / benchmark-tools.jar` | `ba687e884685d6a2438910b161562e9d8dc35cd583e27339f8fc76243cd1eb9f` |
| `amd64 / cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar` | `02058b8a21475075ea77ede99487b89e36424ee6518856685ca5dcaf2c98cb9e` |
| `amd64 / manifest.json` | `a312330ebaf5d8fda4f34e7f1ccdc29ed19cd8446089c7598fa3d7f2a46f9826` |

固定产物前缀：

- `arm64`：`s3://mahonem-wild-sf10k-261012-503960270539-use1/scaleup-reference/261123/01a20622-0ca8-74d0-9805-6bc842a9fdb3/faa47fe51f99/arm64/a1958779b5d4/`
- `amd64`：`s3://mahonem-wild-sf10k-261012-503960270539-use1/scaleup-reference/261123/01a20622-0ca8-74d0-9805-6bc842a9fdb3/faa47fe51f99/amd64/a312330ebaf5/`

下载本页 `artifact-locations.json` 后，在干净 checkout 根目录执行以下命令；NVL 选 `arm64`，AWS 选 `amd64`。脚本使用精确对象 URI，避免依赖列举 bucket 的权限，并核对所有文件哈希。

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


也可以从源码构建。使用 Java 17；私有优化器源码必须是上面固定的干净提交。

```bash
bash benchmarks/scaleup_reference/scripts/build.sh arm64
bash benchmarks/scaleup_reference/scripts/build.sh amd64
```

构建默认使用独立的 `.build/maven-repository`，不要与其他构建任务共享可写 Maven repository。`BUILD_JAVA_HOME`、`MAVEN_REPOSITORY`、`PRIVATE_SOURCE` 可以指定自己的路径。

输出在 `.build/artifacts/<architecture>/`：引擎 JAR、`benchmark-tools.jar` 和 `manifest.json`。Manifest 包含 SHA-256、内嵌组件版本、源码提交和工作区状态，三者应当一起保存。重新构建会产生自己的文件哈希，也需要自己的运行验证。

## 准备运行环境

部署启动还有以下会影响性能的行为，均由保存的脚本实现：

- 每个 GPU 单独启动一个 Spark worker，按 GPU UUID 设置 `CUDA_VISIBLE_DEVICES`，在该 worker 内只暴露逻辑 GPU 0。实际 barrier 分配记录验证了 NVL 32 个、AWS 8 个不同的 GPU UUID。
- CPU 绑定到 GPU 对应的 NUMA 节点。NVL 使用 `numactl --cpunodebind=<node> --membind=<node>`；AWS 使用 `--cpunodebind=<node> --preferred=<node>`。NUMA 节点从 GPU PCI 设备的 sysfs 信息读取。
- Worker 宣告的 cores/memory 为 NVL 32/350g、AWS 24/200g；每个 executor 的 8 cores 和内存仍由参考 Spark 配置决定，`spark.cores.max` 限制整个 application 的核数。
- `LD_LIBRARY_PATH` 首先选择本次构建的 UCX，然后选择 CUDA 13.1 的库目录；NVL 再加入 host CUDA 13.0 的 cuFile 目录。`UCX_MODULE_DIR` 固定指向同一次 UCX 安装的 `lib/ucx`。实际传给 driver/executor 的路径在各 application 的 `profile.json` 中。
- NVL 使用 `UCX_TLS=^posix`、`UCX_NET_DEVICES=bond0`；AWS 使用 `self,sm,tcp,cuda_copy,cuda_ipc`。其余 executor 环境变量都写在完整配置 JSON 内，不需要依赖 shell 历史。

Spark 必须是原版 3.5.3，其 `jars/` 中没有预装 RAPIDS、cuDF、Gluten 或 Flux 引擎 JAR。可用 `scripts/prepare_spark.sh` 准备独立的 Spark 3.5.3 Python 环境。

使用 `scripts/build_ucx.sh INSTALL_PREFIX CUDA_HOME` 构建 UCX。CUDA_HOME 应明确指向完整 SDK，例如当前 AWS AMI 中的 `/usr/local/cuda-13.0`。安装 CUDA 13.1 运行库后，`/usr/local/cuda` 可能指向缺少开发头文件的目录，因此不应依赖这个默认软链接。

部署 JSON 指定机器路径，例如：

```json
{
  "spark_home": "/your/shared/path/to/pyspark",
  "java_home": "/usr/lib/jvm/java-17-openjdk-arm64",
  "ucx_home": "/your/shared/path/to/ucx",
  "scratch_root": "/var/tmp/scaleup-reference"
}
```

AWS 用对应的 amd64 Java 路径，scratch 使用本地 NVMe。NVL 还需要明确的 `container_image`、`container_mounts`，以及可选 `container_setup`。部署文件选择机器位置、Slurm/container 和节点；工作负载参数位于参考配置 JSON。

仍保留的环境绑定需要显式核对：

- 可选 AWS 建机脚本默认使用本次环境的 AMI、subnet 和 IAM instance profile，可通过参数覆盖；资源的 `Owner` 标签固定为 `mahonem`，其他使用者创建资源前应修改这个标签。
- NVL 原生读取预检使用 `/usr/local/cuda-13.0`；application 默认优先加载 CUDA 13.1 运行库，NVL 再加入 CUDA 13.0 库目录。换 SDK 布局时需要同时检查预检和 application 的库路径。
- AWS 提交入口固定使用 master 端口 `17744` 和 UI 端口 `18190`。同一主机同时运行多套实验时需要先分配不同端口。

### AWS

本次 AMI 已将本地 NVMe 挂载在 `/opt/dlami/nvme`；数据脚本要求这个路径确实是独立挂载点。其他 AMI 需要先由机器使用者准备足够空间的本地存储，脚本不会自动格式化磁盘。

在自己创建并管理的 benchmark 实例上执行 `scripts/prepare_aws_host.sh`，安装依赖并写入 NVIDIA driver 的 P2P 设置；重启后再验证。`nvidia-smi topo -p2p r` 和 `-p2p w` 都应显示全部 56 对有向 GPU 配对可用。这是能力检查，之后仍需执行并校验实际 Spark 查询。

可选的 `scripts/aws_instance.py` 提供 `allocate`、`status`、`terminate`。其默认 AMI、subnet、IAM instance profile 属于本次 us-east-2 环境，使用者需要相应权限。它会安装保存的 host 准备脚本、重启，并设置绝对关机期限。创建/销毁实例与提交 benchmark 是分开的操作；运行结束后保留证据，并释放该任务自己的实例、security group 和导入的公钥资源。

本次 AWS NVIDIA driver 为 `595.91.07`；准备脚本写入 `/etc/modprobe.d/scaleup-reference-p2p.conf`：

```text
options nvidia NVreg_RegistryDwords="ForceP2P=17;RMForceP2PType=1;RMPcieP2PType=1" NVreg_DmaRemapPeerMmio=0
```

重启后读取 `/proc/driver/nvidia/params`，确认 `RegistryDwords` 三项值和 `DmaRemapPeerMmio: 0` 真正生效，再检查两个 P2P 矩阵。本次已读回这些参数，读、写各 56 对均为 OK；完整记录见 `aws-host-prerequisites.json`。本次安装的 `cuda-nvrtc-13-1`、`libnvjitlink-13-1` 均为 `13.1.115-1`。

### NVL

使用自己的正常授权接入方式登录 Slurm controller，检查维护安排和预约。Controller 不运行 Spark，也不编译原生库。UCX 可以在不申请 GPU 的 CPU 构建任务中准备；从代码根目录执行：

```bash
module load slurm/slurm/25.05
sbatch --partition=nvidia-part --nodes=1 --ntasks=1 --cpus-per-task=16 --mem=32G --time=00:20:00 \
  --wrap='bash "$SLURM_SUBMIT_DIR/benchmarks/scaleup_reference/scripts/build_ucx.sh" "$SLURM_SUBMIT_DIR/benchmarks/scaleup_reference/dependencies/ucx-arm64" /usr/local/cuda-13.0'
```

将构建得到的 UCX 目录填入 deployment。

挂载必须覆盖共享源码和产物、数据、本地工作存储、`/dev/infiniband`、`/run/udev`、host CUDA/cuFile、`/etc/cufile.json`、`/dev/nvidia-fs*`。本次节点 NVIDIA driver 为 `580.105.08`，数据挂载为 GPFS `/scratch`；使用 `nvcr.io#nvidia/cuda:13.1.0-base-ubuntu24.04`；`prepare_nvl_container.sh` 列出所需 userspace 包。

入口创建每次运行自己的 cuFile 配置，关闭 compatibility mode 和 cuFile I/O 诊断统计，不修改 host 配置。`cufile_stats=0` 与用于查询优化的 catalog 统计快照是两回事；catalog 统计仍然必须加载。启动 Spark 前，会在每个 GPU 上分别走原生 host 和严格 GDS 路径，对完整的 16 MiB 内容与 CPU 读取结果逐字节比较，并检查 cuFile 运行日志。32 个 GPU 的预检都通过后才执行查询。

NVL controller 只检查产物和提交配置；数据、Java 和原生库路径在计算节点/container 内检查，controller 无须具有计算节点的 `/scratch` 挂载。

本次实测 deployment、展开配置、命令、节点和 application ID 位于本页链接的证据文件。里面的个人目录是本次验证的实际部署位置；复现时替换为自己能访问、所有运行节点均可见的路径。

## 提交、查看和停止

在 NVL controller 上先加载站点 Slurm module；AWS 在准备好的实例上执行。

```bash
python3 benchmarks/scaleup_reference/benchmark.py plan --platform nvl72 --deployment deployment.json
python3 benchmarks/scaleup_reference/benchmark.py validate --platform nvl72 --deployment deployment.json
python3 benchmarks/scaleup_reference/benchmark.py submit --platform nvl72 --deployment deployment.json --queries 2,9 --iterations 2
# After the focused validation passes:
python3 benchmarks/scaleup_reference/benchmark.py submit --platform nvl72 --deployment deployment.json
```

AWS 将 `nvl72` 换为 `aws48x`。使用构建目录之外的产物时，显式提供 `--engine /absolute/path/engine.jar --tools /absolute/path/benchmark-tools.jar`，并将 `manifest.json` 放在引擎旁边。

```bash
python3 benchmarks/scaleup_reference/benchmark.py status --run RUN_DIRECTORY
python3 benchmarks/scaleup_reference/benchmark.py collect --run RUN_DIRECTORY
python3 benchmarks/scaleup_reference/benchmark.py stop --run RUN_DIRECTORY
```

每次提交都会生成新的运行目录。停止命令只使用该次运行保存的 Slurm/PID 所有权记录，不通过宽泛的 Spark 进程名杀进程。EC2 实例仍需单独 terminate。

## 时间口径和查询间清理

每个样本从提交 SQL、规划到完整 JVM collect 结束计时。Application 启动、表和统计注册、结果校验及清理单独计时。

主表取每条查询两遍中较快的 SQL 时间再求和。同时给出同一次样本的 SQL 加清理时间之和；不会另外挑一个清理更快的样本。这个和不是整个 application 的墙钟时间，也没有做操作系统 cache reset。

清理覆盖两类资源：

- Driverless broadcast：单独移植的上游修复，在正常 UCX query-end reclaim 释放 shuffle 时，释放其缓存的广播 build。
- 普通广播：等待 SQL 完成、没有活跃 Spark job，并收到正常 UCX reclaim 确认后，在所有 executor 上执行 barrier 清理。先移除普通广播的 host block 和 Spark cache 条目，再释放其 GPU/host 资源，避免后续缓存淘汰尝试序列化已清理的对象。

这要求每遍都创建新的 SQL 动作并完整 collect。并发查询、保留并复用 DataFrame、缓存 RDD 和流式 iterator 不在接口支持范围内。已退休的 payload 对象会拒绝继续访问。这是对应 #17019 的受限实验实现，不能理解成通用产品清理策略已经完成。

每次清理保存 executor 确认、对象数量、持有的 GPU batch 字节数，以及清理前后的 RMM 计数。清理耗时独立保留，并进入配套的 SQL 加清理总时间。

## 证据索引与完成条件

源码中的提交链如下，所有链接固定到本次测试的提交；部署和结果证据文件位于下方证据文件链接。

| 材料 | 用途 |
| --- | --- |
| `qualification-summary.json` | 44 个样本/平台、GPU 分配、清理、application ID 和汇总成绩 |
| `per-query-results.csv` | 两轮原始时间、所选轮次、SQL 与对应清理时间 |
| `nvl72-evidence.tar.gz` / `aws48x-evidence.tar.gz` | `config.json`、逐查询 profiles、实际命令、结果、逻辑/执行计划、统计读回、GPU 分配、清理记录、节点和服务退出记录；不包含服务停止用的临时 token |
| `tested-deployments.json` | 两边实际验证的机器路径、NVL container/mounts/节点选择；复制自己对应的平台对象并修改路径 |
| `artifact-locations.json`、两个 `*-manifest.json` | 确切 S3 对象 URI、字节数、SHA-256、源码/JNI/cuDF/私有优化器版本 |
| `aws-host-prerequisites.json` | 实际生效的驱动参数、两套 P2P 矩阵和运行库包版本 |
| `build-and-log-validation.json` / `verification-logs.tar.gz` | 构建、回归测试、UCX interval-tree 测试、Spark 3.5.3 shim 静态检查、正式 driver 日志审计 |
| `attachment-checksums.json` | 以上证据文件的 SHA-256 |

| 源码入口 | 职责 |
| --- | --- |
| [benchmark.py](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/benchmark.py) | 计划、校验、提交、状态、收集、停止 |
| [configs/](https://github.com/binmahone/spark-rapids/tree/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/configs) | 两套完整配置、显式查询例外、依赖锁定 |
| [inputs/](https://github.com/binmahone/spark-rapids/tree/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/inputs) | SQL、固定 catalog 统计、结果参考；大型原始 Parquet 使用前述外部数据路径 |
| [scripts/build.sh](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/build.sh) | 引擎/私有优化器/工具构建和选定回归测试 |
| [scripts/artifact_manifest.py](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/artifact_manifest.py) | 产物哈希与内嵌版本检查 |
| [scripts/build_ucx.sh](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/build_ucx.sh) | 完整 UCX 安装、补丁和 interval-tree 测试 |
| [scripts/prepare_aws_host.sh](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/prepare_aws_host.sh)、[prepare_aws_data.sh](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/prepare_aws_data.sh) | AWS 驱动前提、运行库、本地数据准备 |
| [scripts/run_nvl.slurm](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/run_nvl.slurm)、[run_node.sh](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/run_node.sh)、[prepare_nvl_container.sh](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/prepare_nvl_container.sh) | NVL Slurm/container 启动链 |
| [scripts/node.py](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/node.py)、[qualify_nvl.py](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/qualify_nvl.py) | 节点预检、每 GPU 原生读取校验和每次运行自己的 cuFile 配置 |
| [scripts/services.py](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/services.py) | 按已保存的 PID/启动时间/所有权记录创建和停止服务 |
| [scripts/cohort.py](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/cohort.py)、[run_sql.py](https://github.com/binmahone/spark-rapids/blob/faa47fe51f998ae97385e2b218be4a261006bfa6/benchmarks/scaleup_reference/scripts/run_sql.py) | application 生命周期、native library 路径、SQL 执行、计时、校验、查询间清理 |

构建验证包括私有优化器 427 项通过、Spark 4/4.1 所需的 5 项取消、1 项忽略；两种架构各 38 项 CPU 生命周期测试；10 项 Python 入口测试；AWS 上 1 项实际 GPU 广播释放测试；原生 UCX 构建各 27 项 interval-tree 测试。支持范围是原版 Spark 3.5.3；未宣称其他 shim、最新 main 或所有上游测试组合已完成验证。


正式成绩要求所有 22 条查询、每条两遍都完整完成，结果校验通过，目标 Hybrid 路径有正的读取字节数，没有异常 GDS 回退，所有 executor 都确认清理，没有报告 UCX reclaim 失败，结束后该任务自己的服务已退出。

Slurm `COMPLETED` 或 Spark application 成功本身不足以构成完整验证。下方证据文件链接保留解释和复现结果所需的配置、命令、结果、执行计划和确认记录，不包含原始 event log 或大型临时参考文件。正式 driver 日志已完成审计；AWS driver 日志另存于同一私有 S3 产物前缀下的 `evidence/aws-retained-driver-logs.tar.gz`，SHA-256 为 `70459fe76ed4ceb9f4db5b58400d00fe33cbe52e4147ffb922f532d82c7eb8f8`。NVL driver 日志保留在 controller 的本次运行目录内。构建缓存不是复现前提，测试和产物保存后已清除。
