# Native GPU-to-GPU Broadcast for spark-rapids (OSS Spark 4.0)

## Problem statement

spark-rapids' current `GpuBroadcastExchangeExec` follows Spark's driver-mediated
broadcast path:

```
executor GPU (build side) → D2H → driver collect → driver host hashtable
  → TorrentBroadcast → executor host → H2D → GPU hashtable rebuild
```

For Q5 SF1000 4-GPU with CBO+autoBroadcastJoinThreshold=8g, this driver round-trip
costs **~25 s/iter of GPU idle time**: per-iter wall 31 s, GPU active only 5 s,
the other ~26 s the GPU sits dark while the driver collects and broadcasts
GB-scale data. nsys profile: `60% GPU idle / 95% wall idle` on the
`q5_cgt5_cbo_bcast8g` run.

Aggressive BHJ plan (every dim table broadcast) is the right plan-shape direction
— it eliminates the 168 GB lineitem shuffle that costs ~613 ms of
`dispatch_map_type` gather kernel per iter on the SHJ path. CBO + table stats
already pick this plan correctly. The blocker is the broadcast TRANSPORT.

Databricks Spark has `EXECUTOR_BROADCAST` mode which uses shuffle instead of
driver round-trip. spark-rapids has the consumer-side support for it in the
`spark330db` shim (see `GpuExecutorBroadcastHelper.scala`). OSS Spark 4.0 does
NOT have the `EXECUTOR_BROADCAST` mode, but exposes the building blocks
(`getShuffleRDD()` at `ShuffleExchangeExec.scala:81/157`, used by AQE).

## Approach

Implement the equivalent of `EXECUTOR_BROADCAST` entirely inside the spark-rapids
plugin, without modifying Spark itself. Use spark-rapids' existing
`RapidsShuffleManager` (UCX-based, GPU-to-GPU) as the transport.

**Architecture**: new SparkPlan operator pair that bypasses Spark's
`BroadcastExchangeExec` and `BroadcastHashJoinExec` entirely. Replacement
happens at `GpuOverrides` rewrite time.

```
  Spark generates:                              spark-rapids rewrites to:
  ─────────────────                             ──────────────────────────
  BroadcastExchangeExec                         GpuShuffleBroadcastExchangeExec
       │                                              │
       ▼                                              ▼  (RapidsShuffleManager
  BroadcastHashJoinExec        →                      │   with replicate
       │                                              │   partitioning)
       ▼                                              ▼
                                                GpuShuffleBroadcastHashJoinExec
                                                      │
                                                      ▼
                                                consumes shuffle RDD via
                                                  getShuffleRDD()
```

### Required new components

1. **`GpuBroadcastReplicatePartitioning(numPartitions)`** — a `Partitioning`
   subclass that emits each input row to **every** output partition. Satisfies
   `BroadcastDistribution(mode)` so any consumer requiring broadcast
   distribution can use it.

2. **`GpuPartitioning.sliceForBroadcastReplicate()`** — implementation of the
   replicate partitioning in our partitioner. Input N rows → output P × N rows
   where each output partition gets a full copy. Reuse current
   contiguous-split / kudo wire format unchanged; just emit P times.

3. **`GpuShuffleBroadcastExchangeExec`** — new `Exchange`-like operator. Its
   `outputPartitioning = GpuBroadcastReplicatePartitioning(N)`. Inherits map
   side from `GpuShuffleExchangeExec` (same shuffle write infra). Exposes
   `getShuffleRDD()`.

4. **`GpuShuffleBroadcastHashJoinExec`** — new join operator consuming a shuffle
   RDD as build side. Logic ported from
   `spark330db/GpuBroadcastHashJoinExec.doColumnarExecutorBroadcastJoin` and its
   helper `GpuExecutorBroadcastHelper.getExecutorBroadcastBatch`. The consumer
   reads its shuffle partition (a complete copy of the build), coalesces, builds
   GPU hash table, then probes.

5. **Plan rewrite rule** in `GpuOverrides` — convert
   `GpuBroadcastExchangeExec → GpuShuffleBroadcastExchangeExec` and the
   downstream `GpuBroadcastHashJoinExec → GpuShuffleBroadcastHashJoinExec` when:
     - build estimated size > `spark.rapids.shuffle.broadcast.driverThreshold`
       (default = current `autoBroadcastJoinThreshold`)
     - build estimated size < `spark.rapids.shuffle.broadcast.executorThreshold`
       (default = 8 GB)
   Outside this range, use the existing driver-mediated path.

### Wire format

Reuse the current `RapidsShuffleManager` wire format unchanged (ContiguousTable
or Kudo). Each output partition writes the full data; reducer reads its assigned
partition. **Data volume cost**: with N consumers, N × build_size travels over
shuffle. For 4 executors × 8 reducer-tasks = 32 consumers and 3 GB build = 96 GB
network traffic. B200 NVLink P2P (~365 GB/s) handles this in ~0.3 s. Acceptable
overhead for an initial implementation; future optimization could share one
copy per node.

### What about `BroadcastDistribution` / Spark planner

We replace `GpuBroadcastHashJoinExec` with `GpuShuffleBroadcastHashJoinExec`
at the same time as we replace the exchange. The new join's
`requiredChildDistribution` returns `UnspecifiedDistribution` (we feed it from
shuffle, not broadcast). `EnsureRequirements` won't re-inject a shuffle/broadcast
because we're already past that phase by the time `GpuOverrides` rewrites.

## Phases

| Phase | Work | Status |
|-------|------|--------|
| 1 | `GpuBroadcastReplicatePartitioning` + NVTX label `BROADCAST_REPLICATE_PARTITION` | ✅ done, compiles |
| 2 | `GpuShuffleBroadcastExchangeExec` — **superseded by design decision**: reuse existing `GpuShuffleExchangeExec` constructed with `GpuBroadcastReplicatePartitioning(N, mode)`. The shuffle write path (`prepareBatchShuffleDependency`) already routes `Array[(ColumnarBatch, Int)]` correctly, our partitioning's `columnarEvalAny` emits N copies. No new exchange class needed. | ✅ done (zero new code; design choice) |
| 3 | `GpuShuffleBroadcastHelper` (port spark330db logic into shim-agnostic location) + `GpuShuffleBroadcastHashJoinExec` (consumes `getShuffleRDD()`, builds hash, probes) | ✅ done, compiles |
| 4a | `RapidsConf` knobs `spark.rapids.shuffle.broadcast.enabled` / `.maxSize` | ✅ done |
| 4b | `GpuOverrides` plan rewrite rule — when `GpuBroadcastExchangeExec` build size in `(driverThreshold, maxSize]`, swap exchange → `GpuShuffleExchangeExec(GpuBroadcastReplicatePartitioning(N, mode))` AND swap downstream `GpuBroadcastHashJoinExec` → `GpuShuffleBroadcastHashJoinExec` atomically | pending |
| 5 | Q5 integration test + nsys verify driver round-trip is gone (CBO+bcast=8g Q5 30 s → ~5-6 s target) | pending |
| 6 | Edge cases: empty relation, sub-query broadcast, ReusedExchange, AQE off (AQE-on is a P2 follow-up — known RapidsShuffleManager + AQE crash, see project memory) | pending |

## Code locations

New files:
- `sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuBroadcastReplicatePartitioning.scala`
- `sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/GpuShuffleBroadcastHelper.scala`
- `sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/GpuShuffleBroadcastHashJoinExec.scala`

Edits:
- `sql-plugin/.../NvtxRangeWithDoc.scala` — add `BROADCAST_REPLICATE_PARTITION` NVTX label + register
- `sql-plugin/.../RapidsConf.scala` — add `SHUFFLE_BROADCAST_ENABLED` + `SHUFFLE_BROADCAST_MAX_SIZE` confs + accessors

## Validation targets

Q5 SF1000 4-GPU on B200 (4× B200 GPUs, driver 580):

| | current | target |
|---|---:|---:|
| baseline tune1 (SHJ) Min Hot | 4.60 s | – |
| CBO + driver bcast Min Hot | 30.7 s | – |
| **CBO + native bcast Min Hot** | – | **5-6 s** (driver round-trip eliminated) |
| pv-cli Min Hot | 1.29 s | (still a gap from BHJ probe cost + per-iter active GPU window) |

Definition of done: ` nvcomp::unsnap_kernel` / `dispatch_map_type` partition kernel
drops out as expected from the baseline kernel mix, and timeline shows GPU
active fraction > 50% (vs the current 5%).

## Risks

- **`getShuffleRDD()` API stability**: OSS Spark 4.0 has it, but it's labeled
  "for AQE". Future versions might tighten access. Mitigation: gate via
  `SparkShimImpl` like other Spark-version-specific code.
- **DPP (Dynamic Partition Pruning)** uses broadcast side-effect to push filter
  to scan. Our new exchange doesn't currently produce a Spark Broadcast variable
  → DPP filter won't propagate. Mitigation: stage rewrite rule to either skip
  DPP-feeding broadcasts or add a parallel `executeBroadcast()` path.
- **Memory cost from N× data on shuffle wire**: addressed by the
  `executorThreshold` cap.
- **RapidsShuffleManager + AQE bug** (already observed when we tried
  `adaptive.enabled=true`): keep AQE off in the initial implementation.

## File layout

New files under `sql-plugin/src/main/scala/.../shuffle/broadcast/`:
- `GpuBroadcastReplicatePartitioning.scala`
- `GpuShuffleBroadcastExchangeExec.scala`
- `GpuShuffleBroadcastHashJoinExec.scala`

Edits:
- `GpuPartitioning.scala` — add replicate branch in `sliceInternalGpuOrCpuAndClose`
- `GpuOverrides.scala` — new plan rewrite rule
- `RapidsConf.scala` — new conf knobs

Reference (read but do not modify):
- `spark330db/GpuExecutorBroadcastHelper.scala` — consumer logic to port
- `spark330db/GpuBroadcastHashJoinExec.scala` — full Databricks integration
- `GpuBroadcastExchangeExecBase.scala` — current driver path
- `GpuShuffleExchangeExec.scala` — base class for new exchange
- OSS `ShuffleExchangeExec.scala:81` `getShuffleRDD()` API
