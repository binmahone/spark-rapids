/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, BroadcastMode, Distribution, IdentityBroadcastMode}
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Replicate-to-all partitioning. Each input batch is emitted N times, once per
 * output partition, where N = numPartitions. Used for native (worker-to-worker
 * via shuffle) broadcast — every consumer reads its assigned shuffle partition
 * and receives a complete copy of the build side.
 *
 * Satisfies BroadcastDistribution(mode) so a downstream
 * GpuShuffleBroadcastHashJoinExec can declare it as its required input
 * distribution.
 *
 * The N copies share the underlying GPU device buffers via refcount; the
 * shuffle writer serialises each copy independently into the wire format, so
 * N × payload bytes travel over the network. For an initial implementation
 * this is acceptable on B200 NVLink (~365 GB/s P2P). A future optimisation
 * could share one copy per executor instead of per consumer task.
 */
case class GpuBroadcastReplicatePartitioning(numPartitions: Int, mode: BroadcastMode)
    extends GpuExpression with ShimExpression with GpuPartitioning {

  override def children: Seq[GpuExpression] = Nil
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies0(required: Distribution): Boolean = required match {
    case BroadcastDistribution(m) => m == mode
    case _ => super.satisfies0(required)
  }

  override def columnarEvalAny(batch: ColumnarBatch): Any = NvtxRegistry.BROADCAST_REPLICATE_PARTITION {
    val numCols = batch.numCols()
    val rows = batch.numRows()
    if (numPartitions == 1) {
      // Single consumer — no replication needed; downstream code expects the
      // same Array shape so wrap accordingly.
      Array((batch, 0))
    } else {
      val results = new Array[(ColumnarBatch, Int)](numPartitions)
      var partIdx = 0
      try {
        while (partIdx < numPartitions) {
          val cols = new Array[ColumnVector](numCols)
          var c = 0
          while (c < numCols) {
            batch.column(c) match {
              case gpu: GpuColumnVector =>
                cols(c) = GpuColumnVector.from(gpu.getBase.incRefCount(), gpu.dataType())
              case other =>
                throw new IllegalStateException(
                  "GpuBroadcastReplicatePartitioning expects GpuColumnVector inputs, got " +
                    other.getClass.getName)
            }
            c += 1
          }
          results(partIdx) = (new ColumnarBatch(cols, rows), partIdx)
          partIdx += 1
        }
      } catch {
        case t: Throwable =>
          var i = 0
          while (i < partIdx) {
            if (results(i) != null) results(i)._1.close()
            i += 1
          }
          throw t
      }
      // The original batch's column references stay alive via the per-copy
      // refcount increments above; close the batch wrapper itself.
      batch.close()
      results
    }
  }
}

object GpuBroadcastReplicatePartitioning {
  /** Default partitioning mode for executor-broadcast use — analogous to
   *  Databricks' `ExecutorBroadcastMode` (identity, no key transform). */
  def identity(numPartitions: Int): GpuBroadcastReplicatePartitioning =
    GpuBroadcastReplicatePartitioning(numPartitions, IdentityBroadcastMode)
}
