/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Helper for the native (worker-to-worker via UCX shuffle) broadcast
 * implementation in spark-rapids on OSS Spark 4.x. Mirrors the
 * spark330db EXECUTOR_BROADCAST consumer logic in
 * `GpuExecutorBroadcastHelper.scala`, but lives in the main scala dir so it
 * builds for every Spark shim (in particular, the OSS `400` shim that does
 * not have Databricks' `EXECUTOR_BROADCAST` mode).
 *
 * Given an RDD[ColumnarBatch] produced by a GpuShuffleExchangeExec writing
 * with GpuBroadcastReplicatePartitioning, every partition contains a full
 * copy of the build side. Each downstream consumer reads its assigned
 * partition (== full build) and coalesces into a single GPU ColumnarBatch
 * suitable for use as the build side of a hash join.
 */
package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids.{ConcatAndConsumeAll, GpuCoalesceIterator, GpuColumnVector, GpuMetric, NoopMetric, RapidsConf, RequireSingleBatch}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.CoalesceReadOption
import com.nvidia.spark.rapids.GpuShuffleCoalesceUtils

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuShuffleBroadcastHelper {
  import com.nvidia.spark.rapids.GpuMetric._

  private def shuffleDataIterator(shuffleData: RDD[ColumnarBatch]): Iterator[ColumnarBatch] = {
    // Concatenate iterators across the local partitions; each partition in
    // a replicate-broadcast shuffle contains the full build data, but we
    // pass through whatever Spark routes here. With CoalescedPartitionSpec
    // covering all numMappers, the iterator yields the full build copy
    // collected by this consumer.
    shuffleData.partitions.map { part =>
      shuffleData.iterator(part, TaskContext.get())
    }.reduceLeft(_ ++ _)
  }

  private def shuffleCoalesceIterator(
      shuffleData: RDD[ColumnarBatch],
      buildSchema: StructType,
      metricsMap: Map[String, GpuMetric],
      targetSize: Long): Iterator[ColumnarBatch] = {
    val dataTypes = GpuColumnVector.extractTypes(buildSchema)
    val rawIter = shuffleDataIterator(shuffleData)
    // When the RAPIDS Shuffle Manager + UCX is in use, the shuffle reader
    // returns batches that are already on the GPU (GpuColumnVectorFromBuffer
    // wrappers over contiguous-table buffers), so we must not run them
    // through the host-side deserialisation path in
    // getGpuShuffleCoalesceIterator. Skip directly to the GPU coalesce step
    // that concatenates many GPU batches into one.
    //
    // In the non-UCX path the reader yields host-serialised batches
    // (SerializedTableColumn or KudoSerializedTableColumn) that must be
    // routed through the existing deserialiser pipeline.
    val useGpuShuffle = GpuShuffleEnv.useGPUShuffle(new RapidsConf(SQLConf.get))
    val coalesceInput: Iterator[ColumnarBatch] = if (useGpuShuffle) {
      rawIter
    } else {
      val shuffleMetrics = Map(
        CONCAT_TIME -> metricsMap(CONCAT_TIME),
        OP_TIME_LEGACY -> metricsMap(OP_TIME_LEGACY)
      ).withDefaultValue(NoopMetric)
      GpuShuffleCoalesceUtils.getGpuShuffleCoalesceIterator(rawIter, targetSize,
        dataTypes,
        CoalesceReadOption(SQLConf.get),
        shuffleMetrics)
    }
    new GpuCoalesceIterator(
      coalesceInput,
      dataTypes,
      RequireSingleBatch,
      NoopMetric, // numInputRows
      NoopMetric, // numInputBatches
      NoopMetric, // numOutputRows
      NoopMetric, // numOutputBatches
      NoopMetric, // collectTime
      metricsMap(CONCAT_TIME),
      metricsMap(OP_TIME_LEGACY),
      "GpuShuffleBroadcastHashJoinExec").asInstanceOf[Iterator[ColumnarBatch]]
  }

  /**
   * Get the fully-coalesced build-side ColumnarBatch from a replicate-broadcast
   * shuffle RDD. The returned batch is on the GPU and owned by the caller.
   *
   * @param shuffleData  RDD whose assigned partition for this consumer contains
   *                     a complete copy of the build side
   * @param buildSchema  schema expected for the build side
   * @param buildOutput  output attributes (used for empty-relation case)
   * @param metricsMap   metrics for I/O / concat time accounting
   * @param targetSize   target single-batch size in bytes
   */
  def getShuffleBroadcastBatch(
      shuffleData: RDD[ColumnarBatch],
      buildSchema: StructType,
      buildOutput: Seq[Attribute],
      metricsMap: Map[String, GpuMetric],
      targetSize: Long): ColumnarBatch = {
    val it = shuffleCoalesceIterator(shuffleData, buildSchema, metricsMap, targetSize)
    ConcatAndConsumeAll.getSingleBatchWithVerification(it, buildOutput)
  }

  /**
   * Iterator-form of getShuffleBroadcastBatch. Called from inside a task
   * (compute()) where the build-side RDD has already been resolved to a
   * concrete iterator by the lineage-aware GpuShuffleBroadcastJoinRDD.
   */
  def getShuffleBroadcastBatchFromIter(
      rawIter: Iterator[ColumnarBatch],
      buildSchema: StructType,
      buildOutput: Seq[Attribute],
      metricsMap: Map[String, GpuMetric],
      targetSize: Long): ColumnarBatch = {
    val dataTypes = GpuColumnVector.extractTypes(buildSchema)
    val useGpuShuffle = GpuShuffleEnv.useGPUShuffle(new RapidsConf(SQLConf.get))
    val coalesceInput: Iterator[ColumnarBatch] = if (useGpuShuffle) {
      rawIter
    } else {
      val shuffleMetrics = Map(
        CONCAT_TIME -> metricsMap(CONCAT_TIME),
        OP_TIME_LEGACY -> metricsMap(OP_TIME_LEGACY)
      ).withDefaultValue(NoopMetric)
      GpuShuffleCoalesceUtils.getGpuShuffleCoalesceIterator(rawIter, targetSize,
        dataTypes,
        CoalesceReadOption(SQLConf.get),
        shuffleMetrics)
    }
    val it = new GpuCoalesceIterator(
      coalesceInput,
      dataTypes,
      RequireSingleBatch,
      NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric,
      metricsMap(CONCAT_TIME),
      metricsMap(OP_TIME_LEGACY),
      "GpuShuffleBroadcastHashJoinExec").asInstanceOf[Iterator[ColumnarBatch]]
    ConcatAndConsumeAll.getSingleBatchWithVerification(it, buildOutput)
  }

  /**
   * Get only the build-side row count without materialising the full batch on
   * the GPU. Useful for null-aware anti join optimisations that skip the join
   * if the build is empty.
   */
  def getShuffleBroadcastBatchNumRows(shuffleData: RDD[ColumnarBatch]): Int = {
    val it = shuffleDataIterator(shuffleData)
    if (it.hasNext) {
      var numRows = 0
      while (it.hasNext) {
        withResource(it.next) { batch =>
          numRows += batch.numRows
        }
      }
      numRows
    } else {
      0
    }
  }
}
