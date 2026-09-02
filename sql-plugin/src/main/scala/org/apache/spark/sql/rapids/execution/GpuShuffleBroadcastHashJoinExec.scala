/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Native-broadcast (worker-to-worker shuffle) hash join. Pairs with a
 * GpuShuffleExchangeExec whose gpuOutputPartitioning is
 * GpuSinglePartitioning. Each executor fetches that single build partition
 * through UCX and caches the resulting GPU batch for its local consumer tasks.
 *
 * This is the OSS Spark 4.x analogue of the Databricks `spark330db`
 * EXECUTOR_BROADCAST consumer logic in
 * `GpuBroadcastHashJoinExec.doColumnarExecutorBroadcastJoin` /
 * `GpuExecutorBroadcastHelper`. Unlike Databricks, we don't need a special
 * BroadcastMode at the Spark planner level; the new exchange is just a
 * GpuShuffleExchangeExec with replicate partitioning.
 */
package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.shims.ShimBinaryExecNode

import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftAnti}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuShuffleBroadcastHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: GpuBuildSide,
    override val condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isNullAwareAntiJoin: Boolean)
    extends ShimBinaryExecNode with GpuHashJoin {

  import GpuMetric._

  // Same checks as Spark / GpuBroadcastHashJoinExec
  if (isNullAwareAntiJoin) {
    require(leftKeys.length == 1, "leftKeys length should be 1")
    require(rightKeys.length == 1, "rightKeys length should be 1")
    require(joinType == LeftAnti, "joinType must be LeftAnti.")
    require(buildSide == GpuBuildRight, "buildSide must be BuildRight.")
    require(condition.isEmpty, "null aware anti join optimize condition should be empty.")
  }

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    STREAM_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_STREAM_TIME),
    JOIN_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_JOIN_TIME),
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES),
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME)
  )

  // The build side is fed by a single-partition GpuShuffleExchangeExec. This
  // operator consumes the exchange directly, so it does not ask Spark to
  // inject a BroadcastExchange through requiredChildDistribution.
  override def requiredChildDistribution: Seq[Distribution] =
    UnspecifiedDistribution :: UnspecifiedDistribution :: Nil

  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  /** Extract the GpuShuffleExchangeExec feeding our build side. The
   *  GpuTransitionOverrides post-pass may wrap the rewritten exchange in
   *  GpuCoalesceBatches or other adaptors before the join consumes it, so
   *  search the build subtree for the first matching exchange. */
  private def buildShuffleExchange: GpuShuffleExchangeExec = {
    buildPlan.collectFirst {
      case gpu: GpuShuffleExchangeExec => gpu
      case reused: ReusedExchangeExec if reused.child.isInstanceOf[GpuShuffleExchangeExec] =>
        reused.child.asInstanceOf[GpuShuffleExchangeExec]
    }.getOrElse {
      throw new IllegalStateException(
        s"GpuShuffleBroadcastHashJoinExec build subtree does not contain a " +
          s"GpuShuffleExchangeExec. buildPlan = ${buildPlan.simpleString(50)}")
    }
  }

  override def doExecute(): RDD[InternalRow] = throw new IllegalStateException(
    "GpuShuffleBroadcastHashJoinExec does not support row-based processing")

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)
    val streamTime = gpuLongMetric(STREAM_TIME)
    val joinTime = gpuLongMetric(JOIN_TIME)

    val targetSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf)
    val joinOptions = RapidsConf.getJoinOptions(conf, targetSize)

    val exchange = buildShuffleExchange
    val buildShuffleId = exchange.shuffleDependencyColumnar.shuffleId

    // Force the build-side map stage to run and register its outputs with
    // the MapOutputTracker before consumer tasks start. Without AQE
    // (where ShuffleQueryStageExec would do this automatically) the stream
    // RDD has no Spark dependency on the build shuffle, so the scheduler
    // would otherwise schedule consumer tasks first and they would fail
    // looking up an unregistered shuffleId.
    val statsFuture = sparkContext.submitMapStage(exchange.shuffleDependencyColumnar)
    statsFuture.get()

    // With GpuSinglePartitioning the build exchange has exactly one reducer
    // partition containing the full build, assembled from all mappers'
    // partition-0 shards. Every consumer task reads that one partition.
    val partitionSpecs = Array[org.apache.spark.sql.execution.ShufflePartitionSpec](
      CoalescedPartitionSpec(0, 1))
    val buildRelation = exchange.getShuffleRDD(partitionSpecs)
      .asInstanceOf[RDD[ColumnarBatch]]

    val streamRdd = streamedPlan.executeColumnar()
    val localIsNullAwareAntiJoin = isNullAwareAntiJoin
    val localTargetSize = targetSize
    val localBuildSchema = buildPlan.schema
    val localBuildOutput = buildPlan.output
    val localBoundBuildKeys = boundBuildKeys
    val localBoundStreamKeys = boundStreamKeys
    val localJoinOptions = joinOptions
    val localAllMetrics = allMetrics
    val localBuildShuffleId = buildShuffleId

    streamRdd.mapPartitions { it =>
      val collectTimeIter =
        new CollectTimeIterator(NvtxRegistry.BROADCAST_JOIN_STREAM, it, streamTime)
      val bufferedStreamIter = new CloseableBufferedIterator(collectTimeIter)
      val builtBatch = closeOnExcept(bufferedStreamIter) { _ =>
        NvtxRegistry.JOIN_FIRST_STREAM_BATCH {
          if (bufferedStreamIter.hasNext) {
            bufferedStreamIter.head
          } else {
            GpuSemaphore.acquireIfNecessary(org.apache.spark.TaskContext.get())
          }
        }
        // Executor-scoped build cache: only the first task on this executor
        // does the shuffle fetch; subsequent tasks share the cached
        // SpillableColumnarBatch and just materialise a ColumnarBatch view
        // from it. Reduces GPU memory K-fold for K consumer tasks per
        // executor and avoids redundant UCX traffic on retries.
        GpuShuffleBroadcastBuildCache.getOrBuild(localBuildShuffleId, () => {
          GpuShuffleBroadcastHelper.getShuffleBroadcastBatch(
            buildRelation, localBuildSchema, localBuildOutput, localAllMetrics,
            localTargetSize)
        })
      }
      if (localIsNullAwareAntiJoin) {
        if (builtBatch.numRows() == 0) {
          withResource(builtBatch)(_ => bufferedStreamIter)
        } else if (closeOnExcept(builtBatch)(GpuHashJoin.anyNullInKey(_, localBoundBuildKeys))) {
          withResource(builtBatch)(_ => Iterator.empty)
        } else {
          val nullFilteredStreamIter = bufferedStreamIter.map { cb =>
            GpuHashJoin.filterNullsWithRetryAndClose(
              SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY),
              localBoundStreamKeys)
          }
          doJoin(builtBatch, nullFilteredStreamIter, localJoinOptions, numOutputRows,
            numOutputBatches, opTime, joinTime)
        }
      } else {
        doJoin(builtBatch, bufferedStreamIter, localJoinOptions, numOutputRows,
          numOutputBatches, opTime, joinTime)
      }
    }
  }
}
