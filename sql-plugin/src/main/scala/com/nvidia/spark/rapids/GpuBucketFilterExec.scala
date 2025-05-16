/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.GpuMetric.{DESCRIPTION_OP_TIME, ESSENTIAL_LEVEL, MODERATE_LEVEL, NUM_OUTPUT_BATCHES, NUM_OUTPUT_ROWS, OP_TIME}
import com.nvidia.spark.rapids.shims.{ShimPredicateHelper, ShimUnaryExecNode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, HiveHash, Literal, Pmod, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.GpuEqualTo
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuBucketFilterExec(
      bucketKeys: Seq[Expression],
      numShufflePartitions: Int,
      child: SparkPlan) extends ShimUnaryExecNode with ShimPredicateHelper with GpuExec {

  private val bucketIdExpression = Pmod(HiveHash(bucketKeys), Literal(numShufflePartitions))

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME))

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME)
    val rdd = child.executeColumnar()

    rdd.mapPartitionsWithIndex { (index, batchIter) =>
      val condition = GpuEqualTo(bucketIdExpression, GpuLiteral(index))
      val boundCondition = GpuBindReferences.bindGpuReferencesTiered(Seq(condition), child.output,
        conf)
      batchIter.flatMap { batch =>
        GpuFilter.filterAndClose(batch, boundCondition, numOutputRows,
          numOutputBatches, opTime)
      }
    }
  }
}