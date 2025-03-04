package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.GpuMetric.{NUM_OUTPUT_BATCHES, NUM_OUTPUT_ROWS}
import com.nvidia.spark.rapids.shims.ShimSparkPlan

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, HashExpression, Murmur3Hash}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashClusteredDistribution, Partitioning, PartitioningCollection, UnionHashPartitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.execution.ShimTrampolineUtil
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuBucketUnionExec(
    children: Seq[SparkPlan],
    requiredNumPartitions: Int,
    hashingFunctionClass: Class[_ <: HashExpression[Int]] = classOf[Murmur3Hash],
    outputIndices: Seq[Int]) extends ShimSparkPlan with GpuExec {

  // updating nullability to make all the children consistent

  override def output: Seq[Attribute] = {
    children.map(_.output).transpose.map {
      attrs =>
        val firstAttr = attrs.head
        val nullable = attrs.exists(_.nullable)
        val newDt = attrs.map(_.dataType).reduce(ShimTrampolineUtil.unionLikeMerge)
        if (firstAttr.dataType == newDt) {
          firstAttr.withNullability(nullable)
        } else {
          AttributeReference(firstAttr.name, newDt, nullable, firstAttr.metadata)(
            firstAttr.exprId,
            firstAttr.qualifier)
        }
    }
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    children.map(
      child =>
        HashClusteredDistribution(
          outputIndices.map(child.output(_)),
          Some(requiredNumPartitions),
          hashingFunctionClass))
  }

  override def outputPartitioning: Partitioning = children.head.outputPartitioning

  // The smallest of our children
  override def outputBatching: CoalesceGoal =
    children.map(GpuExec.outputBatching).reduce(CoalesceGoal.minProvided)

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)

    sparkContext.bucketUnion(children.map(_.executeColumnar())).map { batch =>
      numOutputBatches += 1
      numOutputRows += batch.numRows
      batch
    }
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    GpuBucketUnionExec(newChildren, requiredNumPartitions, hashingFunctionClass, outputIndices)
}

case class GpuParallelBucketUnionExec(
    override val children: Seq[SparkPlan],
    staticPartExpr: Option[Seq[Expression]])
  extends ShimSparkPlan with GpuExec {
  // updating nullability to make all the children consistent
  override def output: Seq[Attribute] = {
    children.map(_.output).transpose.map { attrs =>
      val firstAttr = attrs.head
      val nullable = attrs.exists(_.nullable)
      val newDt = attrs.map(_.dataType).reduce(TrampolineUtil.unionLikeMerge)
      if (firstAttr.dataType == newDt) {
        firstAttr.withNullability(nullable)
      } else {
        AttributeReference(firstAttr.name, newDt, nullable, firstAttr.metadata)(
          firstAttr.exprId, firstAttr.qualifier)
      }
    }
  }

  // The smallest of our children
  override def outputBatching: CoalesceGoal =
    children.map(GpuExec.outputBatching).reduce(CoalesceGoal.minProvided)

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)

    sparkContext.union(children.map(_.executeColumnar())).map { batch =>
      numOutputBatches += 1
      numOutputRows += batch.numRows
      batch
    }
  }

  override def outputPartitioning: Partitioning = {
    val subPlanDist =
      this.children.map(_.outputPartitioning).map { case p: PartitioningCollection => p }
    staticPartExpr match {
      case Some(expr) =>
        UnionHashPartitioning(subPlanDist, expr)
      case None => super.outputPartitioning
    }
  }

  override def withNewChildrenInternal(
    newChildren: IndexedSeq[SparkPlan]): GpuParallelBucketUnionExec =
    GpuParallelBucketUnionExec(newChildren, staticPartExpr)
}