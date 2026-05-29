/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.physical.{RangePartitioning, SinglePartition}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeExec, ShuffleExchangeLike}

/**
 * Rewrite a top-of-plan `Exchange(RangePartitioning) -> global Sort` into
 * `Exchange(SinglePartition) -> Sort`, matching Presto's single-gather ORDER BY shape.
 *
 * Motivation: Spark's `RangePartitioner` samples its input via a separate Spark job to compute
 * partition bounds; with AQE off that sampling job re-executes the entire upstream, double-scanning
 * the input (e.g. TPC-H Q1/Q4 scan lineitem twice). `SinglePartition` needs no bounds, so the
 * sampling job -- and the double scan -- disappear, with one reducer gathering the (small,
 * post-aggregation) result for the final sort.
 *
 * Only the top-of-plan global Sort is rewritten. Per-key sorts inside SortMergeJoin require
 * ClusteredDistribution (HashPartitioning), never OrderedDistribution, so they are never touched.
 *
 * Runs in `preColumnarTransitions` on the vanilla physical plan (before GpuOverrides), so the
 * rewritten SinglePartition ShuffleExchangeExec is subsequently converted to GPU by GpuOverrides.
 *
 * Gated by `spark.rapids.sql.optimizer.rangeSortSinglePartition.enabled` (default false). Intended
 * for queries whose final sorted output is small; do not enable for arbitrary ORDER BY over large
 * result sets, where single-partition gather would bottleneck.
 *
 * Mirrors gluten's MppSinglePartitionSortRule for the spark-rapids plan shape.
 */
class GpuRangeSinglePartitionSortRule extends Rule[SparkPlan] with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!new RapidsConf(plan.conf).isRangeSortSinglePartitionEnabled) {
      plan
    } else {
      rewriteRoot(plan)
    }
  }

  // Recognize the top-of-plan global Sort.
  private object GlobalSort {
    def unapply(p: SparkPlan): Option[SortExec] = p match {
      case s: SortExec if s.global => Some(s)
      case _ => None
    }
  }

  // Walk the root spine (Project / Sort / single-child pass-throughs) until the global Sort,
  // then rewrite the RangePartitioning shuffle feeding it. Stop at AQE roots.
  private def rewriteRoot(node: SparkPlan): SparkPlan = node match {
    case aqe: AdaptiveSparkPlanExec => aqe
    case p: ProjectExec => p.withNewChildren(Seq(rewriteRoot(p.child)))
    case GlobalSort(s) => s.withNewChildren(Seq(rewriteSubtree(s.child)))
    case other if other.children.size == 1 && isRootSpine(other) =>
      other.withNewChildren(Seq(rewriteRoot(other.children.head)))
    case other => other
  }

  private def isRootSpine(p: SparkPlan): Boolean = p match {
    case _: SortExec | _: ProjectExec => true
    case _ => false
  }

  // Find the first RangePartitioning shuffle below the sort and rewrite it to SinglePartition.
  private def rewriteSubtree(node: SparkPlan): SparkPlan = node match {
    case stage: ShuffleQueryStageExec =>
      stage.plan match {
        case sh: ShuffleExchangeLike if sh.outputPartitioning.isInstanceOf[RangePartitioning] =>
          logWarning("GpuRangeSinglePartitionSortRule: rewriting RANGE -> SINGLE (AQE stage)")
          rewriteShuffle(sh)
        case _ => node
      }
    case sh: ShuffleExchangeLike if sh.outputPartitioning.isInstanceOf[RangePartitioning] =>
      logWarning("GpuRangeSinglePartitionSortRule: rewriting RANGE -> SINGLE " +
        s"(${sh.getClass.getSimpleName})")
      rewriteShuffle(sh)
    case other if other.children.size == 1 =>
      other.withNewChildren(Seq(rewriteSubtree(other.children.head)))
    case other => other
  }

  private def rewriteShuffle(sh: ShuffleExchangeLike): SparkPlan = sh match {
    case se: ShuffleExchangeExec =>
      ShuffleExchangeExec(SinglePartition, se.child, se.shuffleOrigin)
    case other =>
      ShuffleExchangeExec(SinglePartition, other.children.head)
  }
}
