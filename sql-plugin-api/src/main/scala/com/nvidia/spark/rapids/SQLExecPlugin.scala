/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan, SparkStrategy}

/**
 * Extension point to enable GPU SQL processing.
 */
class SQLExecPlugin extends (SparkSessionExtensions => Unit) {
  private val strategyRules: SparkStrategy = ShimLoader.newStrategyRules()

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(columnarOverrides)
    extensions.injectQueryStagePrepRule(queryStagePrepOverrides)
    extensions.injectPlannerStrategy(_ => strategyRules)
    extensions.injectPostHocResolutionRule(postHocResolutionOverrides)
    // Trigger so the rule self-registers into spark.experimental.extraOptimizations, where it
    // runs after the Subquery batch (EXISTS -> LeftSemiJoin). No-op unless
    // spark.rapids.sql.optimizer.rewriteLargeLeftSemi.enabled=true.
    extensions.injectOptimizerRule(spark => GpuRewriteLargeLeftSemiToInnerDistinct(spark))
    // This rule also self-registers after CBO, where Spark's selected join order is available.
    // It is disabled unless spark.rapids.sql.optimizer.pushDimensionChainBeforeFact.enabled=true.
    extensions.injectOptimizerRule(spark => GpuPushSelectiveDimensionChainBeforeFact(spark))
    // Reorder a large filtered fact chain only when trusted range and join statistics prove that
    // the alternative reduces intermediate bytes substantially.
    extensions.injectOptimizerRule(spark => GpuReorderSelectiveFactChain(spark))
    // Aggregate pushdown consumes the join order selected by the fact-chain rule.
    extensions.injectOptimizerRule(spark => GpuPushAggregateMeasureBeforeJoin(spark))
    // Add a post-CBO broadcast hint when trusted metadata proves that a selectively filtered
    // dimension keyset fits the ordinary Spark broadcast threshold.
    extensions.injectOptimizerRule(spark => GpuBroadcastSelectiveFilteredDimension(spark))
    // Prune the grouped copy of a fact by a selective keyset already required by the outer join.
    extensions.injectOptimizerRule(spark => GpuPushSelectiveDimensionFilterIntoAggregate(spark))
    // Apply an already-required selective keyset to both inputs of a later many-key join.
    extensions.injectOptimizerRule(spark => GpuPushSelectiveKeysetToJoinInputs(spark))
    // Deduplicate a large anti-join existence set when trusted NDV statistics prove the benefit.
    extensions.injectOptimizerRule(spark => GpuDeduplicateLargeLeftAntiBuildSide(spark))
  }

  private def columnarOverrides(sparkSession: SparkSession): ColumnarRule = {
    ShimLoader.newColumnarOverrideRules(sparkSession)
  }

  private def queryStagePrepOverrides(sparkSession: SparkSession): Rule[SparkPlan] = {
    ShimLoader.newGpuQueryStagePrepOverrides(sparkSession)
  }

  private def postHocResolutionOverrides(sparkSession: SparkSession): Rule[LogicalPlan] = {
    ShimLoader.newGpuPostHocResolutionOverrides(sparkSession)
  }
}
