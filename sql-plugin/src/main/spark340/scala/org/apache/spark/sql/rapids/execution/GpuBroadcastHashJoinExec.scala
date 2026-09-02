/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids._

import org.apache.spark.internal.Logging
import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ENSURE_REQUIREMENTS,
  ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.internal.SQLConf

class GpuBroadcastHashJoinMeta(
    join: BroadcastHashJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) extends GpuBroadcastHashJoinMetaBase(join, conf, parent, rule) {

  override def convertToGpu(): GpuExec = {
    val Seq(left, right) = childPlans.map(_.convertIfNeeded())
    // The broadcast part of this must be a BroadcastExchangeExec
    val buildSideMeta = buildSide match {
      case GpuBuildLeft => left
      case GpuBuildRight => right
    }
    val originalBuildSide = buildSide match {
      case GpuBuildLeft => join.left
      case GpuBuildRight => join.right
    }
    verifyBuildSideWasReplaced(buildSideMeta)

    val sbEnabled = conf.isShuffleBroadcastEnabled
    val sbDecision = sbEnabled &&
      GpuBroadcastHashJoinMeta.shouldUseShuffleBroadcast(
        buildSideMeta, originalBuildSide, conf)
    GpuBroadcastHashJoinMeta.logRewriteDecision(
      buildSideMeta, originalBuildSide, sbEnabled, sbDecision, conf)
    if (sbDecision) {
      // Swap the GpuBroadcastExchangeExec under the build side for a
      // single-partition GpuShuffleExchangeExec, then construct the consumer
      // GpuShuffleBroadcastHashJoinExec.
      val (newLeft, newRight) =
        GpuBroadcastHashJoinMeta.rewriteBuildToShuffle(left, right, buildSide)
      val extractedCondition = GpuHashJoin.extractJoinConditionIfNeeded(
        conditionMeta, join.joinType, newLeft, newRight)
      val joinExec = GpuShuffleBroadcastHashJoinExec(
        leftKeys.map(_.convertToGpu()),
        rightKeys.map(_.convertToGpu()),
        join.joinType,
        buildSide,
        extractedCondition.joinCondition,
        extractedCondition.left, extractedCondition.right,
        join.isNullAwareAntiJoin)
      val filteredJoinExec = extractedCondition.filterCondition
        .map(c => GpuFilterExec(c, joinExec)()).getOrElse(joinExec)
      extractedCondition.projectIfNeeded(filteredJoinExec)
    } else {
      val extractedCondition = GpuHashJoin.extractJoinConditionIfNeeded(
        conditionMeta, join.joinType, left, right)
      val joinExec = GpuBroadcastHashJoinExec(
        leftKeys.map(_.convertToGpu()),
        rightKeys.map(_.convertToGpu()),
        join.joinType,
        buildSide,
        extractedCondition.joinCondition,
        extractedCondition.left, extractedCondition.right,
        join.isNullAwareAntiJoin)
      val filteredJoinExec = extractedCondition.filterCondition
        .map(c => GpuFilterExec(c, joinExec)()).getOrElse(joinExec)
      extractedCondition.projectIfNeeded(filteredJoinExec)
    }
  }
}

object GpuBroadcastHashJoinMeta extends Logging {

  /** Emit a one-line diagnostic describing whether we will rewrite this BHJ
   *  through the native shuffle-broadcast path. */
  def logRewriteDecision(
      buildSidePlan: SparkPlan,
      originalBuildSidePlan: SparkPlan,
      enabledFlag: Boolean,
      finalDecision: Boolean,
      conf: RapidsConf): Unit = {
    val (sizeStr, unwrapStatus) = unwrapBroadcastExchange(buildSidePlan) match {
      case Some(ex) =>
        val sz = estimatedBuildSize(ex, originalBuildSidePlan)
          .map(_.toString).getOrElse("UNAVAILABLE")
        (sz, "ok")
      case None => ("n/a", s"unwrap_fail(${buildSidePlan.getClass.getSimpleName})")
    }
    val driverThreshold = SQLConf.get.autoBroadcastJoinThreshold
    val maxSize = conf.shuffleBroadcastMaxSize
    logWarning(s"[NATIVE-BCAST] enabled=$enabledFlag unwrap=$unwrapStatus " +
      s"buildSizeBytes=$sizeStr driverThreshold=$driverThreshold maxSize=$maxSize " +
      s"decision=$finalDecision")
  }

  /** Peel BroadcastQueryStageExec / ReusedExchangeExec wrappers to get the
   *  underlying GpuBroadcastExchangeExec on the build side. Returns None if
   *  the structure is unexpected (e.g. AQE on, or already-rewritten). */
  private def unwrapBroadcastExchange(
      plan: SparkPlan): Option[GpuBroadcastExchangeExec] = plan match {
    case g: GpuBroadcastExchangeExec => Some(g)
    case bqse: BroadcastQueryStageExec => bqse.plan match {
      case g: GpuBroadcastExchangeExec => Some(g)
      case ReusedExchangeExec(_, g: GpuBroadcastExchangeExec) => Some(g)
      case _ => None
    }
    case ReusedExchangeExec(_, g: GpuBroadcastExchangeExec) => Some(g)
    case _ => None
  }

  /** Peel the wrappers from the original CPU build side. This plan still has
   *  the logical links that may be absent after GPU conversion. */
  private def unwrapCpuBroadcastExchange(plan: SparkPlan): Option[BroadcastExchangeExec] =
    plan match {
      case b: BroadcastExchangeExec => Some(b)
      case bqse: BroadcastQueryStageExec => bqse.plan match {
        case b: BroadcastExchangeExec => Some(b)
        case ReusedExchangeExec(_, b: BroadcastExchangeExec) => Some(b)
        case _ => None
      }
      case ReusedExchangeExec(_, b: BroadcastExchangeExec) => Some(b)
      case _ => None
    }

  private def staticLogicalSize(plan: SparkPlan): Option[Long] = {
    plan.logicalLink
      .map(_.stats.sizeInBytes)
      .filter(_.isValidLong)
      .map(_.longValue)
  }

  /** Return the static logical-plan estimate for the exchange child.
   *  Runtime statistics are still zero when GpuOverrides performs this
   *  decision, so an unknown static estimate must fail closed. */
  private def estimatedBuildSize(
      exchange: GpuBroadcastExchangeExec,
      originalBuildSidePlan: SparkPlan): Option[Long] =
    staticLogicalSize(exchange.child).orElse {
      unwrapCpuBroadcastExchange(originalBuildSidePlan).flatMap(ex => staticLogicalSize(ex.child))
    }

  /** Decide whether this build side is eligible for shuffle-broadcast. */
  def shouldUseShuffleBroadcast(
      buildSidePlan: SparkPlan,
      originalBuildSidePlan: SparkPlan,
      conf: RapidsConf): Boolean = {
    unwrapBroadcastExchange(buildSidePlan)
      .flatMap(estimatedBuildSize(_, originalBuildSidePlan))
      .exists(size => size >= 0 && size <= conf.shuffleBroadcastMaxSize)
  }

  /** Rewrite the broadcast exchange under the build side into a single-output
   *  shuffle exchange. Every consumer task reads partition 0 (the only
   *  partition) and gets the full build assembled from all mappers' shards. */
  private[execution] def rewriteBuildToShuffle(
      left: SparkPlan,
      right: SparkPlan,
      buildSide: GpuBuildSide): (SparkPlan, SparkPlan) = {
    val buildPlan = buildSide match {
      case GpuBuildLeft => left
      case GpuBuildRight => right
    }
    val broadcastExchange = unwrapBroadcastExchange(buildPlan).getOrElse {
      throw new IllegalStateException(
        "shouldUseShuffleBroadcast returned true but build side has no GpuBroadcastExchangeExec")
    }
    val newExchange = GpuShuffleExchangeExec(
      GpuSinglePartitioning,
      broadcastExchange.child,
      ENSURE_REQUIREMENTS)(GpuSinglePartitioning)

    buildSide match {
      case GpuBuildLeft => (newExchange, right)
      case GpuBuildRight => (left, newExchange)
    }
  }
}

case class GpuBroadcastHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: GpuBuildSide,
    override val condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isNullAwareAntiJoin: Boolean) extends GpuBroadcastHashJoinExecBase(
      leftKeys, rightKeys, joinType, buildSide, condition, left, right, isNullAwareAntiJoin)
