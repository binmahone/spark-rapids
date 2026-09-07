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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Contains, EqualNullSafe}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, In, InSet, Literal}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, Filter, HintInfo, Join, JoinHint}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Broadcast a selectively filtered single-table build after CBO has fixed the join order.
 *
 * Spark cannot estimate literal substring predicates from ordinary column statistics. A large
 * dimension can therefore remain a shuffled build even when its projected keyset fits the normal
 * broadcast threshold, forcing the much larger probe branch through an avoidable exchange. This
 * rule uses the same frozen path-bound metadata as the other Wild optimizer rules and applies a
 * query-independent selectivity estimate. It changes only the join strategy hint; row semantics
 * and join order are unchanged.
 */
case class GpuBroadcastSelectiveFilteredDimension(spark: SparkSession)
  extends Rule[LogicalPlan]
  with Logging {

  private val enabledKey =
    "spark.rapids.sql.optimizer.pushDimensionChainBeforeFact.enabled"
  private val maxOutputColumns = 4
  private val maxInValues = 16
  private val metadata = GpuOptimizerTrustedMetadata.fromSession(spark)

  registerPostCboPass()

  override def apply(plan: LogicalPlan): LogicalPlan = {
    registerPostCboPass()
    if (!enabled || !plan.resolved || plan.isStreaming || metadata.isEmpty) {
      return plan
    }
    plan.transformUp {
      case join: Join => addHint(join).getOrElse(join)
    }
  }

  private def addHint(join: Join): Option[Join] = {
    if (join.joinType != Inner || join.condition.isEmpty || join.hint != JoinHint.NONE) {
      return None
    }

    val left = candidate(join.left, join.right, join.condition.get)
    val right = candidate(join.right, join.left, join.condition.get)
    (left, right) match {
      case (Some(candidateEstimate), None) =>
        logAccepted(join.left, candidateEstimate)
        Some(join.copy(hint = JoinHint(Some(HintInfo(strategy = Some(BROADCAST))), None)))
      case (None, Some(candidateEstimate)) =>
        logAccepted(join.right, candidateEstimate)
        Some(join.copy(hint = JoinHint(None, Some(HintInfo(strategy = Some(BROADCAST))))))
      case _ => None
    }
  }

  private def candidate(
      build: LogicalPlan,
      probe: LogicalPlan,
      condition: Expression): Option[(BigInt, BigInt)] = {
    if (
      build.output.isEmpty || build.output.size > maxOutputColumns ||
      !safeFilteredSingleTable(build) || !hasEquiJoinKey(condition, build, probe)
    ) {
      return None
    }
    val threshold = BigInt(spark.sessionState.conf.autoBroadcastJoinThreshold)
    if (threshold < 0) {
      return None
    }
    metadata.flatMap(_.estimate(build)).filter { estimate =>
      estimate.sizeInBytes > 0 && estimate.sizeInBytes <= threshold &&
        scanBytes(probe) >= estimate.sizeInBytes * 2
    }.map(estimate => estimate.rows -> estimate.sizeInBytes)
  }

  private def safeFilteredSingleTable(plan: LogicalPlan): Boolean = {
    val leaves = plan.collect { case leaf: LeafNode => leaf }
    leaves.size == 1 && !plan.exists {
      case _: LeafNode => false
      case project: Project => !project.projectList.forall(_.deterministic)
      case filter: Filter => !filter.condition.deterministic
      case _: SubqueryAlias => false
      case _ => true
    } && plan.exists {
      case Filter(condition, _) => splitAnd(condition).exists(selectiveLiteralPredicate)
      case _ => false
    }
  }

  private def selectiveLiteralPredicate(expression: Expression): Boolean = expression match {
    case EqualTo(_: Attribute, literal: Literal) => literal.value != null
    case EqualTo(literal: Literal, _: Attribute) => literal.value != null
    case EqualNullSafe(_: Attribute, literal: Literal) => literal.value != null
    case EqualNullSafe(literal: Literal, _: Attribute) => literal.value != null
    case In(_: Attribute, values) =>
      values.nonEmpty && values.size <= maxInValues && values.forall(_.foldable)
    case InSet(_: Attribute, values) => values.nonEmpty && values.size <= maxInValues
    case Contains(_: Attribute, literal: Literal) =>
      literal.value != null && literal.value.toString.nonEmpty
    case _ => false
  }

  private def hasEquiJoinKey(
      condition: Expression,
      build: LogicalPlan,
      probe: LogicalPlan): Boolean = {
    splitAnd(condition).exists {
      case EqualTo(left: Attribute, right: Attribute) =>
        (build.outputSet.contains(left) && probe.outputSet.contains(right)) ||
          (build.outputSet.contains(right) && probe.outputSet.contains(left))
      case _ => false
    }
  }

  private def splitAnd(expression: Expression): Seq[Expression] = expression match {
    case And(left, right) => splitAnd(left) ++ splitAnd(right)
    case other => Seq(other)
  }

  private def scanBytes(plan: LogicalPlan): BigInt = {
    val bytes = plan.collect { case leaf: LeafNode => leaf.stats.sizeInBytes }.sum
    if (bytes > 0) bytes else plan.stats.sizeInBytes
  }

  private def logAccepted(
      build: LogicalPlan,
      estimate: (BigInt, BigInt)): Unit = {
    logWarning(
      "GpuBroadcastSelectiveFilteredDimension: broadcast selective build " +
        s"rows=${estimate._1} bytes=${estimate._2} " +
        s"columns=${build.output.map(_.name).mkString(",")}")
  }

  private def enabled: Boolean = spark.sessionState.conf
    .getConfString(enabledKey, "false")
    .toBoolean

  private def registerPostCboPass(): Unit = {
    if (!enabled) {
      return
    }
    val experimental = spark.experimental
    experimental.synchronized {
      if (!experimental.extraOptimizations.exists(
          _.isInstanceOf[GpuBroadcastSelectiveFilteredDimension])) {
        experimental.extraOptimizations = experimental.extraOptimizations :+ this
      }
    }
  }
}
