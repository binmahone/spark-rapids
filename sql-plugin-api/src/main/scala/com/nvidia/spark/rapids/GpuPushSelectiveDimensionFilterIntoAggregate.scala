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
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, BROADCAST, Filter, HintInfo, Join}
import org.apache.spark.sql.catalyst.plans.logical.{JoinHint, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.JOIN

/**
 * Push an already-required selective dimension key into a grouped copy of the same fact table.
 *
 * For `(fact JOIN filtered_dimension) JOIN (AGGREGATE same_fact GROUP BY fact_key)`, the upper
 * inner joins prove that aggregate groups absent from the filtered dimension cannot contribute.
 * Adding `same_fact LEFT SEMI JOIN filtered_dimension` below the aggregate preserves fact and
 * outer-join multiplicity while avoiding aggregation and shuffle for irrelevant fact keys.
 */
case class GpuPushSelectiveDimensionFilterIntoAggregate(spark: SparkSession)
  extends Rule[LogicalPlan]
  with Logging {

  private val enabledKey =
    "spark.rapids.sql.optimizer.pushDimensionChainBeforeFact.enabled"
  private val maxDimensionScanRatio = BigDecimal("0.25")
  private val maxBroadcastRows = BigInt(512000000)
  private val metadata = GpuOptimizerTrustedMetadata.fromSession(spark)

  registerPostCboPass()

  override def apply(plan: LogicalPlan): LogicalPlan = {
    registerPostCboPass()
    if (!enabled || !plan.resolved || plan.isStreaming) {
      return plan
    }
    plan.transformUpWithPruning(_.containsPattern(JOIN)) {
      case join: Join => rewriteJoin(join).getOrElse(join)
    }
  }

  private def rewriteJoin(join: Join): Option[LogicalPlan] = {
    if (join.joinType != Inner || join.condition.isEmpty) {
      return None
    }
    rewriteOrientation(join, join.left, join.right)
      .orElse(rewriteOrientation(join, join.right, join.left))
  }

  private def rewriteOrientation(
      topJoin: Join,
      outerSide: LogicalPlan,
      aggregateSide: LogicalPlan): Option[LogicalPlan] = {
    val aggregate = unwrapAggregate(aggregateSide).getOrElse(return None)
    if (!safeAggregate(aggregate) || aggregate.child.exists(_.isInstanceOf[Join])) {
      return None
    }
    val groupingInput = aggregate.groupingExpressions.head.asInstanceOf[Attribute]
    val groupingOutput = aggregate.aggregateExpressions.collectFirst {
      case named if named.semanticEquals(groupingInput) => named.toAttribute
    }.getOrElse(return None)
    val outerDimensionKey = equiKeyFromOtherSide(
      topJoin.condition.get,
      outerSide,
      aggregateSide,
      groupingOutput).getOrElse(return None)
    val (outerFact, dimension, dimensionKey) =
      findOuterFactDimensionJoin(outerSide, outerDimensionKey).getOrElse(return None)
    if (
      !sameSingleLeafRelation(outerFact, aggregate.child) ||
      !safeDimension(dimension, dimensionKey) ||
      !worthDuplicatingDimension(dimension, aggregate)
    ) {
      return None
    }

    val filteredInput = Join(
      aggregate.child,
      dimension,
      LeftSemi,
      Some(EqualTo(groupingInput, dimensionKey)),
      JoinHint(None, Some(HintInfo(strategy = Some(BROADCAST)))))
    val rewrittenAggregate = aggregate.copy(child = filteredInput)
    val rewrittenAggregateSide = aggregateSide.transformDown {
      case node if node eq aggregate => rewrittenAggregate
    }
    logWarning(
      "GpuPushSelectiveDimensionFilterIntoAggregate: pushed selective keyset below aggregate " +
        s"groupingKey=${groupingInput.name} dimensionKey=${dimensionKey.name} " +
        s"dimensionBytes=${estimatedOutputBytes(dimension).getOrElse(-1)}")

    if (aggregateSide eq topJoin.left) {
      Some(topJoin.copy(left = rewrittenAggregateSide))
    } else {
      Some(topJoin.copy(right = rewrittenAggregateSide))
    }
  }

  private def unwrapAggregate(plan: LogicalPlan): Option[Aggregate] = plan match {
    case aggregate: Aggregate => Some(aggregate)
    case Project(projectList, child)
        if projectList.forall(e => e.deterministic && !hasSubquery(e)) =>
      unwrapAggregate(child)
    case Filter(condition, child) if condition.deterministic && !hasSubquery(condition) =>
      unwrapAggregate(child)
    case SubqueryAlias(_, child) => unwrapAggregate(child)
    case _ => None
  }

  private def safeAggregate(aggregate: Aggregate): Boolean = {
    aggregate.groupingExpressions match {
      case Seq(_: Attribute) =>
        aggregate.expressions.forall(_.deterministic) &&
          !aggregate.child.exists(_.isInstanceOf[Join]) &&
          singleLeaf(aggregate.child).isDefined
      case _ => false
    }
  }

  private def equiKeyFromOtherSide(
      condition: Expression,
      outerSide: LogicalPlan,
      aggregateSide: LogicalPlan,
      groupingOutput: Attribute): Option[Attribute] = {
    splitAnd(condition).collectFirst {
      case EqualTo(left: Attribute, right: Attribute)
          if left.semanticEquals(groupingOutput) &&
            aggregateSide.outputSet.contains(left) && outerSide.outputSet.contains(right) =>
        right
      case EqualTo(left: Attribute, right: Attribute)
          if right.semanticEquals(groupingOutput) &&
            aggregateSide.outputSet.contains(right) && outerSide.outputSet.contains(left) =>
        left
    }
  }

  private def findOuterFactDimensionJoin(
      plan: LogicalPlan,
      dimensionKey: Attribute): Option[(LogicalPlan, LogicalPlan, Attribute)] = plan match {
    case Project(_, child) if child.outputSet.contains(dimensionKey) =>
      findOuterFactDimensionJoin(child, dimensionKey)
    case Filter(_, child) if child.outputSet.contains(dimensionKey) =>
      findOuterFactDimensionJoin(child, dimensionKey)
    case SubqueryAlias(_, child) if child.outputSet.contains(dimensionKey) =>
      findOuterFactDimensionJoin(child, dimensionKey)
    case Join(left, right, Inner, Some(condition), _) =>
      val orientation =
        if (right.outputSet.contains(dimensionKey)) Some(left -> right)
        else if (left.outputSet.contains(dimensionKey)) Some(right -> left)
        else None
      orientation.filter {
        case (fact, dimension) => splitAnd(condition).exists {
          case EqualTo(leftKey: Attribute, rightKey: Attribute) =>
            (fact.outputSet.contains(leftKey) && rightKey.semanticEquals(dimensionKey) &&
              dimension.outputSet.contains(rightKey)) ||
              (fact.outputSet.contains(rightKey) && leftKey.semanticEquals(dimensionKey) &&
                dimension.outputSet.contains(leftKey))
          case _ => false
        }
      }.map { case (fact, dimension) => (fact, dimension, dimensionKey) }
    case _ => None
  }

  private def safeDimension(dimension: LogicalPlan, key: Attribute): Boolean = {
    dimension.output.size == 1 && dimension.output.head.semanticEquals(key) &&
      singleLeaf(dimension).isDefined &&
      !dimension.exists {
        case _: LeafNode => false
        case project: Project =>
          !project.projectList.forall(e => e.deterministic && !hasSubquery(e))
        case filter: Filter => !filter.condition.deterministic || hasSubquery(filter.condition)
        case _: SubqueryAlias => false
        case _ => true
      } && dimension.exists {
        case Filter(condition, _) => splitAnd(condition).exists {
          case EqualTo(_: Attribute, literal: Literal) => literal.value != null
          case EqualTo(literal: Literal, _: Attribute) => literal.value != null
          case _ => false
        }
        case _ => false
      }
  }

  private def worthDuplicatingDimension(
      dimension: LogicalPlan,
      aggregate: Aggregate): Boolean = {
    val threshold = BigInt(spark.sessionState.conf.autoBroadcastJoinThreshold)
    val outputRows = estimatedOutputRows(dimension).getOrElse(return false)
    val outputBytes = estimatedOutputBytes(dimension).getOrElse(return false)
    val dimensionScanBytes = leafBytes(dimension)
    val factScanBytes = leafBytes(aggregate.child)
    threshold >= 0 && outputRows > 0 && outputRows < maxBroadcastRows &&
      outputBytes > 0 && outputBytes <= threshold &&
      dimensionScanBytes > 0 && factScanBytes > 0 &&
      BigDecimal(dimensionScanBytes) <= BigDecimal(factScanBytes) * maxDimensionScanRatio
  }

  private def estimatedOutputBytes(plan: LogicalPlan): Option[BigInt] = {
    metadata.flatMap(_.estimate(plan)).map(_.sizeInBytes).orElse {
      val bytes = plan.stats.sizeInBytes
      if (bytes > 0 && bytes < BigInt(Long.MaxValue)) Some(bytes) else None
    }
  }

  private def estimatedOutputRows(plan: LogicalPlan): Option[BigInt] = {
    metadata.flatMap(_.estimateRows(plan))
      .orElse(plan.stats.rowCount.filter(_ > 0))
      // Project and Filter can drop rowCount from Spark statistics. Their single input leaf is a
      // conservative upper bound because neither wrapper can add rows.
      .orElse(singleLeaf(plan).flatMap(_.stats.rowCount.filter(_ > 0)))
  }

  private def leafBytes(plan: LogicalPlan): BigInt =
    singleLeaf(plan).map(_.stats.sizeInBytes).getOrElse(BigInt(0))

  private def sameSingleLeafRelation(left: LogicalPlan, right: LogicalPlan): Boolean =
    (singleLeaf(left), singleLeaf(right)) match {
      case (Some(leftLeaf), Some(rightLeaf)) => leftLeaf.sameResult(rightLeaf)
      case _ => false
    }

  private def singleLeaf(plan: LogicalPlan): Option[LeafNode] = {
    val leaves = plan.collect { case leaf: LeafNode => leaf }
    if (leaves.size == 1) Some(leaves.head) else None
  }

  private def splitAnd(expression: Expression): Seq[Expression] = expression match {
    case And(left, right) => splitAnd(left) ++ splitAnd(right)
    case other => Seq(other)
  }

  private def hasSubquery(expression: Expression): Boolean = expression.exists {
    case _: SubqueryExpression => true
    case _ => false
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
          _.isInstanceOf[GpuPushSelectiveDimensionFilterIntoAggregate])) {
        experimental.extraOptimizations = experimental.extraOptimizations :+ this
      }
    }
  }
}
