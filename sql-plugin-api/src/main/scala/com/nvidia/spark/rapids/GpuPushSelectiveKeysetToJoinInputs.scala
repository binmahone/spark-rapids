/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids

import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeSet, Contains}
import org.apache.spark.sql.catalyst.expressions.{EqualNullSafe, EqualTo, Expression, In, InSet}
import org.apache.spark.sql.catalyst.expressions.{Literal, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, Filter, HintInfo, Join, JoinHint}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.JOIN

/**
 * Apply an already-required selective keyset to another large input before a many-key join.
 *
 * If one side of an inner join already contains `probe JOIN filtered_dimension` and the other
 * side joins on a key equivalent to the dimension key, every final row from the other side must
 * have a key present in that dimension. A left-semi copy of the keyset can therefore discard
 * irrelevant rows before the expensive join without changing multiplicity. The original inner
 * dimension join remains in place and continues to carry the dimension's bag semantics.
 *
 * A keyset whose conservative row estimate crosses Spark's 512-million-row driver-broadcast
 * limit is accepted only when the explicitly enabled shuffle-broadcast path is trusted and its
 * configured size limit admits the build. That path assembles the build on executors and does not
 * materialize Spark's driver-side broadcast relation.
 */
case class GpuPushSelectiveKeysetToJoinInputs(spark: SparkSession)
  extends Rule[LogicalPlan]
  with Logging {

  private val enabledKey =
    "spark.rapids.sql.optimizer.pushSelectiveKeysetToJoinInputs.enabled"
  private val maxBroadcastRows = BigInt(512000000)
  private val maxShuffleBroadcastRows = BigInt(Int.MaxValue)
  private val maxInValues = 16
  private val metadata = GpuOptimizerTrustedMetadata.fromSession(spark)

  registerPostCboPass()

  override def apply(plan: LogicalPlan): LogicalPlan = {
    registerPostCboPass()
    if (!enabled || !plan.resolved || plan.isStreaming || metadata.isEmpty) {
      return plan
    }
    plan.transformUpWithPruning(_.containsPattern(JOIN)) {
      case join: Join => rewriteJoin(join).getOrElse(join)
    }
  }

  private case class DimensionJoin(
      join: Join,
      dimension: LogicalPlan,
      dimensionKey: Attribute,
      probeKey: Attribute,
      dimensionOnLeft: Boolean,
      estimate: GpuOptimizerTrustedMetadata#Estimate)

  private def rewriteJoin(topJoin: Join): Option[LogicalPlan] = {
    if (topJoin.joinType != Inner || topJoin.condition.isEmpty) {
      return None
    }
    rewriteOrientation(topJoin, topJoin.left, topJoin.right, containingOnLeft = true)
      .orElse(rewriteOrientation(topJoin, topJoin.right, topJoin.left, containingOnLeft = false))
  }

  private def rewriteOrientation(
      topJoin: Join,
      containingSide: LogicalPlan,
      targetSide: LogicalPlan,
      containingOnLeft: Boolean): Option[LogicalPlan] = {
    if (!safeTarget(targetSide)) {
      return None
    }
    val topEdges = equiEdges(topJoin.condition.get, containingSide.outputSet, targetSide.outputSet)
    if (topEdges.isEmpty) {
      return None
    }
    val equivalence = equivalenceClasses(containingSide)
    val candidates = dimensionJoins(containingSide).flatMap {
      candidate =>
        topEdges.collectFirst {
          case (containingKey, targetKey)
              if equivalence.connected(candidate.dimensionKey, containingKey) &&
                worthPrefiltering(candidate.estimate, targetSide) =>
            (candidate, targetKey)
        }
    }
    if (candidates.size != 1) {
      return None
    }

    val (candidate, targetKey) = candidates.head
    val freshDimensionKey = Alias(candidate.dimensionKey, candidate.dimensionKey.name)()
    val keyset = Project(Seq(freshDimensionKey), candidate.dimension)
    val broadcastRight = JoinHint(None, Some(HintInfo(strategy = Some(BROADCAST))))
    val filteredTarget = Join(
      targetSide,
      keyset,
      LeftSemi,
      Some(EqualTo(targetKey, freshDimensionKey.toAttribute)),
      broadcastRight)
    val hintedContaining = containingSide.transformDown {
      case join: Join if join eq candidate.join =>
        val hint = if (candidate.dimensionOnLeft) {
          JoinHint(Some(HintInfo(strategy = Some(BROADCAST))), None)
        } else {
          broadcastRight
        }
        join.copy(hint = hint)
    }
    logWarning(
      "GpuPushSelectiveKeysetToJoinInputs: applied selective keyset to both join inputs " +
        s"dimensionKey=${candidate.dimensionKey.name} targetKey=${targetKey.name} " +
        s"rows=${candidate.estimate.rows} bytes=${candidate.estimate.sizeInBytes} " +
        s"shuffleBroadcast=${usesShuffleBroadcast(candidate.estimate)}")

    if (containingOnLeft) {
      Some(topJoin.copy(left = hintedContaining, right = filteredTarget))
    } else {
      Some(topJoin.copy(left = filteredTarget, right = hintedContaining))
    }
  }

  private def dimensionJoins(plan: LogicalPlan): Seq[DimensionJoin] = plan.collect {
    case join @ Join(left, right, Inner, Some(condition), hint)
        if hint == JoinHint.NONE =>
      Seq(
        dimensionJoin(join, left, right, condition, dimensionOnLeft = true),
        dimensionJoin(join, right, left, condition, dimensionOnLeft = false)).flatten
  }.flatten

  private def dimensionJoin(
      join: Join,
      dimension: LogicalPlan,
      probe: LogicalPlan,
      condition: Expression,
      dimensionOnLeft: Boolean): Option[DimensionJoin] = {
    if (!safeSelectiveDimension(dimension)) {
      return None
    }
    val estimate = metadata.flatMap(_.estimate(dimension)).filter(admittedBuild)
      .getOrElse(return None)
    equiEdges(condition, dimension.outputSet, probe.outputSet).collectFirst {
      case (dimensionKey, probeKey) =>
        DimensionJoin(join, dimension, dimensionKey, probeKey, dimensionOnLeft, estimate)
    }
  }

  private def admittedBuild(estimate: GpuOptimizerTrustedMetadata#Estimate): Boolean = {
    val ordinaryThreshold = BigInt(spark.sessionState.conf.autoBroadcastJoinThreshold)
    val ordinary = estimate.rows > 0 && estimate.rows < maxBroadcastRows &&
      ordinaryThreshold >= 0 && estimate.sizeInBytes > 0 &&
      estimate.sizeInBytes <= ordinaryThreshold
    ordinary || usesShuffleBroadcast(estimate)
  }

  private def usesShuffleBroadcast(estimate: GpuOptimizerTrustedMetadata#Estimate): Boolean = {
    val enabled = booleanConf("spark.rapids.shuffle.broadcast.enabled", default = false)
    val trustSparkPlan = booleanConf(
      "spark.rapids.shuffle.broadcast.trustSparkPlan.enabled", default = false)
    val maxBytes = bytesConf("spark.rapids.shuffle.broadcast.maxSize", "8g")
    enabled && trustSparkPlan && estimate.rows > 0 &&
      estimate.rows <= maxShuffleBroadcastRows && estimate.sizeInBytes > 0 &&
      estimate.sizeInBytes <= maxBytes
  }

  private def worthPrefiltering(
      estimate: GpuOptimizerTrustedMetadata#Estimate,
      target: LogicalPlan): Boolean = {
    val targetBytes = leafBytes(target)
    targetBytes > 0 && targetBytes >= estimate.sizeInBytes * 4
  }

  private def safeTarget(plan: LogicalPlan): Boolean = {
    val leaves = plan.collect { case leaf: LeafNode => leaf }
    leaves.size == 1 && !plan.exists {
      case _: LeafNode => false
      case project: Project => !project.projectList.forall(safeExpression)
      case filter: Filter => !safeExpression(filter.condition)
      case _: SubqueryAlias => false
      case _ => true
    }
  }

  private def safeSelectiveDimension(plan: LogicalPlan): Boolean = {
    val leaves = plan.collect { case leaf: LeafNode => leaf }
    leaves.size == 1 && plan.output.nonEmpty && plan.output.size <= 4 &&
      !plan.exists {
        case _: LeafNode => false
        case project: Project => !project.projectList.forall(safeExpression)
        case filter: Filter => !safeExpression(filter.condition)
        case _: SubqueryAlias => false
        case _ => true
      } && plan.exists {
        case Filter(condition, _) => splitAnd(condition).exists(selectiveLiteralPredicate)
        case _ => false
      }
  }

  private def safeExpression(expression: Expression): Boolean =
    expression.deterministic && !expression.exists {
      case _: SubqueryExpression => true
      case _ => false
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

  private def equiEdges(
      condition: Expression,
      left: AttributeSet,
      right: AttributeSet): Seq[(Attribute, Attribute)] = splitAnd(condition).collect {
    case EqualTo(leftKey: Attribute, rightKey: Attribute)
        if left.contains(leftKey) && right.contains(rightKey) => (leftKey, rightKey)
    case EqualTo(leftKey: Attribute, rightKey: Attribute)
        if left.contains(rightKey) && right.contains(leftKey) => (rightKey, leftKey)
  }

  private def equivalenceClasses(plan: LogicalPlan): EquivalenceClasses = {
    val classes = new EquivalenceClasses

    def addEqualities(expression: Expression): Unit = {
      splitAnd(expression).foreach {
        case EqualTo(left: Attribute, right: Attribute) => classes.union(left, right)
        case _ =>
      }
    }

    def visit(current: LogicalPlan): Unit = current match {
      case Join(left, right, Inner, condition, _) =>
        condition.foreach(addEqualities)
        visit(left)
        visit(right)
      case Filter(condition, child) =>
        addEqualities(condition)
        visit(child)
      case Project(_, child) => visit(child)
      case SubqueryAlias(_, child) => visit(child)
      case _ =>
    }

    visit(plan)
    classes
  }

  private def leafBytes(plan: LogicalPlan): BigInt =
    plan.collect { case leaf: LeafNode => leaf.stats.sizeInBytes }.sum

  private def splitAnd(expression: Expression): Seq[Expression] = expression match {
    case And(left, right) => splitAnd(left) ++ splitAnd(right)
    case other => Seq(other)
  }

  private def booleanConf(key: String, default: Boolean): Boolean =
    Try(spark.sessionState.conf.getConfString(key, default.toString).toBoolean).getOrElse(default)

  private def bytesConf(key: String, default: String): BigInt =
    Try(BigInt(JavaUtils.byteStringAsBytes(
      spark.sessionState.conf.getConfString(key, default)))).getOrElse(
      BigInt(JavaUtils.byteStringAsBytes(default)))

  private def enabled: Boolean = booleanConf(enabledKey, default = false)

  private def registerPostCboPass(): Unit = {
    if (!enabled) {
      return
    }
    val experimental = spark.experimental
    experimental.synchronized {
      if (!experimental.extraOptimizations.exists(
          _.isInstanceOf[GpuPushSelectiveKeysetToJoinInputs])) {
        experimental.extraOptimizations = experimental.extraOptimizations :+ this
      }
    }
  }

  final private class EquivalenceClasses {
    private val parent = scala.collection.mutable.HashMap.empty[Long, Long]

    private def find(value: Long): Long = {
      val current = parent.getOrElseUpdate(value, value)
      if (current == value) value else {
        val root = find(current)
        parent.update(value, root)
        root
      }
    }

    def union(left: Attribute, right: Attribute): Unit = {
      val leftRoot = find(left.exprId.id)
      val rightRoot = find(right.exprId.id)
      if (leftRoot != rightRoot) parent.update(leftRoot, rightRoot)
    }

    def connected(left: Attribute, right: Attribute): Boolean =
      left.semanticEquals(right) || find(left.exprId.id) == find(right.exprId.id)
  }
}
