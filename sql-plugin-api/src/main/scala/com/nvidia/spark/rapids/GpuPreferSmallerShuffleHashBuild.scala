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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, Join, JoinHint, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.logical.SHUFFLE_HASH
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.JOIN

/**
 * Prefer the smaller trusted-statistics input as the shuffled-hash build side.
 *
 * Spark can choose the syntactic right input even when a filtered multi-relation subtree is much
 * smaller. A side-specific SHUFFLE_HASH hint preserves join order and output attributes while
 * making the build-side decision explicit. The rule applies only to unhinted inner equi-joins for
 * which both complete input estimates are available. It does not override an input that remains
 * eligible for ordinary Spark broadcast.
 */
case class GpuPreferSmallerShuffleHashBuild(spark: SparkSession)
  extends Rule[LogicalPlan]
  with Logging {

  private val enabledKey =
    "spark.rapids.sql.optimizer.preferSmallerShuffleHashBuild.enabled"
  private val minRatioKey =
    "spark.rapids.sql.optimizer.preferSmallerShuffleHashBuild.minSizeRatio"
  private val metadata = GpuOptimizerTrustedMetadata.fromSession(spark)

  registerPostCboPass()

  override def apply(plan: LogicalPlan): LogicalPlan = {
    registerPostCboPass()
    if (!enabled || !plan.resolved || plan.isStreaming || metadata.isEmpty) {
      return plan
    }
    plan.transformUpWithPruning(_.containsPattern(JOIN)) {
      case join @ Join(left, right, Inner, Some(condition), JoinHint.NONE)
          if isEquiJoin(condition, left, right) =>
        chooseBuildSide(join).getOrElse(join)
    }
  }

  private def chooseBuildSide(join: Join): Option[LogicalPlan] = {
    val trusted = metadata.get
    val left = trusted.estimate(join.left).getOrElse(return None)
    val right = trusted.estimate(join.right).getOrElse(return None)
    if (left.sizeInBytes <= 0 || right.sizeInBytes <= 0) {
      return None
    }

    val (smallerOnLeft, smaller, larger) =
      if (left.sizeInBytes <= right.sizeInBytes) {
        (true, left, right)
      } else {
        (false, right, left)
      }
    if (ordinaryBroadcastEligible(smaller.sizeInBytes) ||
        BigDecimal(larger.sizeInBytes) < BigDecimal(smaller.sizeInBytes) * minSizeRatio) {
      return None
    }

    val shuffleHash = HintInfo(strategy = Some(SHUFFLE_HASH))
    val hint = if (smallerOnLeft) {
      JoinHint(Some(shuffleHash), None)
    } else {
      JoinHint(None, Some(shuffleHash))
    }
    logWarning(
      "GpuPreferSmallerShuffleHashBuild: selected trusted smaller build side " +
        s"side=${if (smallerOnLeft) "left" else "right"} " +
        s"leftRows=${left.rows} leftBytes=${left.sizeInBytes} " +
        s"rightRows=${right.rows} rightBytes=${right.sizeInBytes} " +
        s"minSizeRatio=$minSizeRatio")
    Some(join.copy(hint = hint))
  }

  private def ordinaryBroadcastEligible(bytes: BigInt): Boolean = {
    val threshold = BigInt(spark.sessionState.conf.autoBroadcastJoinThreshold)
    threshold >= 0 && bytes <= threshold
  }

  private def isEquiJoin(condition: Expression, left: LogicalPlan, right: LogicalPlan): Boolean = {
    val predicates = splitAnd(condition)
    predicates.nonEmpty && predicates.forall {
      case EqualTo(leftKey: Attribute, rightKey: Attribute) =>
        (left.outputSet.contains(leftKey) && right.outputSet.contains(rightKey)) ||
          (left.outputSet.contains(rightKey) && right.outputSet.contains(leftKey))
      case _ => false
    }
  }

  private def splitAnd(expression: Expression): Seq[Expression] = expression match {
    case And(left, right) => splitAnd(left) ++ splitAnd(right)
    case other => Seq(other)
  }

  private def minSizeRatio: BigDecimal = {
    Try(BigDecimal(spark.sessionState.conf.getConfString(minRatioKey, "1.25")))
      .filter(_ >= BigDecimal(1))
      .getOrElse(BigDecimal("1.25"))
  }

  private def enabled: Boolean =
    Try(spark.sessionState.conf.getConfString(enabledKey, "false").toBoolean).getOrElse(false)

  private def registerPostCboPass(): Unit = {
    if (!enabled) {
      return
    }
    val experimental = spark.experimental
    experimental.synchronized {
      if (!experimental.extraOptimizations.exists(
          _.isInstanceOf[GpuPreferSmallerShuffleHashBuild])) {
        experimental.extraOptimizations = experimental.extraOptimizations :+ this
      }
    }
  }
}
