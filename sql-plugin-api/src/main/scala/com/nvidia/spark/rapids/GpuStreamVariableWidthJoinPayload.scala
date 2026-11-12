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
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualTo, Expression, IsNotNull}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, BROADCAST, Filter, HintInfo}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{BinaryType, StringType}

/**
 * Stream an unfiltered variable-width relation while broadcasting its aggregate-filtered partner.
 *
 * This explicit experimental hint targets manually qualified build branches. Callers must qualify
 * build capacity; native shuffle-broadcast admission retains its existing behavior. The original
 * join condition, output ordering and duplicate multiplicity remain intact.
 */
case class GpuStreamVariableWidthJoinPayload(spark: SparkSession)
  extends Rule[LogicalPlan] with Logging {

  private val enabled = "spark.rapids.sql.optimizer.streamVariableWidthJoinPayload.enabled"

  private val experimental = spark.experimental
  experimental.synchronized {
    if (!experimental.extraOptimizations.exists(
        _.isInstanceOf[GpuStreamVariableWidthJoinPayload])) {
      experimental.extraOptimizations = experimental.extraOptimizations :+ this
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!spark.conf.get(enabled, "false").toBoolean || !plan.resolved || plan.isStreaming) {
      return plan
    }
    plan.transformUp {
      case join: Join if join.joinType == Inner && noStrategyHint(join) &&
          join.condition.forall(_.deterministic) =>
        val pairs = join.condition.toSeq.flatMap(splitAnd).collect {
          case EqualTo(left: Attribute, right: Attribute)
              if join.left.outputSet.contains(left) && join.right.outputSet.contains(right) =>
            left -> right
          case EqualTo(right: Attribute, left: Attribute)
              if join.left.outputSet.contains(left) && join.right.outputSet.contains(right) =>
            left -> right
        }
        if (pairs.nonEmpty && streamable(join.left, pairs.map(_._1)) &&
            aggregateFiltered(join.right)) {
          logWarning("GpuStreamVariableWidthJoinPayload: stream left payload; broadcast right")
          join.copy(hint = join.hint.copy(rightHint = Some(HintInfo(strategy = Some(BROADCAST)))))
        } else if (pairs.nonEmpty && streamable(join.right, pairs.map(_._2)) &&
            aggregateFiltered(join.left)) {
          logWarning("GpuStreamVariableWidthJoinPayload: stream right payload; broadcast left")
          join.copy(hint = join.hint.copy(leftHint = Some(HintInfo(strategy = Some(BROADCAST)))))
        } else {
          join
        }
    }
  }

  private def noStrategyHint(join: Join): Boolean =
    !join.hint.leftHint.exists(_.strategy.isDefined) &&
      !join.hint.rightHint.exists(_.strategy.isDefined)

  private def streamable(plan: LogicalPlan, keys: Seq[Attribute]): Boolean = {
    val payload = plan.output.exists { attribute =>
      !keys.exists(_.semanticEquals(attribute)) &&
        (attribute.dataType == StringType || attribute.dataType == BinaryType)
    }
    payload && !plan.exists {
      case _: LogicalRelation => false
      case project: Project => !project.expressions.forall(_.deterministic)
      case _: SubqueryAlias => false
      case Filter(condition, _) => !splitAnd(condition).forall(_.isInstanceOf[IsNotNull])
      case _ => true
    }
  }

  private def aggregateFiltered(plan: LogicalPlan): Boolean = {
    val deterministic = !plan.exists(_.expressions.exists(!_.deterministic))
    deterministic && plan.exists {
      case Filter(condition, child) if child.exists(_.isInstanceOf[Aggregate]) =>
        splitAnd(condition).exists(!_.isInstanceOf[IsNotNull])
      case _ => false
    }
  }

  private def splitAnd(expression: Expression): Seq[Expression] = expression match {
    case And(left, right) => splitAnd(left) ++ splitAnd(right)
    case other => Seq(other)
  }
}
