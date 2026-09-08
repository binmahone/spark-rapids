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
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, JoinHint, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Deduplicate a large single-key left-anti build side when trusted statistics prove substantial
 * repetition. Left-anti semantics depend only on key existence, so duplicate right rows cannot
 * affect the result. Statistics are used only as a cost guard, not as a semantic assumption.
 */
case class GpuDeduplicateLargeLeftAntiBuildSide(spark: SparkSession)
  extends Rule[LogicalPlan]
  with Logging {

  private val minReductionRatio = BigInt(2)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.resolved) {
      plan
    } else {
      GpuOptimizerTrustedMetadata.fromSession(spark) match {
        case Some(metadata) => plan.transformDown {
          case join @ Join(left, right, LeftAnti, Some(condition), JoinHint.NONE) =>
            rightKey(condition, left.outputSet, right.outputSet) match {
              case Some(key) if shouldDeduplicate(right, key, metadata) =>
                logWarning(
                  "GpuDeduplicateLargeLeftAntiBuildSide: deduplicated right existence key " +
                    s"key=${key.name} rows=${metadata.estimateRows(right).get} " +
                    s"distinct=${metadata.estimateDistinct(right, key).get}")
                join.copy(right = Aggregate(Seq(key), Seq(key), right))
              case _ => join
            }
        }
        case None => plan
      }
    }
  }

  private def shouldDeduplicate(
      right: LogicalPlan,
      key: Attribute,
      metadata: GpuOptimizerTrustedMetadata): Boolean = {
    val estimate = for {
      rows <- metadata.estimateRows(right)
      distinct <- metadata.estimateDistinct(right, key)
    } yield rows -> distinct
    estimate.exists {
      case (rows, distinct) => distinct > 0 && rows >= distinct * minReductionRatio
    }
  }

  private def rightKey(
      condition: Expression,
      leftOutput: AttributeSet,
      rightOutput: AttributeSet): Option[Attribute] = {
    splitAnd(condition) match {
      case Seq(EqualTo(left: Attribute, right: Attribute))
          if leftOutput.contains(left) && rightOutput.contains(right) => Some(right)
      case Seq(EqualTo(left: Attribute, right: Attribute))
          if leftOutput.contains(right) && rightOutput.contains(left) => Some(left)
      case _ => None
    }
  }

  private def splitAnd(expression: Expression): Seq[Expression] = expression match {
    case And(left, right) => splitAnd(left) ++ splitAnd(right)
    case other => Seq(other)
  }
}
