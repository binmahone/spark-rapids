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
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, JoinHint, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.JOIN
import org.apache.spark.sql.internal.SQLConf

/**
 * Rewrite `LeftSemiJoin(left, right, equi-condition)` into
 * `InnerJoin(left, Aggregate(rightKeys -> rightKeys, right), equi-condition)` when the right side
 * is estimated to be too large. Mirrors what Presto's optimizer auto-produces for `EXISTS`
 * subqueries (TPC-H Q4: the ~28 GB lineitem existence-set build becomes a ~1.5 GB
 * distinct-orderkey build), and matches the SQL-level `IN (... GROUP BY key)` rewrite without
 * touching the query.
 *
 * Why this helps cuDF: Spark Catalyst pins LeftSemi to BuildRight as a semantic choice
 * ("right is the existence set"), independent of cost. When the right side is huge (lineitem after
 * only a date-window filter), cudf::hash_join must materialize the whole build as one GPU
 * allocation. Adding `DISTINCT(rightKeys)` collapses it to one row per join-key value, shrinking
 * the build (and enabling BHJ or a smaller SHJ hash table).
 *
 * Conservative match -- only triggers when ALL hold:
 *   - LeftSemiJoin with a single equi-join condition tree (AND of `attr = attr` pairs)
 *   - Each equi conjunct references exactly one attribute from each side (no expressions, no
 *     correlated non-key predicates such as `l_suppkey <> l1.l_suppkey`)
 *   - No user-supplied JoinHint on the Join
 *   - `right.stats.sizeInBytes` exceeds `autoBroadcastJoinThreshold * thresholdMultiplier`
 *     (default 2x). For unanalyzed parquet temp views Spark reports raw file-size sum, a usable
 *     proxy.
 *
 * Gated by `spark.rapids.sql.optimizer.rewriteLargeLeftSemi.enabled` (default false). Multiplier
 * tunable via `spark.rapids.sql.optimizer.rewriteLargeLeftSemi.thresholdMultiplier` (default 2).
 * Mixed-semi-join with a non-equi conjunct (e.g. TPC-H Q21) is intentionally NOT covered.
 *
 * Ported from gluten's RewriteLargeLeftSemiToInnerDistinct for the spark-rapids plan path.
 */
case class GpuRewriteLargeLeftSemiToInnerDistinct(spark: SparkSession)
  extends Rule[LogicalPlan]
  with Logging {

  private val confKey = "spark.rapids.sql.optimizer.rewriteLargeLeftSemi.enabled"
  private val confDefault = "false"
  private val multiplierKey =
    "spark.rapids.sql.optimizer.rewriteLargeLeftSemi.thresholdMultiplier"
  private val multiplierDefault = "2"

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Self-register into spark.experimental.extraOptimizations on first apply so this rule lands
    // in the "User Provided Optimizers" batch (FixedPoint), which runs AFTER the optimizer's
    // "Subquery" batch where RewriteSubquery converts EXISTS into LeftSemiJoin -- the only state
    // where our pattern matches. injectOptimizerRule alone lands in "Operator Optimization"
    // (before Subquery), where the plan is still Filter(Exists(...)) and never matches.
    // Idempotent (reference-equality) and synchronized against concurrent first-query startup.
    val experimental = spark.experimental
    experimental.synchronized {
      if (!experimental.extraOptimizations.exists(_ eq this)) {
        experimental.extraOptimizations = experimental.extraOptimizations :+ this
        logDebug("GpuRewriteLargeLeftSemiToInnerDistinct: self-registered into " +
          "spark.experimental.extraOptimizations for post-RewriteSubquery pass")
      }
    }

    val conf = SQLConf.get
    val enabled = conf.getConfString(confKey, confDefault).toBoolean
    if (!enabled) {
      plan
    } else {
      val multiplier = scala.util
        .Try(conf.getConfString(multiplierKey, multiplierDefault).toLong)
        .getOrElse(multiplierDefault.toLong)
      val sizeThreshold = conf.autoBroadcastJoinThreshold * multiplier

      plan.transformDownWithPruning(_.containsPattern(JOIN)) {
        case j @ Join(left, right, LeftSemi, Some(condition), hint)
            if !hasUserHint(hint) && right.stats.sizeInBytes > BigInt(sizeThreshold) =>
          extractEquiKeys(condition, left.outputSet, right.outputSet) match {
            case Some((_, rightKeys)) =>
              val deduplicatedRight = Aggregate(
                groupingExpressions = rightKeys,
                aggregateExpressions = rightKeys,
                child = right
              )
              logDebug("GpuRewriteLargeLeftSemiToInnerDistinct: rewrote LeftSemi -> Inner with " +
                s"DISTINCT on right keys ${rightKeys.map(_.name).mkString("[", ",", "]")}; " +
                s"right.sizeInBytes=${right.stats.sizeInBytes} > threshold=$sizeThreshold")
              Join(left, deduplicatedRight, Inner, Some(condition), JoinHint.NONE)
            case None =>
              // Preserve the original node so TreeNodeTags and referential identity survive.
              j
          }
      }
    }
  }

  /**
   * Split `cond` into top-level conjuncts and verify every conjunct is `EqualTo(leftAttr,
   * rightAttr)` (or the symmetric form) where one side references only `leftOut` and the other
   * only `rightOut`. Returns Some((leftKeys, rightKeys)) on match, None otherwise.
   */
  private def extractEquiKeys(
      cond: Expression,
      leftOut: AttributeSet,
      rightOut: AttributeSet): Option[(Seq[Attribute], Seq[Attribute])] = {
    val conjuncts = splitAnd(cond)
    val pairs = conjuncts.foldLeft(Option(Vector.empty[(Attribute, Attribute)])) {
      case (Some(acc), EqualTo(l: Attribute, r: Attribute))
          if leftOut.contains(l) && rightOut.contains(r) => Some(acc :+ (l -> r))
      case (Some(acc), EqualTo(l: Attribute, r: Attribute))
          if leftOut.contains(r) && rightOut.contains(l) => Some(acc :+ (r -> l))
      case _ => None
    }
    pairs.filter(_.nonEmpty).map { allPairs =>
      val uniquePairs = allPairs.foldLeft(Vector.empty[(Attribute, Attribute)]) {
        case (acc, pair) if acc.exists(_._2.exprId == pair._2.exprId) => acc
        case (acc, pair) => acc :+ pair
      }
      (uniquePairs.map(_._1), uniquePairs.map(_._2))
    }
  }

  private def splitAnd(expr: Expression): Seq[Expression] = expr match {
    case And(l, r) => splitAnd(l) ++ splitAnd(r)
    case other => Seq(other)
  }

  private def hasUserHint(hint: JoinHint): Boolean = {
    hint.leftHint.exists(_.strategy.isDefined) ||
    hint.rightHint.exists(_.strategy.isDefined)
  }
}
