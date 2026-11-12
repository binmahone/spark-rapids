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
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, BloomFilterMightContain}
import org.apache.spark.sql.catalyst.expressions.{Contains, EqualTo, Expression, IsNotNull, Literal}
import org.apache.spark.sql.catalyst.expressions.{ScalarSubquery, StartsWith, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.XxHash64
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.logical.{Join, Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * Add bounded Bloom summaries of already-required keys before large join inputs are exchanged.
 *
 * The original join remains responsible for exact matching, duplicates and residual predicates.
 * A summary can admit false positives but cannot discard a matching equality key. This prototype
 * accepts repeatable, deterministic single-relation branches only; it does not move operator
 * state or bound the source branch's own aggregation memory.
 */
case class GpuRequiredKeyBloomFilter(spark: SparkSession)
  extends Rule[LogicalPlan] with Logging {

  private val prefix = "spark.rapids.sql.optimizer.requiredKeyBloomFilter."
  private val hashSeed = 42L

  registerPostSubqueryPass()

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!spark.conf.get(prefix + "enabled", "false").toBoolean ||
        !plan.resolved || plan.isStreaming) {
      return plan
    }
    val items = spark.conf.get(prefix + "expectedItems", "1000000").toLong
    val bits = spark.conf.get(prefix + "numBits", "8388608").toLong
    require(items > 0 && bits >= 64 && bits <= 67108864,
      "Required-key Bloom parameters must be positive and the filter at most 8 MiB")

    plan.transformUp {
      case join: Join if (join.joinType == Inner || join.joinType == LeftSemi) &&
          join.condition.forall(_.deterministic) =>
        val pairs = join.condition.toSeq.flatMap(splitAnd).collect {
          case EqualTo(left: Attribute, right: Attribute)
              if join.left.outputSet.contains(left) && join.right.outputSet.contains(right) &&
                left.dataType == right.dataType => left -> right
          case EqualTo(right: Attribute, left: Attribute)
              if join.left.outputSet.contains(left) && join.right.outputSet.contains(right) &&
                left.dataType == right.dataType => left -> right
        }
        if (pairs.isEmpty) {
          join
        } else if (eligibleSource(join.right) && eligibleTarget(join.left)) {
          join.copy(left = prefilter(join.left, pairs.map(_._1), join.right,
            pairs.map(_._2), items, bits))
        } else if (join.joinType == Inner && eligibleSource(join.left) &&
            eligibleTarget(join.right)) {
          join.copy(right = prefilter(join.right, pairs.map(_._2), join.left,
            pairs.map(_._1), items, bits))
        } else {
          join
        }
    }
  }

  private def prefilter(
      target: LogicalPlan,
      targetKeys: Seq[Attribute],
      source: LogicalPlan,
      sourceKeys: Seq[Attribute],
      items: Long,
      bits: Long): LogicalPlan = {
    val summary = Alias(BloomFilterAggregate(
      XxHash64(sourceKeys, hashSeed), Literal(items), Literal(bits), 0, 0).toAggregateExpression(),
      "__rapids_required_key_bloom")()
    val scalar = ScalarSubquery(Aggregate(Nil, Seq(summary), source))
    logWarning(s"GpuRequiredKeyBloomFilter: inserted pre-exchange filter " +
      s"sourceKeys=${sourceKeys.map(_.name).mkString(",")} " +
      s"targetKeys=${targetKeys.map(_.name).mkString(",")} items=$items bits=$bits")
    Filter(BloomFilterMightContain(scalar, XxHash64(targetKeys, hashSeed)), target)
  }

  private def eligibleTarget(plan: LogicalPlan): Boolean =
    safeBranch(plan) && !plan.exists(_.expressions.exists(_.exists {
      case _: BloomFilterMightContain => true
      case _ => false
    }))

  private def eligibleSource(plan: LogicalPlan): Boolean = safeBranch(plan) && plan.exists {
    case Filter(condition, child) =>
      val predicates = splitAnd(condition).filterNot(_.isInstanceOf[IsNotNull])
      val grouped = child.exists(_.isInstanceOf[Aggregate])
      predicates.exists { expression =>
        grouped || (expression match {
          case EqualTo(_: Attribute, _: Literal) | EqualTo(_: Literal, _: Attribute) => true
          case Contains(_: Attribute, _: Literal) | StartsWith(_: Attribute, _: Literal) => true
          case _ => false
        })
      }
    case _ => false
  }

  private def safeBranch(plan: LogicalPlan): Boolean = {
    var leaves = 0
    val safe = !plan.exists { node =>
      val supported = node match {
        case _: LogicalRelation | _: LocalRelation =>
          leaves += 1
          true
        case _: Filter | _: Project | _: Aggregate | _: SubqueryAlias => true
        case _ => false
      }
      !supported || node.expressions.exists { expression =>
        !expression.deterministic || expression.exists {
          case _: SubqueryExpression => true
          case _ => false
        }
      }
    }
    safe && leaves == 1
  }

  private def splitAnd(expression: Expression): Seq[Expression] = expression match {
    case And(left, right) => splitAnd(left) ++ splitAnd(right)
    case other => Seq(other)
  }

  private def registerPostSubqueryPass(): Unit = {
    val experimental = spark.experimental
    experimental.synchronized {
      if (!experimental.extraOptimizations.exists(_.isInstanceOf[GpuRequiredKeyBloomFilter])) {
        experimental.extraOptimizations = experimental.extraOptimizations :+ this
      }
    }
  }
}

/** Experimental extension usable alongside the unchanged historical GPU plugin extension. */
class GpuRequiredKeyBloomExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule(spark => GpuRequiredKeyBloomFilter(spark))
    extensions.injectPreCBORule(spark => GpuRequiredKeyBloomFilter(spark))
  }
}
