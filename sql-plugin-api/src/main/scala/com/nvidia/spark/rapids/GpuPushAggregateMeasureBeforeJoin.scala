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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, BinaryComparison}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Sum}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, JoinHint}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Reduce a SUM measure before an inner-join cluster.
 *
 * This targets a general network-width problem: an aggregate such as `SUM(price * (1 - discount))`
 * otherwise carries both source columns through every intervening exchange, even though only the
 * computed value is needed above the joins. When the measure branch is selectively filtered and
 * its equi-join key is also a final grouping key, the rule can additionally pre-aggregate the SUM
 * by that key. Both rewrites require one composite SUM input, all of its attributes on one join
 * branch, a smaller result width, and no overlap with grouping keys, join predicates, or another
 * aggregate expression.
 */
case class GpuPushAggregateMeasureBeforeJoin(spark: SparkSession)
  extends Rule[LogicalPlan]
  with PredicateHelper
  with Logging {

  private val enabledKey =
    "spark.rapids.sql.optimizer.pushAggregateMeasureBeforeJoin.enabled"

  registerPostCboPass()

  override def apply(plan: LogicalPlan): LogicalPlan = {
    registerPostCboPass()
    if (!enabled || !plan.resolved) {
      plan
    } else {
      plan.transformDown {
        case aggregate: Aggregate => rewriteAggregate(aggregate)
      }
    }
  }

  private def registerPostCboPass(): Unit = {
    if (!enabled) {
      return
    }
    val experimental = spark.experimental
    experimental.synchronized {
      if (!experimental.extraOptimizations.exists(
          _.isInstanceOf[GpuPushAggregateMeasureBeforeJoin])) {
        experimental.extraOptimizations = experimental.extraOptimizations :+ this
      }
    }
  }

  private def enabled: Boolean = {
    spark.sessionState.conf.getConfString(enabledKey, "false").toBoolean
  }

  private def rewriteAggregate(aggregate: Aggregate): LogicalPlan = {
    if (!containsJoin(aggregate.child)) {
      return aggregate
    }

    val sumInputs = aggregate.aggregateExpressions.flatMap(_.collect {
      case sum: Sum if isCompositeMeasure(sum.child) => sum.child
    })
    val candidates = sumInputs.foldLeft(Vector.empty[Expression]) { (result, expression) =>
      if (result.exists(_.semanticEquals(expression))) result else result :+ expression
    }
    if (candidates.size != 1 || sumInputs.size != 1) {
      return aggregate
    }

    val candidate = candidates.head
    val candidateRefs = candidate.references
    if (
      aggregate.groupingExpressions.exists(_.references.intersect(candidateRefs).nonEmpty) ||
      aggregate.aggregateExpressions.exists(
        usesCandidateOutsideTargetSum(_, candidate, candidateRefs))
    ) {
      return aggregate
    }

    val joinRefs = AttributeSet(aggregate.child.collect {
      case Join(_, _, _, condition, _) => condition.toSeq.flatMap(_.references)
    }.flatten)
    if (joinRefs.intersect(candidateRefs).nonEmpty) {
      return aggregate
    }

    val inputWidth = candidateRefs.toSeq.map(_.dataType.defaultSize).sum
    if (candidate.dataType.defaultSize >= inputWidth) {
      return aggregate
    }

    val aliasName = s"_rapids_measure_${math.abs(candidate.semanticHash())}"
    preAggregateBeforeJoin(aggregate, candidate, candidateRefs, aliasName) match {
      case Some((rewrittenChild, preAggregateAttribute, groupingKeys)) =>
        logWarning(
          "GpuPushAggregateMeasureBeforeJoin: pushed a distributive SUM before an inner join " +
            s"groupingKeys=${groupingKeys.map(_.name).sorted.mkString(",")} " +
            s"inputAttributes=${candidateRefs.toSeq.map(_.name).sorted.mkString(",")}")
        aggregate.copy(
          aggregateExpressions = replaceTargetSum(
            aggregate.aggregateExpressions,
            candidate,
            preAggregateAttribute),
          child = rewrittenChild)
      case None => materializeMeasure(aggregate, candidate, candidateRefs, aliasName, inputWidth)
    }
  }

  private def materializeMeasure(
      aggregate: Aggregate,
      candidate: Expression,
      candidateRefs: AttributeSet,
      aliasName: String,
      inputWidth: Int): LogicalPlan = {
    val alias = Alias(candidate, aliasName)()
    pushIntoSourceBranch(aggregate.child, candidateRefs, alias) match {
      case Some(rewrittenChild) =>
        logWarning(
          "GpuPushAggregateMeasureBeforeJoin: materialized a width-reducing SUM input " +
            s"before joins inputWidth=$inputWidth outputWidth=${candidate.dataType.defaultSize} " +
            s"inputAttributes=${candidateRefs.toSeq.map(_.name).sorted.mkString(",")}")
        aggregate.copy(
          aggregateExpressions = replaceTargetSum(
            aggregate.aggregateExpressions,
            candidate,
            alias.toAttribute),
          child = rewrittenChild)
      case None => aggregate
    }
  }

  private def replaceTargetSum(
      expressions: Seq[NamedExpression],
      candidate: Expression,
      replacement: Attribute): Seq[NamedExpression] = {
    expressions.map { named =>
      named.transformDown {
        case sum: Sum if sum.child.semanticEquals(candidate) =>
          sum.withNewChildren(Seq(replacement))
      }.asInstanceOf[NamedExpression]
    }
  }

  private def preAggregateBeforeJoin(
      aggregate: Aggregate,
      candidate: Expression,
      candidateRefs: AttributeSet,
      aliasName: String): Option[(LogicalPlan, Attribute, Seq[Attribute])] = {
    val (projectList, join) = aggregate.child match {
      case Project(expressions, child: Join) => (Some(expressions), child)
      case child: Join => (None, child)
      case _ => return None
    }
    if (join.joinType != Inner || join.hint != JoinHint.NONE) {
      return None
    }

    val (measureSide, otherSide, replaceLeft) = {
      val inLeft = candidateRefs.subsetOf(join.left.outputSet)
      val inRight = candidateRefs.subsetOf(join.right.outputSet)
      if (inLeft == inRight) {
        return None
      } else if (inLeft) {
        (join.left, join.right, true)
      } else {
        (join.right, join.left, false)
      }
    }
    if (containsJoin(measureSide) || !hasSelectiveLiteralFilter(measureSide)) {
      return None
    }

    val condition = join.condition.getOrElse(return None)
    val groupingRefs = AttributeSet(aggregate.groupingExpressions.flatMap(_.references))
    val groupingKeys = splitConjunctivePredicates(condition).flatMap {
      case EqualTo(left: Attribute, right: Attribute)
          if measureSide.outputSet.contains(left) && otherSide.outputSet.contains(right) =>
        Some(left)
      case EqualTo(left: Attribute, right: Attribute)
          if measureSide.outputSet.contains(right) && otherSide.outputSet.contains(left) =>
        Some(right)
      case _ => None
    }.foldLeft(Vector.empty[Attribute]) { (result, attribute) =>
      if (result.exists(_.semanticEquals(attribute))) result else result :+ attribute
    }
    if (groupingKeys.isEmpty || !groupingKeys.forall(groupingRefs.contains)) {
      return None
    }

    val measureConditionRefs = condition.references.intersect(measureSide.outputSet)
    if (!measureConditionRefs.subsetOf(AttributeSet(groupingKeys))) {
      return None
    }

    val preAggregateAlias = Alias(Sum(candidate).toAggregateExpression(), aliasName)()
    val preAggregate = Aggregate(
      groupingKeys,
      groupingKeys :+ preAggregateAlias,
      measureSide)
    val rewrittenJoin =
      if (replaceLeft) join.copy(left = preAggregate) else join.copy(right = preAggregate)

    val rewrittenChild = projectList match {
      case Some(expressions) =>
        val retained = expressions.filterNot {
          case attribute: Attribute => candidateRefs.contains(attribute)
          case _ => false
        }
        if (
          retained.exists(_.references.intersect(candidateRefs).nonEmpty) ||
          retained.exists(expression => !expression.references.subsetOf(rewrittenJoin.outputSet))
        ) {
          return None
        }
        Project(retained :+ preAggregateAlias.toAttribute, rewrittenJoin)
      case None => rewrittenJoin
    }
    Some((rewrittenChild, preAggregateAlias.toAttribute, groupingKeys))
  }

  private def hasSelectiveLiteralFilter(plan: LogicalPlan): Boolean = {
    plan.exists {
      case Filter(condition, _) =>
        splitConjunctivePredicates(condition).exists {
          case comparison: BinaryComparison =>
            comparison.left.foldable != comparison.right.foldable
          case _ => false
        }
      case _ => false
    }
  }

  private def isCompositeMeasure(expression: Expression): Boolean = {
    expression.deterministic &&
      !expression.isInstanceOf[Attribute] &&
      expression.references.size >= 2
  }

  private def usesCandidateOutsideTargetSum(
      expression: Expression,
      candidate: Expression,
      candidateRefs: AttributeSet): Boolean = {
    expression.collect {
      case aggregateExpression: AggregateExpression => aggregateExpression
    }.exists { aggregateExpression =>
      aggregateExpression.aggregateFunction match {
        case sum: Sum if sum.child.semanticEquals(candidate) =>
          aggregateExpression.filter.exists(
            _.references.intersect(candidateRefs).nonEmpty)
        case _ =>
          aggregateExpression.references.intersect(candidateRefs).nonEmpty
      }
    }
  }

  private def containsJoin(plan: LogicalPlan): Boolean = {
    plan.exists(_.isInstanceOf[Join])
  }

  private def pushIntoSourceBranch(
      plan: LogicalPlan,
      refs: AttributeSet,
      alias: Alias): Option[LogicalPlan] = plan match {
    case join @ Join(left, right, _, _, _) =>
      val inLeft = refs.subsetOf(left.outputSet)
      val inRight = refs.subsetOf(right.outputSet)
      if (inLeft == inRight) {
        None
      } else if (inLeft) {
        pushIntoSourceBranch(left, refs, alias).map(newLeft => join.copy(left = newLeft))
      } else {
        pushIntoSourceBranch(right, refs, alias).map(newRight => join.copy(right = newRight))
      }

    case project @ Project(projectList, child) if refs.subsetOf(project.outputSet) =>
      val directAttributes = AttributeSet(projectList.collect { case attr: Attribute => attr })
      if (!refs.subsetOf(directAttributes)) {
        projectMeasure(plan, refs, alias)
      } else {
        pushIntoSourceBranch(child, refs, alias).map { rewrittenChild =>
          val retained = projectList.filterNot {
            case attr: Attribute => refs.contains(attr)
            case _ => false
          }
          project.copy(projectList = retained :+ alias.toAttribute, child = rewrittenChild)
        }
      }

    case filter @ Filter(condition, child)
        if refs.subsetOf(child.outputSet) && condition.references.intersect(refs).isEmpty =>
      pushIntoSourceBranch(child, refs, alias)
        .map(rewrittenChild => filter.copy(child = rewrittenChild))

    case other => projectMeasure(other, refs, alias)
  }

  private def projectMeasure(
      plan: LogicalPlan,
      refs: AttributeSet,
      alias: Alias): Option[LogicalPlan] = {
    if (!refs.subsetOf(plan.outputSet)) {
      None
    } else {
      val retained = plan.output.filterNot(refs.contains)
      Some(Project(retained :+ alias, plan))
    }
  }
}
