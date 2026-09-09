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

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!enabled || !plan.resolved) {
      plan
    } else {
      plan.transformDown {
        case aggregate: Aggregate => rewriteAggregate(aggregate)
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

    pushGlobalSumsThroughJoin(aggregate)
      .orElse(pushSumsThroughLookupJoins(aggregate))
      .orElse(rewriteAliasedMeasure(aggregate))
      .getOrElse(rewriteDirectMeasure(aggregate))
  }

  private case class SumTarget(expression: AggregateExpression, function: Sum)

  private case class LookupJoin(
      join: Join,
      lookup: LogicalPlan,
      lookupOnRight: Boolean,
      projectsAbove: Seq[Project])

  /**
   * Partially aggregate global sums by the equi-join keys on their source side. The final global
   * sum remains above the join, so filtering and multiplicity on the other side retain their
   * original semantics.
   */
  private def pushGlobalSumsThroughJoin(aggregate: Aggregate): Option[LogicalPlan] = {
    if (aggregate.groupingExpressions.nonEmpty) {
      return None
    }
    val (project, join) = aggregate.child match {
      case p @ Project(_, child: Join) => (Some(p), child)
      case child: Join => (None, child)
      case _ => return None
    }
    if (join.joinType != Inner || join.hint != JoinHint.NONE ||
        join.condition.isEmpty) {
      return None
    }

    val targets = supportedSumTargets(aggregate).getOrElse(return None)
    val mappedInputs = targets.map { target =>
      mapThroughProject(target.function.child, project).getOrElse(return None)
    }
    val allReferences = AttributeSet(mappedInputs.flatMap(_.references))
    val sourceOnLeft = allReferences.subsetOf(join.left.outputSet)
    val sourceOnRight = allReferences.subsetOf(join.right.outputSet)
    if (sourceOnLeft == sourceOnRight) {
      return None
    }
    val source = if (sourceOnLeft) join.left else join.right
    if (source.exists(_.isInstanceOf[Aggregate])) {
      return None
    }

    val groupingKeys = joinKeysForSide(
      join.condition.get,
      source.outputSet,
      if (sourceOnLeft) join.right.outputSet else join.left.outputSet)
      .getOrElse(return None)
    val inputAliases = mappedInputs.zipWithIndex.map {
      case (input, index) => Alias(input, s"_rapids_global_sum_input_$index")()
    }
    val preProject = Project(groupingKeys ++ inputAliases, source)
    val preSums = targets.zip(inputAliases).zipWithIndex.map {
      case ((target, inputAlias), index) =>
        Alias(
          target.expression.copy(aggregateFunction = target.function.copy(
            child = inputAlias.toAttribute), resultId = NamedExpression.newExprId),
          s"_rapids_pre_sum_$index")()
    }
    val preAggregate = Aggregate(
      groupingKeys,
      groupingKeys ++ preSums,
      preProject)
    val rewrittenJoin = if (sourceOnLeft) {
      join.copy(left = preAggregate)
    } else {
      join.copy(right = preAggregate)
    }
    val replacements = targets.map(_.expression).zip(preSums.map(_.toAttribute))
    val rewrittenExpressions = replaceAggregateSums(aggregate.aggregateExpressions, replacements)
    logWarning(
      "GpuPushAggregateMeasureBeforeJoin: pushed global SUMs below an inner join " +
        s"groupingKeys=${groupingKeys.map(_.name).mkString(",")}")
    Some(aggregate.copy(aggregateExpressions = rewrittenExpressions, child = rewrittenJoin))
  }

  /**
   * Move grouped sums below trusted PK/FK lookup joins. A final aggregation is retained after the
   * lookup chain, which is conservative when several lookup keys map to the same payload value.
   */
  private def pushSumsThroughLookupJoins(aggregate: Aggregate): Option[LogicalPlan] = {
    if (aggregate.groupingExpressions.isEmpty) {
      return None
    }
    val metadata = GpuOptimizerTrustedMetadata.fromSession(spark).getOrElse(return None)
    val (project, source) = aggregate.child match {
      case p @ Project(_, child) => (Some(p), child)
      case child => (None, child)
    }
    val (base, lookups) = peelLookupJoins(source, metadata)
    if (lookups.isEmpty || base.exists(_.isInstanceOf[Aggregate])) {
      return None
    }

    val targets = supportedSumTargets(aggregate).getOrElse(return None)
    val mappedInputs = targets.map { target =>
      mapThroughProject(target.function.child, project).getOrElse(return None)
    }
    if (!mappedInputs.forall(_.references.subsetOf(base.outputSet))) {
      return None
    }

    val mappedGrouping = aggregate.groupingExpressions.map { expression =>
      mapThroughProject(expression, project).getOrElse(return None)
    }
    val baseGrouping = mappedGrouping.filter(_.references.subsetOf(base.outputSet))
    if (!baseGrouping.forall(_.isInstanceOf[Attribute])) {
      return None
    }
    val lookupBaseKeys = lookups.flatMap { lookup =>
      val currentBase = if (lookup.lookupOnRight) lookup.join.left else lookup.join.right
      metadata.cardinalityPreservingLookupKeys(
        currentBase,
        lookup.lookup,
        lookup.join.condition.get).toSeq.flatten.map(_._1).filter(base.outputSet.contains)
    }
    val preGrouping = (baseGrouping.map(_.asInstanceOf[Attribute]) ++ lookupBaseKeys)
      .foldLeft(Vector.empty[Attribute]) { (result, attribute) =>
        if (result.exists(_.semanticEquals(attribute))) result else result :+ attribute
      }
    if (preGrouping.isEmpty) {
      return None
    }

    val inputAliases = mappedInputs.zipWithIndex.map {
      case (input, index) => Alias(input, s"_rapids_lookup_sum_input_$index")()
    }
    val preProject = Project(preGrouping ++ inputAliases, base)
    val preSums = targets.zip(inputAliases).zipWithIndex.map {
      case ((target, inputAlias), index) =>
        Alias(
          target.expression.copy(aggregateFunction = target.function.copy(
            child = inputAlias.toAttribute), resultId = NamedExpression.newExprId),
          s"_rapids_lookup_pre_sum_$index")()
    }
    val preAggregate = Aggregate(preGrouping, preGrouping ++ preSums, preProject)
    val restored = lookups.foldLeft[LogicalPlan](preAggregate) {
      case (current, lookup) =>
        val joined = if (lookup.lookupOnRight) lookup.join.copy(left = current)
        else lookup.join.copy(right = current)
        lookup.projectsAbove.reverse.foldLeft[LogicalPlan](joined) {
          case (projectChild, original) =>
            val retained = original.projectList.filter(
              _.references.subsetOf(projectChild.outputSet))
            val carriedSums = preSums.map(_.toAttribute).filter(projectChild.outputSet.contains)
              .filterNot(attribute => retained.exists(_.toAttribute.semanticEquals(attribute)))
            Project(retained ++ carriedSums, projectChild)
        }
    }
    val postLookup = project match {
      case Some(original) =>
        val retained = original.projectList.filter(
          _.references.subsetOf(restored.outputSet))
        Project(retained ++ preSums.map(_.toAttribute), restored)
      case None => restored
    }
    if (!aggregate.groupingExpressions.forall(_.references.subsetOf(postLookup.outputSet))) {
      return None
    }

    val replacements = targets.map(_.expression).zip(preSums.map(_.toAttribute))
    val eliminated = eliminateRedundantLookupAggregate(
      aggregate,
      postLookup,
      preGrouping,
      replacements)
    eliminated match {
      case Some(projected) =>
        logWarning(
          "GpuPushAggregateMeasureBeforeJoin: eliminated a redundant SUM after trusted " +
            s"lookup joins lookupCount=${lookups.size} " +
            s"uniqueKeys=${preGrouping.map(_.name).mkString(",")}")
        Some(projected)
      case None =>
        val rewrittenExpressions = replaceAggregateSums(
          aggregate.aggregateExpressions,
          replacements)
        logWarning(
          "GpuPushAggregateMeasureBeforeJoin: pushed SUMs below trusted lookup joins " +
            s"lookupCount=${lookups.size} preGrouping=${preGrouping.map(_.name).mkString(",")}")
        Some(aggregate.copy(aggregateExpressions = rewrittenExpressions, child = postLookup))
    }
  }

  /**
   * The pre-aggregate emits at most one row for each pre-grouping key. Trusted lookup joins retain
   * exactly one row for every input row. If the final grouping contains an equivalent attribute
   * for every pre-grouping key, each final group therefore contains at most one row and SUM is an
   * identity operation. Keep the original output expression IDs by projecting the rewritten
   * aggregate expressions.
   */
  private def eliminateRedundantLookupAggregate(
      aggregate: Aggregate,
      child: LogicalPlan,
      preGrouping: Seq[Attribute],
      replacements: Seq[(AggregateExpression, Attribute)]): Option[LogicalPlan] = {
    val finalGrouping = aggregate.groupingExpressions.collect { case attribute: Attribute =>
      attribute
    }
    if (finalGrouping.size != aggregate.groupingExpressions.size) {
      return None
    }

    val equivalence = equalityClasses(child)
    val preservesUniqueKey = preGrouping.forall { uniqueKey =>
      finalGrouping.exists(equivalence.connected(uniqueKey, _))
    }
    if (!preservesUniqueKey) {
      return None
    }

    val projected = aggregate.aggregateExpressions.map { named =>
      named.transformDown {
        case current: AggregateExpression =>
          replacements.collectFirst {
            case (target, replacement) if current.semanticEquals(target) => replacement
          }.getOrElse(current)
      }.asInstanceOf[NamedExpression]
    }
    if (projected.exists(_.exists(_.isInstanceOf[AggregateExpression])) ||
        projected.exists(expression => !expression.references.subsetOf(child.outputSet))) {
      None
    } else {
      Some(Project(projected, child))
    }
  }

  private def equalityClasses(plan: LogicalPlan): EqualityClasses = {
    val classes = new EqualityClasses
    plan.foreach {
      case Join(_, _, Inner, Some(condition), _) =>
        splitConjunctivePredicates(condition).foreach {
          case EqualTo(left: Attribute, right: Attribute) => classes.union(left, right)
          case _ =>
        }
      case Filter(condition, _) =>
        splitConjunctivePredicates(condition).foreach {
          case EqualTo(left: Attribute, right: Attribute) => classes.union(left, right)
          case _ =>
        }
      case _ =>
    }
    classes
  }

  private final class EqualityClasses {
    private val parent = scala.collection.mutable.HashMap.empty[Long, Long]

    private def find(value: Long): Long = {
      val current = parent.getOrElseUpdate(value, value)
      if (current == value) {
        value
      } else {
        val root = find(current)
        parent.update(value, root)
        root
      }
    }

    def union(left: Attribute, right: Attribute): Unit = {
      val leftRoot = find(left.exprId.id)
      val rightRoot = find(right.exprId.id)
      if (leftRoot != rightRoot) {
        parent.update(leftRoot, rightRoot)
      }
    }

    def connected(left: Attribute, right: Attribute): Boolean =
      left.semanticEquals(right) || find(left.exprId.id) == find(right.exprId.id)
  }

  private def peelLookupJoins(
      source: LogicalPlan,
      metadata: GpuOptimizerTrustedMetadata): (LogicalPlan, Seq[LookupJoin]) = {
    def loop(
        current: LogicalPlan,
        outer: List[LookupJoin],
        projectsAbove: Vector[Project]): (LogicalPlan, Seq[LookupJoin]) = {
      current match {
        case project @ Project(projectList, child)
            if projectList.forall(_.isInstanceOf[Attribute]) =>
          loop(child, outer, projectsAbove :+ project)
        case join @ Join(left, right, Inner, Some(condition), _) =>
          metadata.cardinalityPreservingLookupKeys(left, right, condition) match {
            case Some(_) => loop(
              left,
              LookupJoin(join, right, lookupOnRight = true, projectsAbove) :: outer,
              Vector.empty)
            case None =>
              metadata.cardinalityPreservingLookupKeys(right, left, condition) match {
                case Some(_) => loop(
                  right,
                  LookupJoin(join, left, lookupOnRight = false, projectsAbove) :: outer,
                  Vector.empty)
                case None => (rewrapProjects(current, projectsAbove), outer)
              }
          }
        case _ => (rewrapProjects(current, projectsAbove), outer)
      }
    }

    def rewrapProjects(plan: LogicalPlan, projects: Seq[Project]): LogicalPlan =
      projects.reverse.foldLeft(plan) { case (child, project) => project.copy(child = child) }

    loop(source, Nil, Vector.empty)
  }

  private def supportedSumTargets(aggregate: Aggregate): Option[Seq[SumTarget]] = {
    val allAggregates = aggregate.aggregateExpressions.flatMap(_.collect {
      case expression: AggregateExpression => expression
    })
    val targets = allAggregates.collect {
      case expression @ AggregateExpression(sum: Sum, _, false, None, _)
          if sum.child.dataType == sum.dataType => SumTarget(expression, sum)
    }
    if (targets.nonEmpty && targets.size == allAggregates.size) Some(targets) else None
  }

  private def mapThroughProject(
      expression: Expression,
      project: Option[Project]): Option[Expression] = project match {
    case None => Some(expression)
    case Some(value) =>
      val assignments = value.projectList.map(named => named.toAttribute.exprId -> named).toMap
      val mapped = expression.transformDown {
        case attribute: Attribute if assignments.contains(attribute.exprId) =>
          assignments(attribute.exprId) match {
            case alias: Alias => alias.child
            case source: Attribute => source
            case other => other
          }
      }
      if (mapped.references.subsetOf(value.child.outputSet)) Some(mapped) else None
  }

  private def joinKeysForSide(
      condition: Expression,
      sourceOutput: AttributeSet,
      otherOutput: AttributeSet): Option[Seq[Attribute]] = {
    val keys = splitConjunctivePredicates(condition).map {
      case EqualTo(left: Attribute, right: Attribute)
          if sourceOutput.contains(left) && otherOutput.contains(right) => Some(left)
      case EqualTo(left: Attribute, right: Attribute)
          if sourceOutput.contains(right) && otherOutput.contains(left) => Some(right)
      case _ => None
    }
    if (keys.nonEmpty && keys.forall(_.isDefined)) Some(keys.flatten.distinct) else None
  }

  private def replaceAggregateSums(
      expressions: Seq[NamedExpression],
      replacements: Seq[(AggregateExpression, Attribute)]): Seq[NamedExpression] = {
    expressions.map { named =>
      named.transformDown {
        case current: AggregateExpression =>
          replacements.collectFirst {
            case (target, replacement) if current.semanticEquals(target) =>
              val sum = current.aggregateFunction.asInstanceOf[Sum]
              current.copy(aggregateFunction = sum.copy(child = replacement))
          }.getOrElse(current)
      }.asInstanceOf[NamedExpression]
    }
  }

  private def rewriteDirectMeasure(aggregate: Aggregate): LogicalPlan = {
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

  private def rewriteAliasedMeasure(aggregate: Aggregate): Option[LogicalPlan] = {
    val (project, joinChild) = aggregate.child match {
      case p @ Project(_, child) if containsJoin(child) => (p, child)
      case _ => return None
    }
    val sumChildren = aggregate.aggregateExpressions.flatMap(_.collect {
      case sum: Sum => sum.child
    })
    val candidates = sumChildren.flatMap {
      case attribute: Attribute =>
        project.projectList.collectFirst {
          case alias: Alias
              if alias.toAttribute.semanticEquals(attribute) && isCompositeMeasure(alias.child) =>
            (attribute, alias)
        }
      case _ => None
    }
    if (candidates.size != 1 || sumChildren.size != 1) {
      return None
    }

    val (sumAttribute, alias) = candidates.head
    val candidateRefs = alias.child.references
    if (
      aggregate.groupingExpressions.exists(
        expression => expression.references.contains(sumAttribute) ||
          expression.references.intersect(candidateRefs).nonEmpty) ||
      aggregate.aggregateExpressions.exists(
        usesAliasedCandidateOutsideTargetSum(_, sumAttribute, candidateRefs))
    ) {
      return None
    }
    val otherProjectExpressions = project.projectList.filterNot {
      case current: Alias => current.exprId == alias.exprId
      case _ => false
    }
    if (otherProjectExpressions.exists(_.references.intersect(candidateRefs).nonEmpty)) {
      return None
    }

    val joinRefs = AttributeSet(joinChild.collect {
      case Join(_, _, _, condition, _) => condition.toSeq.flatMap(_.references)
    }.flatten)
    if (joinRefs.intersect(candidateRefs).nonEmpty) {
      return None
    }
    val inputWidth = candidateRefs.toSeq.map(_.dataType.defaultSize).sum
    if (alias.dataType.defaultSize >= inputWidth) {
      return None
    }

    pushIntoSourceBranch(joinChild, candidateRefs, alias).map { rewrittenJoinChild =>
      val rewrittenProjectList = project.projectList.map {
        case current: Alias if current.exprId == alias.exprId => alias.toAttribute
        case other => other
      }
      logWarning(
        "GpuPushAggregateMeasureBeforeJoin: moved an aliased width-reducing SUM input " +
          s"to its first available join inputWidth=$inputWidth " +
          s"outputWidth=${alias.dataType.defaultSize} " +
          s"inputAttributes=${candidateRefs.toSeq.map(_.name).sorted.mkString(",")}")
      aggregate.copy(child = project.copy(
        projectList = rewrittenProjectList,
        child = rewrittenJoinChild))
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

  private def usesAliasedCandidateOutsideTargetSum(
      expression: Expression,
      sumAttribute: Attribute,
      candidateRefs: AttributeSet): Boolean = {
    expression.collect {
      case aggregateExpression: AggregateExpression => aggregateExpression
    }.exists { aggregateExpression =>
      aggregateExpression.aggregateFunction match {
        case sum: Sum if sum.child.semanticEquals(sumAttribute) =>
          aggregateExpression.filter.exists(
            filter => filter.references.contains(sumAttribute) ||
              filter.references.intersect(candidateRefs).nonEmpty)
        case _ =>
          aggregateExpression.references.contains(sumAttribute) ||
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
        // This is the deepest join at which a measure spanning both inputs becomes available.
        // Materializing immediately above it still moves the narrower value below every ancestor
        // join and exchange without changing the join that supplies the measure's inputs.
        if (!inLeft && refs.subsetOf(join.outputSet)) {
          projectMeasure(join, refs, alias)
        } else {
          None
        }
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
