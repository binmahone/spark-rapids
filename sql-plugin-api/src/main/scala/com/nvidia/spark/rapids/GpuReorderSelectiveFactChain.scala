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

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet, EqualTo}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Filter, HintInfo, Join, JoinHint, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.logical.{Project, SHUFFLE_HASH}
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Start a large inner-join chain from two selective fact branches when trusted statistics prove
 * that doing so reduces intermediate bytes substantially.
 *
 * This rule is intentionally narrower than a general join enumerator. It requires the selected
 * seed pair to contain literal filters on both sides and to retain at least one billion estimated
 * rows on each side. The guard targets large fact-to-fact joins while leaving star joins and small
 * selective dimensions to Spark CBO and the dimension-chain rules.
 */
case class GpuReorderSelectiveFactChain(spark: SparkSession)
  extends Rule[LogicalPlan]
  with Logging {

  private val enabledKey =
    "spark.rapids.sql.optimizer.reorderSelectiveFactChain.enabled"
  private val minSeedRows = BigInt(1000000000L)
  private val minImprovementRatio = BigDecimal("1.25")
  private val maxClusterItems = 8
  private val maxWrapperDepth = 4
  private val metadata = GpuOptimizerTrustedMetadata.fromSession(spark)
  private val firstRewriteLog = new AtomicBoolean(false)

  registerPostCboPass()

  override def apply(plan: LogicalPlan): LogicalPlan = {
    registerPostCboPass()
    if (!enabled || !plan.resolved || plan.isStreaming || metadata.isEmpty) {
      plan
    } else {
      rewritePlan(plan)
    }
  }

  private def rewritePlan(plan: LogicalPlan): LogicalPlan = {
    val rewritten = rewriteCurrent(plan).getOrElse(plan)
    if (!rewritten.fastEquals(plan)) {
      rewritten
    } else {
      val children = plan.children.map(rewritePlan)
      if (children.zip(plan.children).forall { case (left, right) => left.fastEquals(right) }) {
        plan
      } else {
        plan.withNewChildren(children)
      }
    }
  }

  private def rewriteCurrent(plan: LogicalPlan): Option[LogicalPlan] = plan match {
    case Project(projectList, child) if projectList.forall(_.deterministic) =>
      rewriteCluster(child, AttributeSet(projectList.flatMap(_.references))).map {
        rewritten => Project(projectList, rewritten)
      }
    case join: Join =>
      rewriteCluster(join, join.outputSet)
    case _ => None
  }

  private def rewriteCluster(
      root: LogicalPlan,
      requiredOutput: AttributeSet): Option[LogicalPlan] = {
    extractInnerJoinCluster(root).flatMap { case (items, conditions) =>
      if (items.length < 4 || items.length > maxClusterItems ||
          !conditions.forall(isSafeEquiPredicate)) {
        None
      } else {
        for {
          originalCost <- intermediateCost(root)
          greedy <- buildGreedy(items, conditions, requiredOutput)
          (candidate, candidateCost, seedLeft, seedRight) = greedy
          if candidate.outputSet.intersect(requiredOutput) == requiredOutput
          if BigDecimal(candidateCost) * minImprovementRatio <= BigDecimal(originalCost)
        } yield {
          if (firstRewriteLog.compareAndSet(false, true)) {
            logWarning(
              "GpuReorderSelectiveFactChain: rewrote a large selective fact chain " +
                s"items=${items.length} originalCostBytes=$originalCost " +
                s"candidateCostBytes=$candidateCost " +
                s"seedLeft=${planSummary(seedLeft)} seedRight=${planSummary(seedRight)}")
          }
          Project(root.output.filter(candidate.outputSet.contains), candidate)
        }
      }
    }
  }

  private def buildGreedy(
      items: Seq[LogicalPlan],
      conditions: Seq[Expression],
      requiredOutput: AttributeSet)
  : Option[(LogicalPlan, BigInt, LogicalPlan, LogicalPlan)] = {
    val seedCandidates = for {
      (left, leftIndex) <- items.zipWithIndex
      (right, rightIndex) <- items.zipWithIndex
      if leftIndex < rightIndex
      if isLargeSelectiveSeed(left) && isLargeSelectiveSeed(right)
      edges = edgePredicates(left.outputSet, right.outputSet, conditions)
      if edges.nonEmpty
      built <- buildStep(left, right, edges, conditions.diff(edges), requiredOutput)
      estimate <- metadata.flatMap(_.estimate(built))
    } yield (built, estimate.sizeInBytes, left, right, conditions.diff(edges),
      items.zipWithIndex.filterNot(p => p._2 == leftIndex || p._2 == rightIndex).map(_._1))

    seedCandidates.sortBy(candidate => (candidate._2, candidate._2.toString)).headOption.flatMap {
      case (seed, seedCost, seedLeft, seedRight, seedConditions, seedRemaining) =>
        var acc = seed
        var cost = seedCost
        var remainingConditions = seedConditions
        var remaining = seedRemaining
        var valid = true

        while (remaining.nonEmpty && valid) {
          val candidates = remaining.flatMap { next =>
            val edges = edgePredicates(acc.outputSet, next.outputSet, remainingConditions)
            if (edges.isEmpty) {
              None
            } else {
              val restConditions = remainingConditions.diff(edges)
              buildStep(acc, next, edges, restConditions, requiredOutput).flatMap { built =>
                metadata.flatMap(_.estimate(built)).map { estimate =>
                  (built, estimate.sizeInBytes, next, restConditions)
                }
              }
            }
          }
          candidates.sortBy(candidate => (candidate._2, candidate._3.outputSet.toString)).headOption
            .fold({ valid = false }) {
              case (built, bytes, next, restConditions) =>
                acc = built
                cost += bytes
                remainingConditions = restConditions
                remaining = remaining.filterNot(_ eq next)
            }
        }

        if (valid && remainingConditions.isEmpty) {
          Some((acc, cost, seedLeft, seedRight))
        } else {
          None
        }
    }
  }

  private def buildStep(
      left: LogicalPlan,
      right: LogicalPlan,
      edges: Seq[Expression],
      remainingConditions: Seq[Expression],
      requiredOutput: AttributeSet): Option[LogicalPlan] = {
    val joined = Join(
      left,
      right,
      Inner,
      Some(edges.reduceLeft(And)),
      smallerSideShuffleHashHint(left, right))
    val future = requiredOutput ++ AttributeSet(remainingConditions.flatMap(_.references))
    val keep = joined.output.filter(future.contains)
    if (keep.isEmpty) None else Some(Project(keep, joined))
  }

  private def smallerSideShuffleHashHint(
      left: LogicalPlan,
      right: LogicalPlan): JoinHint = {
    val shuffleHash = HintInfo(strategy = Some(SHUFFLE_HASH))
    (metadata.flatMap(_.estimate(left)), metadata.flatMap(_.estimate(right))) match {
      case (Some(leftEstimate), Some(rightEstimate))
          if leftEstimate.sizeInBytes <= rightEstimate.sizeInBytes =>
        JoinHint(Some(shuffleHash), None)
      case (Some(_), Some(_)) =>
        JoinHint(None, Some(shuffleHash))
      case _ => JoinHint.NONE
    }
  }

  private def intermediateCost(plan: LogicalPlan): Option[BigInt] = {
    val estimates = plan.collect {
      case join: Join => metadata.flatMap(_.estimate(join)).map(_.sizeInBytes)
    }
    if (estimates.nonEmpty && estimates.forall(_.isDefined)) {
      Some(estimates.flatten.sum)
    } else {
      None
    }
  }

  private def isLargeSelectiveSeed(plan: LogicalPlan): Boolean = {
    hasLiteralFilter(plan) && metadata.flatMap(_.estimate(plan)).exists(_.rows >= minSeedRows)
  }

  private def hasLiteralFilter(plan: LogicalPlan): Boolean = plan.exists {
    case Filter(condition, _) => splitAnd(condition).exists {
      case expression if expression.children.length == 2 =>
        val children = expression.children
        (children.head.isInstanceOf[Attribute] && children.last.isInstanceOf[Literal]) ||
          (children.head.isInstanceOf[Literal] && children.last.isInstanceOf[Attribute])
      case _ => false
    }
    case _ => false
  }

  private def extractInnerJoinCluster(
      plan: LogicalPlan): Option[(Seq[LogicalPlan], Seq[Expression])] = {
    val items = ArrayBuffer.empty[LogicalPlan]
    val conditions = ArrayBuffer.empty[Expression]

    def collect(current: LogicalPlan): Boolean = stripJoinWrapper(current, 0) match {
      case Join(left, right, Inner, Some(condition), JoinHint.NONE) =>
        conditions ++= splitAnd(condition)
        collect(left) && collect(right)
      case _: Join => false
      case other =>
        items += other
        true
    }

    if (collect(plan) && items.length >= 2 && conditions.nonEmpty) {
      Some(items.toSeq -> conditions.toSeq)
    } else {
      None
    }
  }

  private def stripJoinWrapper(plan: LogicalPlan, depth: Int): LogicalPlan = plan match {
    case Project(projectList, child)
        if depth < maxWrapperDepth && projectList.forall(_.isInstanceOf[Attribute]) &&
          containsInnerJoin(child, depth + 1) =>
      stripJoinWrapper(child, depth + 1)
    case SubqueryAlias(_, child)
        if depth < maxWrapperDepth && containsInnerJoin(child, depth + 1) =>
      stripJoinWrapper(child, depth + 1)
    case other => other
  }

  private def containsInnerJoin(plan: LogicalPlan, depth: Int): Boolean = plan match {
    case Join(_, _, Inner, _, _) => true
    case Project(projectList, child)
        if depth < maxWrapperDepth && projectList.forall(_.isInstanceOf[Attribute]) =>
      containsInnerJoin(child, depth + 1)
    case SubqueryAlias(_, child) if depth < maxWrapperDepth =>
      containsInnerJoin(child, depth + 1)
    case _ => false
  }

  private def edgePredicates(
      left: AttributeSet,
      right: AttributeSet,
      conditions: Seq[Expression]): Seq[Expression] = conditions.filter { condition =>
    condition.references.subsetOf(left ++ right) &&
      condition.references.exists(left.contains) && condition.references.exists(right.contains)
  }

  private def isSafeEquiPredicate(expression: Expression): Boolean = expression match {
    case EqualTo(left: Attribute, right: Attribute) => left.deterministic && right.deterministic
    case _ => false
  }

  private def splitAnd(expression: Expression): Seq[Expression] = expression match {
    case And(left, right) => splitAnd(left) ++ splitAnd(right)
    case other => Seq(other)
  }

  private def planSummary(plan: LogicalPlan): String = {
    val estimate = metadata.flatMap(_.estimate(plan))
    val rows = estimate.map(_.rows).getOrElse("?")
    val bytes = estimate.map(_.sizeInBytes).getOrElse("?")
    s"${plan.nodeName}[${plan.output.map(_.name).take(4).mkString(",")}] " +
      s"rows=$rows bytes=$bytes"
  }

  private def registerPostCboPass(): Unit = {
    if (!enabled) {
      return
    }
    val experimental = spark.experimental
    experimental.synchronized {
      if (!experimental.extraOptimizations.exists(
          _.isInstanceOf[GpuReorderSelectiveFactChain])) {
        experimental.extraOptimizations = experimental.extraOptimizations :+ this
      }
    }
  }

  private def enabled: Boolean =
    spark.sessionState.conf.getConfString(enabledKey, "false").toBoolean
}
