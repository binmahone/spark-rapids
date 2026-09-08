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

import java.io.FileInputStream
import java.time.LocalDate
import java.util.Properties

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, Contains, EqualNullSafe}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, GreaterThan, GreaterThanOrEqual}
import org.apache.spark.sql.catalyst.expressions.{In, InSet, IsNotNull, LessThan, LessThanOrEqual}
import org.apache.spark.sql.catalyst.expressions.{Literal, Or}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias, View}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.DateType

/**
 * Read frozen optimizer metadata for path-backed tables that have no persistent catalog.
 *
 * A relation is matched only when every scan root in the candidate plan is exactly one direct
 * child of the configured dataset path. View and query aliases are deliberately ignored. This
 * keeps metadata binding independent of SQL names and prevents statistics from being attached to
 * a different dataset accidentally.
 */
private[rapids] final class GpuOptimizerTrustedMetadata private(
    datasetPath: String,
    tableRows: Map[String, BigInt],
    distinctCounts: Map[(String, String), BigInt],
    valueRanges: Map[(String, String), GpuOptimizerTrustedMetadata.ValueRange],
    primaryKeys: Map[String, Seq[String]],
    foreignKeys: Map[(String, Seq[String]), (String, Seq[String])],
    notNullColumns: Map[String, Set[String]]) extends Logging {

  private val normalizedDatasetPath = normalizePath(datasetPath)

  private[rapids] case class Estimate(rows: BigInt, sizeInBytes: BigInt)

  private case class DetailedEstimate(
      rows: BigInt,
      lineage: Map[Long, (String, String)],
      distinct: Map[Long, BigInt])

  /** Estimate a path-bound branch from frozen cardinalities, NDVs, and declared FK relations. */
  def estimate(plan: LogicalPlan): Option[Estimate] = {
    estimateDetailed(plan).map { detailed =>
      val outputWidth = plan.output.map(_.dataType.defaultSize).sum.max(1)
      Estimate(detailed.rows, detailed.rows * outputWidth)
    }.orElse(tableFor(plan).flatMap {
      table =>
        tableRows.get(table).map {
          rows =>
            val selectivity = plan.collect {
              case Filter(condition, _) => predicateSelectivity(table, condition)
            }.foldLeft(BigDecimal(1))(_.min(_))
            val estimate = (BigDecimal(rows) * selectivity)
              .setScale(0, BigDecimal.RoundingMode.CEILING)
            val estimatedRows = estimate.toBigInt.max(BigInt(1))
            val outputWidth = plan.output.map(_.dataType.defaultSize).sum.max(1)
            Estimate(estimatedRows, estimatedRows * outputWidth)
        }
    })
  }

  def estimateRows(plan: LogicalPlan): Option[BigInt] = estimate(plan).map(_.rows)

  def estimateDistinct(plan: LogicalPlan, attribute: Attribute): Option[BigInt] = {
    estimateDetailed(plan).flatMap(_.distinct.get(attribute.exprId.id))
  }

  def isTrustedUniqueKey(plan: LogicalPlan, attributes: Seq[Attribute]): Boolean = {
    estimateDetailed(plan).exists { estimate =>
      val columns = attributes.map(attribute => estimate.lineage.get(attribute.exprId.id))
      columns.forall(_.isDefined) && columns.flatten.map(_._1).distinct.size == 1 && {
        val resolved = columns.flatten
        primaryKeys.get(resolved.head._1).contains(resolved.map(_._2))
      }
    }
  }

  /**
   * Return the base/lookup equi-key pairs when trusted constraints prove that an inner lookup
   * join preserves every base row exactly once.
   */
  def cardinalityPreservingLookupKeys(
      base: LogicalPlan,
      lookup: LogicalPlan,
      condition: Expression): Option[Seq[(Attribute, Attribute)]] = {
    for {
      baseEstimate <- estimateDetailed(base)
      lookupEstimate <- estimateDetailed(lookup)
      if onlyConstraintPreservingFilters(lookup, lookupEstimate)
      pairs <- equiPairs(condition, base.outputSet, lookup.outputSet)
      sourceColumns <- columnsFor(pairs.map(_._1), baseEstimate)
      targetColumns <- columnsFor(pairs.map(_._2), lookupEstimate)
      if sourceColumns.map(_._1).distinct.size == 1
      if targetColumns.map(_._1).distinct.size == 1
      sourceTable = sourceColumns.head._1
      targetTable = targetColumns.head._1
      sourceNames = sourceColumns.map(_._2)
      targetNames = targetColumns.map(_._2)
      if primaryKeys.get(targetTable).contains(targetNames)
      if foreignKeys.get(sourceTable -> sourceNames).contains(targetTable -> targetNames)
      if sourceNames.forall(notNullColumns.getOrElse(sourceTable, Set.empty).contains)
    } yield pairs
  }

  private def columnsFor(
      attributes: Seq[Attribute],
      estimate: DetailedEstimate): Option[Seq[(String, String)]] = {
    val columns = attributes.map(attribute => estimate.lineage.get(attribute.exprId.id))
    if (columns.forall(_.isDefined)) Some(columns.flatten) else None
  }

  private def equiPairs(
      condition: Expression,
      baseOutput: org.apache.spark.sql.catalyst.expressions.AttributeSet,
      lookupOutput: org.apache.spark.sql.catalyst.expressions.AttributeSet)
  : Option[Seq[(Attribute, Attribute)]] = {
    val pairs = splitAnd(condition).map {
      case EqualTo(left: Attribute, right: Attribute)
          if baseOutput.contains(left) && lookupOutput.contains(right) => Some(left -> right)
      case EqualTo(left: Attribute, right: Attribute)
          if baseOutput.contains(right) && lookupOutput.contains(left) => Some(right -> left)
      case _ => None
    }
    if (pairs.nonEmpty && pairs.forall(_.isDefined)) Some(pairs.flatten) else None
  }

  private def onlyConstraintPreservingFilters(
      plan: LogicalPlan,
      estimate: DetailedEstimate): Boolean = {
    plan.collect { case Filter(condition, _) => splitAnd(condition) }.flatten.forall {
      case IsNotNull(attribute: Attribute) =>
        estimate.lineage.get(attribute.exprId.id).exists {
          case (table, column) => notNullColumns.getOrElse(table, Set.empty).contains(column)
        }
      case _ => false
    }
  }

  /** Estimate projected/filter/join dimension chains from frozen row counts and NDVs. */
  private def estimateDetailed(plan: LogicalPlan): Option[DetailedEstimate] = plan match {
    case relation: LogicalRelation =>
      tableForRelation(relation).flatMap { table =>
        tableRows.get(table).map { rows =>
          val lineage = relation.output.map { attribute =>
            attribute.exprId.id -> (table -> normalizedColumn(attribute.name))
          }.toMap
          val distinct = lineage.flatMap {
            case (exprId, column) => distinctCounts.get(column).map(exprId -> _.min(rows))
          }
          DetailedEstimate(rows, lineage, distinct)
        }
      }

    case Filter(condition, child) =>
      estimateDetailed(child).map { input =>
        val selectivity = predicateSelectivity(input, condition)
        val rows = ceilRows(input.rows, selectivity)
        input.copy(rows = rows, distinct = input.distinct.map {
          case (exprId, ndv) => exprId -> ndv.min(rows)
        })
      }

    case project: Project =>
      estimateDetailed(project.child).map { input =>
        val mappings = project.projectList.zip(project.output).flatMap {
          case (source: Attribute, output) => Some(output.exprId.id -> source.exprId.id)
          case (Alias(source: Attribute, _), output) => Some(output.exprId.id -> source.exprId.id)
          case _ => None
        }
        DetailedEstimate(
          input.rows,
          mappings.flatMap {
            case (output, source) => input.lineage.get(source).map(output -> _)
          }.toMap,
          mappings.flatMap {
            case (output, source) => input.distinct.get(source).map(output -> _)
          }.toMap)
      }

    case alias: SubqueryAlias =>
      estimateDetailed(alias.child).map { input =>
        remapOutput(input, alias.child, alias)
      }

    case view: View =>
      estimateDetailed(view.child).map(remapOutput(_, view.child, view))

    case Join(left, right, Inner, Some(condition), _) =>
      for {
        leftEstimate <- estimateDetailed(left)
        rightEstimate <- estimateDetailed(right)
        denominator <- joinDenominator(condition, leftEstimate, rightEstimate)
      } yield {
        val rows = ((leftEstimate.rows * rightEstimate.rows + denominator - 1) / denominator)
          .max(BigInt(1))
        DetailedEstimate(
          rows,
          leftEstimate.lineage ++ rightEstimate.lineage,
          (leftEstimate.distinct ++ rightEstimate.distinct).map {
            case (exprId, ndv) => exprId -> ndv.min(rows)
          })
      }

    case _ => None
  }

  private def remapOutput(
      input: DetailedEstimate,
      source: LogicalPlan,
      output: LogicalPlan): DetailedEstimate = {
    val mappings = source.output.zip(output.output).map {
      case (sourceAttribute, outputAttribute) =>
        outputAttribute.exprId.id -> sourceAttribute.exprId.id
    }
    DetailedEstimate(
      input.rows,
      mappings.flatMap {
        case (outputExprId, sourceExprId) =>
          input.lineage.get(sourceExprId).map(outputExprId -> _)
      }.toMap,
      mappings.flatMap {
        case (outputExprId, sourceExprId) =>
          input.distinct.get(sourceExprId).map(outputExprId -> _)
      }.toMap)
  }

  private def joinDenominator(
      condition: Expression,
      left: DetailedEstimate,
      right: DetailedEstimate): Option[BigInt] = {
    val candidates = splitAnd(condition).flatMap {
      case EqualTo(leftAttribute: Attribute, rightAttribute: Attribute) =>
        val direct = trustedJoinDenominator(
          leftAttribute, rightAttribute, left, right)
        val reversed = trustedJoinDenominator(
          rightAttribute, leftAttribute, left, right)
        direct.orElse(reversed)
      case _ => None
    }
    if (candidates.nonEmpty) Some(candidates.max.max(BigInt(1))) else None
  }

  private def trustedJoinDenominator(
      leftAttribute: Attribute,
      rightAttribute: Attribute,
      left: DetailedEstimate,
      right: DetailedEstimate): Option[BigInt] = for {
    leftColumn <- left.lineage.get(leftAttribute.exprId.id)
    rightColumn <- right.lineage.get(rightAttribute.exprId.id)
    if isDeclaredSingleColumnForeignKey(leftColumn, rightColumn) ||
      isDeclaredSingleColumnForeignKey(rightColumn, leftColumn)
    leftNdv <- left.distinct.get(leftAttribute.exprId.id)
    rightNdv <- right.distinct.get(rightAttribute.exprId.id)
  } yield leftNdv.max(rightNdv)

  private def isDeclaredSingleColumnForeignKey(
      source: (String, String),
      target: (String, String)): Boolean = {
    val targetIsPrimary = primaryKeys.get(target._1).contains(Seq(target._2))
    targetIsPrimary && foreignKeys.get((source._1, Seq(source._2)))
      .contains((target._1, Seq(target._2)))
  }

  private def predicateSelectivity(
      estimate: DetailedEstimate,
      expression: Expression): BigDecimal = {
    val predicates = splitAnd(expression)
    val rangeSelectivities = rangeSelectivity(
      predicates,
      attribute => estimate.lineage.get(attribute.exprId.id).flatMap(valueRanges.get))
    val otherSelectivities = predicates.filterNot(isRangePredicate).map {
      atomicPredicateSelectivity(estimate, _)
    }
    (rangeSelectivities ++ otherSelectivities).reduceOption(_.min(_)).getOrElse(BigDecimal(1))
  }

  private def atomicPredicateSelectivity(
      estimate: DetailedEstimate,
      expression: Expression): BigDecimal = expression match {
    case Or(left, right) =>
      (predicateSelectivity(estimate, left) + predicateSelectivity(estimate, right))
        .min(BigDecimal(1))
    case EqualTo(attribute: Attribute, _: Literal) => equalitySelectivity(estimate, attribute)
    case EqualTo(_: Literal, attribute: Attribute) => equalitySelectivity(estimate, attribute)
    case EqualNullSafe(attribute: Attribute, _: Literal) => equalitySelectivity(estimate, attribute)
    case EqualNullSafe(_: Literal, attribute: Attribute) => equalitySelectivity(estimate, attribute)
    case In(attribute: Attribute, values) if values.nonEmpty && values.forall(_.foldable) =>
      inSelectivity(estimate, attribute, values.size)
    case InSet(attribute: Attribute, values) if values.nonEmpty =>
      inSelectivity(estimate, attribute, values.size)
    case Contains(_: Attribute, literal: Literal)
        if literal.value != null && literal.value.toString.nonEmpty => BigDecimal("0.25")
    case _ => BigDecimal(1)
  }

  private def rangeSelectivity(
      predicates: Seq[Expression],
      rangeFor: Attribute => Option[GpuOptimizerTrustedMetadata.ValueRange])
  : Seq[BigDecimal] = {
    predicates.flatMap(rangeBound).groupBy(_._1.exprId.id).values.flatMap { bounds =>
      val attribute = bounds.head._1
      rangeFor(attribute).map { domain =>
        val lower = bounds.flatMap(_._2).foldLeft(domain.min)(_.max(_)).max(domain.min)
        val upper = bounds.flatMap(_._3).foldLeft(domain.max)(_.min(_)).min(domain.max)
        if (upper <= lower) {
          BigDecimal(0)
        } else if (domain.max == domain.min) {
          BigDecimal(1)
        } else {
          ((upper - lower) / (domain.max - domain.min)).max(BigDecimal(0)).min(BigDecimal(1))
        }
      }
    }.toSeq
  }

  private def isRangePredicate(expression: Expression): Boolean = rangeBound(expression).nonEmpty

  private def rangeBound(
      expression: Expression): Option[(Attribute, Option[BigDecimal], Option[BigDecimal])] =
    expression match {
      case GreaterThan(attribute: Attribute, literal: Literal) =>
        literalValue(attribute, literal).map(value => (attribute, Some(value), None))
      case GreaterThanOrEqual(attribute: Attribute, literal: Literal) =>
        literalValue(attribute, literal).map(value => (attribute, Some(value), None))
      case LessThan(attribute: Attribute, literal: Literal) =>
        literalValue(attribute, literal).map(value => (attribute, None, Some(value)))
      case LessThanOrEqual(attribute: Attribute, literal: Literal) =>
        literalValue(attribute, literal).map(value => (attribute, None, Some(value)))
      case GreaterThan(literal: Literal, attribute: Attribute) =>
        literalValue(attribute, literal).map(value => (attribute, None, Some(value)))
      case GreaterThanOrEqual(literal: Literal, attribute: Attribute) =>
        literalValue(attribute, literal).map(value => (attribute, None, Some(value)))
      case LessThan(literal: Literal, attribute: Attribute) =>
        literalValue(attribute, literal).map(value => (attribute, Some(value), None))
      case LessThanOrEqual(literal: Literal, attribute: Attribute) =>
        literalValue(attribute, literal).map(value => (attribute, Some(value), None))
      case _ => None
    }

  private def literalValue(attribute: Attribute, literal: Literal): Option[BigDecimal] = {
    if (literal.value == null) {
      None
    } else if (attribute.dataType == DateType) {
      literal.value match {
        case value: Int => Some(BigDecimal(value))
        case value: java.sql.Date => Some(BigDecimal(value.toLocalDate.toEpochDay))
        case value: LocalDate => Some(BigDecimal(value.toEpochDay))
        case _ => None
      }
    } else {
      literal.value match {
        case value: java.lang.Number => Try(BigDecimal(value.toString)).toOption
        case _ => None
      }
    }
  }

  private def splitAnd(expression: Expression): Seq[Expression] = expression match {
    case And(left, right) => splitAnd(left) ++ splitAnd(right)
    case other => Seq(other)
  }

  private def equalitySelectivity(
      estimate: DetailedEstimate,
      attribute: Attribute): BigDecimal =
    estimate.distinct.get(attribute.exprId.id) match {
      case Some(ndv) if ndv > 0 => BigDecimal(1) / BigDecimal(ndv)
      case _ => BigDecimal(1)
    }

  private def inSelectivity(
      estimate: DetailedEstimate,
      attribute: Attribute,
      valueCount: Int): BigDecimal =
    estimate.distinct.get(attribute.exprId.id) match {
      case Some(ndv) if ndv > 0 =>
        (BigDecimal(valueCount) / BigDecimal(ndv)).min(BigDecimal(1))
      case _ => BigDecimal(1)
    }

  private def ceilRows(rows: BigInt, selectivity: BigDecimal): BigInt =
    (BigDecimal(rows) * selectivity)
      .setScale(0, BigDecimal.RoundingMode.CEILING)
      .toBigInt.max(BigInt(1))

  private def tableFor(plan: LogicalPlan): Option[String] = {
    val roots = plan.collect {
      case relation: LogicalRelation =>
        relation.relation match {
          case hadoop: HadoopFsRelation =>
            hadoop.location.rootPaths.map(path => normalizePath(path.toString))
          case _ => Seq.empty
        }
    }.flatten.distinct

    roots match {
      case Seq(root) =>
        val path = new Path(root)
        val parent = Option(path.getParent).map(p => normalizePath(p.toString))
        val table = path.getName.toLowerCase(java.util.Locale.ROOT)
        if (parent.contains(normalizedDatasetPath) && tableRows.contains(table)) {
          Some(table)
        } else {
          None
        }
      case _ => None
    }
  }

  private def tableForRelation(relation: LogicalRelation): Option[String] = {
    val roots = relation.relation match {
      case hadoop: HadoopFsRelation =>
        hadoop.location.rootPaths.map(path => normalizePath(path.toString)).distinct
      case _ => Seq.empty
    }
    roots match {
      case Seq(root) =>
        val path = new Path(root)
        val parent = Option(path.getParent).map(p => normalizePath(p.toString))
        val table = path.getName.toLowerCase(java.util.Locale.ROOT)
        if (parent.contains(normalizedDatasetPath) && tableRows.contains(table)) {
          Some(table)
        } else {
          None
        }
      case _ => None
    }
  }

  /**
   * Unknown predicates contribute selectivity 1. Conjunctions use the least selective recognized
   * bound rather than assuming column independence, so correlated predicates cannot make an
   * oversized branch look artificially small.
   */
  private def predicateSelectivity(table: String, expression: Expression): BigDecimal = {
    val predicates = splitAnd(expression)
    val rangeSelectivities = rangeSelectivity(
      predicates,
      attribute => valueRanges.get((table, normalizedColumn(attribute.name))))
    val otherSelectivities = predicates.filterNot(isRangePredicate).map {
      atomicPredicateSelectivity(table, _)
    }
    (rangeSelectivities ++ otherSelectivities).reduceOption(_.min(_)).getOrElse(BigDecimal(1))
  }

  private def atomicPredicateSelectivity(table: String, expression: Expression): BigDecimal =
    expression match {
      case Or(left, right) =>
        val leftSelectivity = predicateSelectivity(table, left)
        val rightSelectivity = predicateSelectivity(table, right)
        (leftSelectivity + rightSelectivity).min(BigDecimal(1))
      case EqualTo(attribute: Attribute, _: Literal) =>
        equalitySelectivity(table, attribute)
      case EqualTo(_: Literal, attribute: Attribute) =>
        equalitySelectivity(table, attribute)
      case EqualNullSafe(attribute: Attribute, _: Literal) =>
        equalitySelectivity(table, attribute)
      case EqualNullSafe(_: Literal, attribute: Attribute) =>
        equalitySelectivity(table, attribute)
      case In(attribute: Attribute, values) if values.nonEmpty && values.forall(_.foldable) =>
        inSelectivity(table, attribute, values.size)
      case InSet(attribute: Attribute, values) if values.nonEmpty =>
        inSelectivity(table, attribute, values.size)
      case Contains(_: Attribute, literal: Literal)
          if literal.value != null && literal.value.toString.nonEmpty =>
        // Spark and Presto statistics do not carry substring histograms. Use a conservative,
        // query-independent heuristic for a non-empty literal containment predicate.
        BigDecimal("0.25")
      case _ => BigDecimal(1)
    }

  private def equalitySelectivity(table: String, attribute: Attribute): BigDecimal =
    distinctCounts.get((table, normalizedColumn(attribute.name))) match {
      case Some(ndv) if ndv > 0 => BigDecimal(1) / BigDecimal(ndv)
      case _ => BigDecimal(1)
    }

  private def inSelectivity(table: String, attribute: Attribute, valueCount: Int): BigDecimal =
    distinctCounts.get((table, normalizedColumn(attribute.name))) match {
      case Some(ndv) if ndv > 0 =>
        (BigDecimal(valueCount) / BigDecimal(ndv)).min(BigDecimal(1))
      case _ => BigDecimal(1)
    }

  private def normalizePath(raw: String): String = {
    val value = new Path(raw).toUri.getPath
    if (value.length > 1) value.stripSuffix("/") else value
  }

  private def normalizedColumn(column: String): String =
    column.toLowerCase(java.util.Locale.ROOT)
}

private[rapids] object GpuOptimizerTrustedMetadata extends Logging {
  private[rapids] case class ValueRange(min: BigDecimal, max: BigDecimal)

  val pathConf = "spark.rapids.sql.optimizer.trustedMetadata.path"

  def fromSession(spark: SparkSession): Option[GpuOptimizerTrustedMetadata] = {
    fromConf(spark.sessionState.conf)
  }

  def fromConf(conf: org.apache.spark.sql.internal.SQLConf): Option[GpuOptimizerTrustedMetadata] = {
    val configuredPath = conf.getConfString(pathConf, "").trim
    if (configuredPath.isEmpty) {
      None
    } else {
      Some(load(configuredPath))
    }
  }

  private[rapids] def load(path: String): GpuOptimizerTrustedMetadata = {
    val properties = new Properties
    val stream = new FileInputStream(path)
    try {
      properties.load(stream)
    } finally {
      stream.close()
    }

    val values = properties.stringPropertyNames().asScala.map {
      key => key -> properties.getProperty(key).trim
    }.toMap
    val datasetPath = values.getOrElse(
      "dataset.path",
      throw new IllegalArgumentException(s"Missing dataset.path in trusted metadata $path"))
    val tableRows = values.collect {
      case (key, value) if key.startsWith("table.") && key.endsWith(".rowCount") =>
        val table = key.stripPrefix("table.").stripSuffix(".rowCount").toLowerCase(
          java.util.Locale.ROOT)
        table -> positiveBigInt(key, value, path)
    }
    val distinctCounts = values.collect {
      case (key, value) if key.startsWith("column.") && key.endsWith(".distinctCount") =>
        val components = key.stripPrefix("column.").stripSuffix(".distinctCount").split("\\.")
        if (components.length != 2) {
          throw new IllegalArgumentException(s"Invalid column statistic key $key in $path")
        }
        val table = components(0).toLowerCase(java.util.Locale.ROOT)
        val column = components(1).toLowerCase(java.util.Locale.ROOT)
        (table, column) -> positiveBigInt(key, value, path)
    }
    val rangeValues: Map[((String, String), String), BigDecimal] = values.collect {
      case (key, value) if key.startsWith("column.") &&
          (key.endsWith(".min") || key.endsWith(".max")) =>
        val suffix = if (key.endsWith(".min")) ".min" else ".max"
        val components = key.stripPrefix("column.").stripSuffix(suffix).split("\\.")
        if (components.length != 2) {
          throw new IllegalArgumentException(s"Invalid column range key $key in $path")
        }
        val table = components(0).toLowerCase(java.util.Locale.ROOT)
        val column = components(1).toLowerCase(java.util.Locale.ROOT)
        ((table, column), suffix.drop(1)) -> rangeValue(key, value, path)
    }
    val rangeColumns: Set[(String, String)] = rangeValues.keys.map(_._1).toSet
    val valueRanges: Map[(String, String), ValueRange] = rangeColumns.map { column =>
      val min = rangeValues.getOrElse(
        column -> "min", throw new IllegalArgumentException(s"Missing min for $column in $path"))
      val max = rangeValues.getOrElse(
        column -> "max", throw new IllegalArgumentException(s"Missing max for $column in $path"))
      if (max < min) {
        throw new IllegalArgumentException(s"Invalid range for $column in $path: $min > $max")
      }
      column -> ValueRange(min, max)
    }.toMap
    val primaryKeys = values.collect {
      case (key, value) if key.startsWith("primaryKey.") =>
        val table = key.stripPrefix("primaryKey.").toLowerCase(java.util.Locale.ROOT)
        table -> columns(value)
    }
    val foreignKeys = values.collect {
      case (key, value) if key.startsWith("foreignKey.") =>
        val source = qualifiedColumns(key.stripPrefix("foreignKey."), key, path)
        val target = qualifiedColumns(value, key, path)
        source -> target
    }
    val notNullColumns = values.collect {
      case (key, value) if key.startsWith("notNull.") =>
        val table = key.stripPrefix("notNull.").toLowerCase(java.util.Locale.ROOT)
        table -> columns(value).toSet
    }
    if (tableRows.isEmpty) {
      throw new IllegalArgumentException(s"No table row counts in trusted metadata $path")
    }
    val unknownTables = distinctCounts.keys.map(_._1).toSet -- tableRows.keySet
    if (unknownTables.nonEmpty) {
      throw new IllegalArgumentException(
        s"Column statistics reference unknown tables in $path: " +
          unknownTables.toSeq.sorted.mkString(","))
    }
    val invalidForeignKeys = foreignKeys.filter {
      case (_, (targetTable, targetColumns)) =>
        !primaryKeys.get(targetTable).contains(targetColumns)
    }.keys
    if (invalidForeignKeys.nonEmpty) {
      throw new IllegalArgumentException(
        s"Foreign keys do not reference declared primary keys in $path: " +
          invalidForeignKeys.toSeq.sortBy(_._1).mkString(","))
    }

    logWarning(
      s"Loaded trusted optimizer metadata path=$path dataset=$datasetPath " +
      s"tables=${tableRows.size} columns=${distinctCounts.size} " +
        s"primaryKeys=${primaryKeys.size} foreignKeys=${foreignKeys.size} " +
        s"notNullTables=${notNullColumns.size}")
    new GpuOptimizerTrustedMetadata(
      datasetPath, tableRows, distinctCounts, valueRanges, primaryKeys, foreignKeys,
      notNullColumns)
  }

  private def columns(value: String): Seq[String] =
    value.split(",").map(_.trim.toLowerCase(java.util.Locale.ROOT)).filter(_.nonEmpty).toSeq

  private def qualifiedColumns(
      value: String,
      key: String,
      path: String): (String, Seq[String]) = {
    val separator = value.indexOf('.')
    if (separator <= 0 || separator == value.length - 1) {
      throw new IllegalArgumentException(s"Invalid qualified columns $key=$value in $path")
    }
    val table = value.substring(0, separator).toLowerCase(java.util.Locale.ROOT)
    table -> columns(value.substring(separator + 1))
  }

  private def positiveBigInt(key: String, value: String, path: String): BigInt =
    Try(BigInt(value)).filter(_ > 0).getOrElse {
      throw new IllegalArgumentException(s"Invalid positive integer $key=$value in $path")
    }

  private def rangeValue(key: String, value: String, path: String): BigDecimal = {
    Try(BigDecimal(value)).orElse(Try(BigDecimal(LocalDate.parse(value).toEpochDay))).getOrElse {
      throw new IllegalArgumentException(s"Invalid range value $key=$value in $path")
    }
  }
}
