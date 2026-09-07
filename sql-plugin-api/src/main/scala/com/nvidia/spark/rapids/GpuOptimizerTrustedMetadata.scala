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
import java.util.Properties

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, Contains, EqualNullSafe}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, In, InSet, Literal, Or}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

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
    primaryKeys: Map[String, Seq[String]],
    foreignKeys: Map[(String, Seq[String]), (String, Seq[String])]) extends Logging {

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
        val mappings = alias.child.output.zip(alias.output).map {
          case (source, output) => output.exprId.id -> source.exprId.id
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
      expression: Expression): BigDecimal = expression match {
    case And(left, right) =>
      predicateSelectivity(estimate, left).min(predicateSelectivity(estimate, right))
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
  private def predicateSelectivity(table: String, expression: Expression): BigDecimal =
    expression match {
      case And(left, right) =>
        predicateSelectivity(table, left).min(predicateSelectivity(table, right))
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
  val pathConf = "spark.rapids.sql.optimizer.trustedMetadata.path"

  def fromSession(spark: SparkSession): Option[GpuOptimizerTrustedMetadata] = {
    val configuredPath = spark.sessionState.conf.getConfString(pathConf, "").trim
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
        s"primaryKeys=${primaryKeys.size} foreignKeys=${foreignKeys.size}")
    new GpuOptimizerTrustedMetadata(
      datasetPath, tableRows, distinctCounts, primaryKeys, foreignKeys)
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
}
