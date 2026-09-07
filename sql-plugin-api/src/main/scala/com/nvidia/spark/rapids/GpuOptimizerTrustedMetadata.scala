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
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Contains, EqualNullSafe, EqualTo}
import org.apache.spark.sql.catalyst.expressions.{Expression, In, InSet, Literal, Or}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
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
    distinctCounts: Map[(String, String), BigInt]) extends Logging {

  private val normalizedDatasetPath = normalizePath(datasetPath)

  private[rapids] case class Estimate(rows: BigInt, sizeInBytes: BigInt)

  /** Conservatively estimate a single-table branch using recognized literal predicates. */
  def estimate(plan: LogicalPlan): Option[Estimate] = {
    tableFor(plan).flatMap {
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
    }
  }

  def estimateRows(plan: LogicalPlan): Option[BigInt] = estimate(plan).map(_.rows)

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
    if (tableRows.isEmpty) {
      throw new IllegalArgumentException(s"No table row counts in trusted metadata $path")
    }
    val unknownTables = distinctCounts.keys.map(_._1).toSet -- tableRows.keySet
    if (unknownTables.nonEmpty) {
      throw new IllegalArgumentException(
        s"Column statistics reference unknown tables in $path: " +
          unknownTables.toSeq.sorted.mkString(","))
    }

    logWarning(
      s"Loaded trusted optimizer metadata path=$path dataset=$datasetPath " +
        s"tables=${tableRows.size} columns=${distinctCounts.size}")
    new GpuOptimizerTrustedMetadata(datasetPath, tableRows, distinctCounts)
  }

  private def positiveBigInt(key: String, value: String, path: String): BigInt =
    Try(BigInt(value)).filter(_ > 0).getOrElse {
      throw new IllegalArgumentException(s"Invalid positive integer $key=$value in $path")
    }
}
