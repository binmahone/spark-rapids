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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Validated access to statistics already attached to a Catalyst logical plan.
 *
 * This adapter does not invent missing cardinalities or load an external metadata manifest. A
 * caller that requires a row count or column statistic must abstain when this object returns
 * `None`. Spark does not attach a freshness timestamp to logical-plan statistics, so this API
 * deliberately exposes runtime provenance but makes no freshness claim for catalog statistics.
 */
private[rapids] object GpuCatalystStatistics {

  private[rapids] case class ColumnEstimate(
      distinctCount: Option[BigInt],
      min: Option[Any],
      max: Option[Any],
      nullCount: Option[BigInt],
      avgLen: Option[Long],
      maxLen: Option[Long])

  private[rapids] case class PlanEstimate(
      rowCount: BigInt,
      sizeInBytes: BigInt,
      isRuntime: Boolean,
      columns: Map[Long, ColumnEstimate]) {

    def column(attribute: Attribute): Option[ColumnEstimate] = {
      columns.get(attribute.exprId.id)
    }

    def distinctCount(attribute: Attribute): Option[BigInt] = {
      column(attribute).flatMap(_.distinctCount)
    }
  }

  /** Return internally consistent Catalyst statistics for the supplied plan. */
  def estimate(plan: LogicalPlan): Option[PlanEstimate] = {
    val statistics = plan.stats
    statistics.rowCount.filter(_ >= 0).flatMap { rows =>
      if (statistics.sizeInBytes < 0) {
        None
      } else {
        val columns = plan.output.flatMap { attribute =>
          statistics.attributeStats.get(attribute).flatMap { column =>
            validateColumn(rows, column.distinctCount, column.nullCount,
              column.avgLen, column.maxLen).map { _ =>
              attribute.exprId.id -> ColumnEstimate(
                column.distinctCount,
                column.min,
                column.max,
                column.nullCount,
                column.avgLen,
                column.maxLen)
            }
          }
        }.toMap
        Some(PlanEstimate(rows, statistics.sizeInBytes, statistics.isRuntime, columns))
      }
    }
  }

  /**
   * Estimate the uncompressed logical width of selected output attributes.
   *
   * ANALYZE TABLE may provide an average length for variable-width columns. Fixed-width or
   * unanalyzed columns use Spark's data-type default width. This is a cost estimate, not an
   * allocation or shuffle-byte guarantee.
   */
  def estimatedRowWidth(plan: LogicalPlan, attributes: Seq[Attribute]): Option[BigInt] = {
    estimate(plan).filter { _ =>
      attributes.forall(plan.outputSet.contains)
    }.map { planEstimate =>
      attributes.map { attribute =>
        planEstimate.column(attribute).flatMap(_.avgLen)
          .map(BigInt(_))
          .getOrElse(BigInt(attribute.dataType.defaultSize))
      }.sum
    }
  }

  private def validateColumn(
      rows: BigInt,
      distinctCount: Option[BigInt],
      nullCount: Option[BigInt],
      avgLen: Option[Long],
      maxLen: Option[Long]): Option[Unit] = {
    val countsAreValid = distinctCount.forall(count => count >= 0 && count <= rows) &&
      nullCount.forall(count => count >= 0 && count <= rows) &&
      (for {
        distinct <- distinctCount
        nulls <- nullCount
      } yield distinct + nulls <= rows).getOrElse(true)
    val lengthsAreValid = avgLen.forall(_ >= 0) && maxLen.forall(_ >= 0) &&
      (for {
        average <- avgLen
        maximum <- maxLen
      } yield average <= maximum).getOrElse(true)
    if (countsAreValid && lengthsAreValid) Some(()) else None
  }
}
