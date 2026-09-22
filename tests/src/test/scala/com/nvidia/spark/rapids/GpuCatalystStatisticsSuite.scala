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

import java.nio.file.Files

import org.apache.commons.io.{FileUtils => ApacheFileUtils}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LeafNode, Statistics}
import org.apache.spark.sql.types.{LongType, StringType}

class GpuCatalystStatisticsSuite extends AnyFunSuite {

  test("reads valid Catalyst row and column statistics") {
    val key = AttributeReference("key", LongType)()
    val payload = AttributeReference("payload", StringType)()
    val plan = CatalystStatisticsTestLeaf(
      Seq(key, payload),
      Statistics(
        sizeInBytes = 2400,
        rowCount = Some(100),
        attributeStats = AttributeMap(Seq(
          key -> ColumnStat(distinctCount = Some(20), nullCount = Some(0)),
          payload -> ColumnStat(
            distinctCount = Some(80),
            nullCount = Some(10),
            avgLen = Some(16),
            maxLen = Some(32)))),
        isRuntime = false))

    val estimate = GpuCatalystStatistics.estimate(plan).get
    assert(estimate.rowCount == 100)
    assert(estimate.sizeInBytes == 2400)
    assert(!estimate.isRuntime)
    assert(estimate.distinctCount(key).contains(20))
    assert(estimate.column(payload).flatMap(_.nullCount).contains(10))
    assert(GpuCatalystStatistics.estimatedRowWidth(plan, Seq(key, payload)).contains(24))
  }

  test("rejects a plan without an explicit row count") {
    val key = AttributeReference("key", LongType)()
    val plan = CatalystStatisticsTestLeaf(
      Seq(key),
      Statistics(sizeInBytes = 800, rowCount = None))

    assert(GpuCatalystStatistics.estimate(plan).isEmpty)
  }

  test("drops contradictory column statistics without rejecting valid plan statistics") {
    val key = AttributeReference("key", LongType)()
    val payload = AttributeReference("payload", StringType)()
    val plan = CatalystStatisticsTestLeaf(
      Seq(key, payload),
      Statistics(
        sizeInBytes = 2400,
        rowCount = Some(100),
        attributeStats = AttributeMap(Seq(
          key -> ColumnStat(distinctCount = Some(101), nullCount = Some(0)),
          payload -> ColumnStat(
            distinctCount = Some(80),
            nullCount = Some(30),
            avgLen = Some(33),
            maxLen = Some(32))))))

    val estimate = GpuCatalystStatistics.estimate(plan).get
    assert(estimate.column(key).isEmpty)
    assert(estimate.column(payload).isEmpty)
  }

  test("keeps zero-row statistics valid") {
    val key = AttributeReference("key", LongType)()
    val plan = CatalystStatisticsTestLeaf(
      Seq(key),
      Statistics(
        sizeInBytes = 0,
        rowCount = Some(0),
        attributeStats = AttributeMap(Seq(
          key -> ColumnStat(distinctCount = Some(0), nullCount = Some(0)))),
        isRuntime = true))

    val estimate = GpuCatalystStatistics.estimate(plan).get
    assert(estimate.rowCount == 0)
    assert(estimate.isRuntime)
    assert(estimate.distinctCount(key).contains(0))
  }
}

class GpuCatalystStatisticsCatalogSuite extends AnyFunSuite {

  test("reads statistics populated by ANALYZE TABLE") {
    val warehouse = Files.createTempDirectory("gpu-catalyst-statistics")
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("GpuCatalystStatisticsCatalogSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.planStats.enabled", "true")
      .config("spark.sql.warehouse.dir", warehouse.toUri.toString)
      .getOrCreate()
    try {
      val table = "gpu_catalyst_statistics_analyzed_table"
      try {
        spark.range(100)
          .selectExpr("id AS key", "CAST(id % 10 AS STRING) AS payload")
          .write
          .format("parquet")
          .mode("overwrite")
          .saveAsTable(table)
        spark.sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS key, payload")

        val plan = spark.table(table).queryExecution.optimizedPlan
        val estimate = GpuCatalystStatistics.estimate(plan).get
        val key = plan.output.find(_.name == "key").get
        val payload = plan.output.find(_.name == "payload").get

        assert(estimate.rowCount == 100)
        assert(estimate.sizeInBytes > 0)
        assert(estimate.distinctCount(key).exists(count => count > 0 && count <= 100))
        assert(estimate.distinctCount(payload).exists(count => count > 0 && count <= 100))
      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $table")
      }
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      ApacheFileUtils.deleteDirectory(warehouse.toFile)
    }
  }
}

private case class CatalystStatisticsTestLeaf(
    override val output: Seq[Attribute],
    statistics: Statistics) extends LeafNode {
  override def computeStats(): Statistics = statistics
}
