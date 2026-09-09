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

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.commons.io.{FileUtils => ApacheFileUtils}

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, SHUFFLE_HASH}

class GpuPreferSmallerShuffleHashBuildSuite extends SparkQueryCompareTestSuite {

  private val enabledKey =
    "spark.rapids.sql.optimizer.preferSmallerShuffleHashBuild.enabled"

  test("selects only a materially smaller non-broadcast build side") {
    val datasetDir = Files.createTempDirectory("smaller-shuffle-build-dataset").toFile
    val metadataFile = datasetDir.toPath.resolve("trusted-metadata.properties")
    val metadata =
      s"""dataset.path=${datasetDir.getCanonicalPath}
         |table.small_side.rowCount=10000000000
         |table.large_side.rowCount=50000000000
         |table.near_side.rowCount=11000000000
         |""".stripMargin
    Files.write(metadataFile, metadata.getBytes(StandardCharsets.UTF_8))
    val conf = new SparkConf()
      .set(enabledKey, "true")
      .set(GpuOptimizerTrustedMetadata.pathConf, metadataFile.toString)
      .set("spark.sql.autoBroadcastJoinThreshold", "1m")

    try {
      withCpuSparkSession(
        spark => {
          import spark.implicits._

          Seq((1L, 10L)).toDF("small_key", "small_value")
            .write.parquet(datasetDir.toPath.resolve("small_side").toString)
          Seq((1L, 20L)).toDF("large_key", "large_value")
            .write.parquet(datasetDir.toPath.resolve("large_side").toString)
          Seq((1L, 30L)).toDF("near_key", "near_value")
            .write.parquet(datasetDir.toPath.resolve("near_side").toString)
          Seq("small_side", "large_side", "near_side").foreach { table =>
            spark.read.parquet(datasetDir.toPath.resolve(table).toString)
              .createOrReplaceTempView(table)
          }

          val smallerLeft = optimized(
            spark,
            "SELECT * FROM small_side JOIN large_side ON small_key = large_key")
          assert(leftShuffleHashHint(smallerLeft), smallerLeft.treeString)

          val smallerRight = optimized(
            spark,
            "SELECT * FROM large_side JOIN small_side ON large_key = small_key")
          assert(rightShuffleHashHint(smallerRight), smallerRight.treeString)

          val nearSized = optimized(
            spark,
            "SELECT * FROM small_side JOIN near_side ON small_key = near_key")
          assert(!hasShuffleHashHint(nearSized), nearSized.treeString)
        },
        conf)
    } finally {
      ApacheFileUtils.deleteDirectory(datasetDir)
    }
  }

  test("estimates a filtered composite-key lookup chain") {
    val datasetDir = Files.createTempDirectory("filtered-composite-lookup-dataset").toFile
    val metadataFile = datasetDir.toPath.resolve("trusted-metadata.properties")
    val metadata =
      s"""dataset.path=${datasetDir.getCanonicalPath}
         |table.fact.rowCount=1000
         |table.lookup.rowCount=400
         |table.filter_keys.rowCount=100
         |table.orders.rowCount=500
         |column.fact.fact_a.distinctCount=100
         |column.fact.fact_b.distinctCount=100
         |column.fact.fact_orderkey.distinctCount=500
         |column.lookup.lookup_a.distinctCount=100
         |column.lookup.lookup_b.distinctCount=100
         |column.filter_keys.filter_key.distinctCount=100
         |column.orders.order_key.distinctCount=500
         |primaryKey.lookup=lookup_a,lookup_b
         |primaryKey.filter_keys=filter_key
         |primaryKey.orders=order_key
         |foreignKey.fact.fact_a,fact_b=lookup.lookup_a,lookup_b
         |foreignKey.fact.fact_orderkey=orders.order_key
         |foreignKey.lookup.lookup_a=filter_keys.filter_key
         |notNull.fact=fact_a,fact_b,fact_orderkey
         |notNull.lookup=lookup_a,lookup_b
         |notNull.filter_keys=filter_key
         |notNull.orders=order_key
         |""".stripMargin
    Files.write(metadataFile, metadata.getBytes(StandardCharsets.UTF_8))
    val conf = new SparkConf()
      .set(enabledKey, "true")
      .set(GpuOptimizerTrustedMetadata.pathConf, metadataFile.toString)
      .set("spark.sql.autoBroadcastJoinThreshold", "1")

    try {
      withCpuSparkSession(
        spark => {
          import spark.implicits._

          Seq((1L, 1L, 1L)).toDF("fact_a", "fact_b", "fact_orderkey")
            .write.parquet(datasetDir.toPath.resolve("fact").toString)
          Seq((1L, 1L)).toDF("lookup_a", "lookup_b")
            .write.parquet(datasetDir.toPath.resolve("lookup").toString)
          Seq(1L).toDF("filter_key")
            .write.parquet(datasetDir.toPath.resolve("filter_keys").toString)
          Seq(1L).toDF("order_key")
            .write.parquet(datasetDir.toPath.resolve("orders").toString)
          Seq("fact", "lookup", "filter_keys", "orders").foreach { table =>
            spark.read.parquet(datasetDir.toPath.resolve(table).toString)
              .createOrReplaceTempView(table)
          }

          val rewritten = optimized(
            spark,
            """SELECT grouped.fact_orderkey
              |FROM (
              |  SELECT fact_orderkey, SUM(fact_a) AS total
              |  FROM fact
              |  JOIN (
              |    SELECT lookup_a, lookup_b
              |    FROM lookup
              |    WHERE EXISTS (
              |      SELECT 1
              |      FROM filter_keys
              |      WHERE filter_key = lookup_a AND filter_key = 1)) filtered_lookup
              |  ON fact_a = lookup_a AND fact_b = lookup_b
              |  GROUP BY fact_orderkey) grouped
              |JOIN orders ON grouped.fact_orderkey = order_key
              |""".stripMargin)
          assert(rewritten.exists {
            case join: Join if join.right.output.exists(_.name == "order_key") =>
              join.hint.leftHint.flatMap(_.strategy).contains(SHUFFLE_HASH) &&
                join.hint.rightHint.isEmpty
            case _ => false
          }, rewritten.treeString)
        },
        conf)
    } finally {
      ApacheFileUtils.deleteDirectory(datasetDir)
    }
  }

  private def optimized(spark: org.apache.spark.sql.SparkSession, sql: String): LogicalPlan = {
    spark.conf.set(enabledKey, "false")
    val original = spark.sql(sql).queryExecution.optimizedPlan
    spark.conf.set(enabledKey, "true")
    GpuPreferSmallerShuffleHashBuild(spark)(original)
  }

  private def leftShuffleHashHint(plan: LogicalPlan): Boolean = plan.exists {
    case join: Join =>
      join.hint.leftHint.flatMap(_.strategy).contains(SHUFFLE_HASH) &&
        join.hint.rightHint.isEmpty
    case _ => false
  }

  private def rightShuffleHashHint(plan: LogicalPlan): Boolean = plan.exists {
    case join: Join =>
      join.hint.rightHint.flatMap(_.strategy).contains(SHUFFLE_HASH) &&
        join.hint.leftHint.isEmpty
    case _ => false
  }

  private def hasShuffleHashHint(plan: LogicalPlan): Boolean =
    leftShuffleHashHint(plan) || rightShuffleHashHint(plan)
}
