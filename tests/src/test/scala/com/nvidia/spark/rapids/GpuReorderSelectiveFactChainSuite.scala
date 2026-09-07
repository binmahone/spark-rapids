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
import java.sql.Date

import org.apache.commons.io.{FileUtils => ApacheFileUtils}

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, SHUFFLE_HASH}

class GpuReorderSelectiveFactChainSuite extends SparkQueryCompareTestSuite {

  private val enabledKey =
    "spark.rapids.sql.optimizer.reorderSelectiveFactChain.enabled"

  test("starts a Q10-shaped chain from filtered orders and lineitem") {
    val datasetDir = Files.createTempDirectory("selective-fact-chain-dataset").toFile
    val metadataFile = datasetDir.toPath.resolve("trusted-metadata.properties")
    val metadata =
      s"""dataset.path=${datasetDir.getCanonicalPath}
         |table.customer.rowCount=4500000000
         |column.customer.c_custkey.distinctCount=4500000000
         |column.customer.c_nationkey.distinctCount=25
         |table.orders.rowCount=45000000000
         |column.orders.o_orderkey.distinctCount=45000000000
         |column.orders.o_custkey.distinctCount=3001715912
         |column.orders.o_orderdate.distinctCount=2382
         |column.orders.o_orderdate.min=1992-01-01
         |column.orders.o_orderdate.max=1998-08-02
         |table.lineitem.rowCount=179999978268
         |column.lineitem.l_orderkey.distinctCount=46007131509
         |column.lineitem.l_returnflag.distinctCount=3
         |table.nation.rowCount=25
         |column.nation.n_nationkey.distinctCount=25
         |primaryKey.customer=c_custkey
         |primaryKey.orders=o_orderkey
         |primaryKey.nation=n_nationkey
         |foreignKey.orders.o_custkey=customer.c_custkey
         |foreignKey.lineitem.l_orderkey=orders.o_orderkey
         |foreignKey.customer.c_nationkey=nation.n_nationkey
         |""".stripMargin
    Files.write(metadataFile, metadata.getBytes(StandardCharsets.UTF_8))
    val conf = new SparkConf()
      .set(enabledKey, "true")
      .set(GpuOptimizerTrustedMetadata.pathConf, metadataFile.toString)

    try {
      withCpuSparkSession(
        spark => {
          import spark.implicits._

          Seq((1L, "customer", 0L)).toDF("c_custkey", "c_name", "c_nationkey")
            .write.parquet(datasetDir.toPath.resolve("customer").toString)
          Seq((10L, 1L, Date.valueOf("1993-11-01")))
            .toDF("o_orderkey", "o_custkey", "o_orderdate")
            .write.parquet(datasetDir.toPath.resolve("orders").toString)
          Seq((10L, 100.0, "R")).toDF("l_orderkey", "l_value", "l_returnflag")
            .write.parquet(datasetDir.toPath.resolve("lineitem").toString)
          Seq((0L, "nation")).toDF("n_nationkey", "n_name")
            .write.parquet(datasetDir.toPath.resolve("nation").toString)

          Seq("customer", "orders", "lineitem", "nation").foreach { table =>
            spark.read.parquet(datasetDir.toPath.resolve(table).toString)
              .createOrReplaceTempView(table)
          }
          val query =
            """SELECT c_custkey, c_name, n_name, SUM(l_value) AS revenue
              |FROM customer
              |JOIN orders ON c_custkey = o_custkey
              |JOIN lineitem ON o_orderkey = l_orderkey
              |JOIN nation ON c_nationkey = n_nationkey
              |WHERE o_orderdate >= DATE '1993-10-01'
              |  AND o_orderdate < DATE '1994-01-01'
              |  AND l_returnflag = 'R'
              |GROUP BY c_custkey, c_name, n_name
              |""".stripMargin
          spark.conf.set(enabledKey, "false")
          val original = spark.sql(query).queryExecution.optimizedPlan
          spark.conf.set(enabledKey, "true")
          val rewritten = GpuReorderSelectiveFactChain(spark)(original)

          assert(!rewritten.fastEquals(original), rewritten.treeString)
          assert(hasDirectOrdersLineitemJoin(rewritten), rewritten.treeString)
          assert(smallerOrdersSideHasShuffleHashHint(rewritten), rewritten.treeString)
          assert(smallerFactSideHasShuffleHashHint(rewritten), rewritten.treeString)
          spark.conf.set(enabledKey, "false")
          val baselineRows = spark.sql(query).collect().toSeq
          spark.conf.set(enabledKey, "true")
          val rewrittenRows = spark.sql(query).collect().toSeq
          assert(rewrittenRows == baselineRows, rewritten.treeString)

          spark.conf.set(enabledKey, "false")
          val onlyOrdersFiltered = spark.sql(
            """SELECT c_custkey, c_name, n_name, SUM(l_value) AS revenue
              |FROM customer
              |JOIN orders ON c_custkey = o_custkey
              |JOIN lineitem ON o_orderkey = l_orderkey
              |JOIN nation ON c_nationkey = n_nationkey
              |WHERE o_orderdate >= DATE '1993-10-01'
              |  AND o_orderdate < DATE '1994-01-01'
              |GROUP BY c_custkey, c_name, n_name
              |""".stripMargin).queryExecution.optimizedPlan
          spark.conf.set(enabledKey, "true")
          val guarded = GpuReorderSelectiveFactChain(spark)(onlyOrdersFiltered)

          assert(guarded.fastEquals(onlyOrdersFiltered), guarded.treeString)
        },
        conf)
    } finally {
      ApacheFileUtils.deleteDirectory(datasetDir)
    }
  }

  private def hasDirectOrdersLineitemJoin(plan: LogicalPlan): Boolean = plan.exists {
    case join: Join =>
      val names = join.output.map(_.name).toSet
      names.contains("o_orderkey") && names.contains("l_orderkey") &&
        !names.contains("c_custkey") && !names.contains("n_nationkey")
    case _ => false
  }

  private def smallerOrdersSideHasShuffleHashHint(plan: LogicalPlan): Boolean = plan.exists {
    case join: Join
        if join.left.output.exists(_.name == "o_orderkey") &&
          join.right.output.exists(_.name == "l_orderkey") =>
      join.hint.leftHint.flatMap(_.strategy).contains(SHUFFLE_HASH) &&
        join.hint.rightHint.isEmpty
    case _ => false
  }

  private def smallerFactSideHasShuffleHashHint(plan: LogicalPlan): Boolean = plan.exists {
    case join: Join
        if join.left.output.exists(_.name == "o_custkey") &&
          join.left.output.exists(_.name.startsWith("_rapids_measure_")) &&
          join.right.output.exists(_.name == "c_custkey") =>
      join.hint.leftHint.flatMap(_.strategy).contains(SHUFFLE_HASH) &&
        join.hint.rightHint.isEmpty
    case _ => false
  }
}
