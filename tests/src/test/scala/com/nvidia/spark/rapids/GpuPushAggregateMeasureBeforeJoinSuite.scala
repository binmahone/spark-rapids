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
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, Project}
import org.apache.spark.sql.functions.{col, expr, max, sum}

class GpuPushAggregateMeasureBeforeJoinSuite extends SparkQueryCompareTestSuite {

  private val enabledKey =
    "spark.rapids.sql.optimizer.pushAggregateMeasureBeforeJoin.enabled"

  private def conf(enabled: Boolean): SparkConf = new SparkConf()
    .set(enabledKey, enabled.toString)
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")

  private def distributiveSumQuery(spark: org.apache.spark.sql.SparkSession): DataFrame = {
    val measures = spark.range(0, 12)
      .selectExpr(
        "id % 4 AS join_key",
        "CAST(id + 1 AS DOUBLE) AS price",
        "CAST((id % 3) / 10.0 AS DOUBLE) AS discount")
      .where("price > 0")
    val duplicatedKeys = spark.range(0, 4)
      .selectExpr("id AS other_key")
      .union(spark.range(0, 2).selectExpr("id AS other_key"))

    measures
      .join(duplicatedKeys, col("join_key") === col("other_key"))
      .groupBy("join_key")
      .agg(sum(expr("price * (1.0 - discount)")).as("revenue"))
  }

  private def materializationOnlyQuery(
      spark: org.apache.spark.sql.SparkSession): DataFrame = {
    val measures = spark.range(0, 12)
      .selectExpr(
        "id % 4 AS join_key",
        "CAST(id + 1 AS DOUBLE) AS price",
        "CAST((id % 3) / 10.0 AS DOUBLE) AS discount")
      .where("price > 0")
    val keys = spark.range(0, 4).selectExpr("id AS other_key")

    measures
      .join(keys, col("join_key") === col("other_key"), "left_outer")
      .groupBy("join_key")
      .agg(sum(expr("price * (1.0 - discount)")).as("revenue"))
  }

  private def compositeSumWithOtherMeasure(
      spark: org.apache.spark.sql.SparkSession): DataFrame = {
    val measures = spark.range(0, 12)
      .selectExpr(
        "id % 4 AS join_key",
        "CAST(id + 1 AS DOUBLE) AS price",
        "CAST((id % 3) / 10.0 AS DOUBLE) AS discount")
      .where("price > 0")
    val keys = spark.range(0, 4).selectExpr("id AS other_key")

    measures
      .join(keys, col("join_key") === col("other_key"))
      .groupBy("join_key")
      .agg(
        sum(expr("price * (1.0 - discount)")).as("revenue"),
        max("price").as("max_price"))
  }

  private def crossJoinMeasureQuery(
      spark: org.apache.spark.sql.SparkSession): DataFrame = {
    val sales = spark.range(0, 12)
      .selectExpr(
        "id AS sale_id",
        "id % 4 AS part_key",
        "CAST(id + 1 AS DOUBLE) AS price",
        "CAST((id % 3) / 10.0 AS DOUBLE) AS discount")
    val costs = spark.range(0, 4)
      .selectExpr(
        "id AS cost_part_key",
        "CAST(id + 0.5 AS DOUBLE) AS supply_cost")
    val orders = spark.range(0, 12).selectExpr("id AS order_id")

    val profit = sales
      .join(costs, col("part_key") === col("cost_part_key"))
      .join(orders, col("sale_id") === col("order_id"))
      .select(
        expr("order_id % 2").as("bucket"),
        expr("price * (1.0 - discount) - supply_cost").as("amount"))

    profit.groupBy("bucket").agg(sum("amount").as("profit"))
  }

  private def globalSumJoinQuery(spark: org.apache.spark.sql.SparkSession): DataFrame = {
    val facts = spark.range(0, 12)
      .selectExpr(
        "id % 4 AS join_key",
        "CAST(id + 1 AS DOUBLE) AS price",
        "CAST((id % 3) / 10.0 AS DOUBLE) AS discount")
    val dimensions = spark.range(0, 6)
      .selectExpr("id % 4 AS other_key", "id % 2 AS selected")
      .where("selected = 1")

    facts
      .join(dimensions, col("join_key") === col("other_key"))
      .agg(sum(expr("price * (1.0 - discount)")).as("total"))
  }

  private def normalized(rows: Array[Row]): Seq[String] = rows.map(_.toString).sorted.toSeq

  test("pre-aggregate a distributive SUM before an inner join") {
    var expected = Seq.empty[String]
    withCpuSparkSession(spark => {
      expected = normalized(distributiveSumQuery(spark).collect())
    }, conf(enabled = false))

    withCpuSparkSession(spark => {
      val query = distributiveSumQuery(spark)
      val optimized = query.queryExecution.optimizedPlan
      val aggregates = optimized.collect { case aggregate: Aggregate => aggregate }

      assert(normalized(query.collect()) === expected)
      assert(aggregates.size >= 2, optimized.treeString)
      assert(optimized.treeString.contains("_rapids_measure_"), optimized.treeString)
      assert(aggregates.exists(_.groupingExpressions.exists(_.toString.contains("join_key"))),
        optimized.treeString)
    }, conf(enabled = true))
  }

  test("do not pre-aggregate through an outer join") {
    withCpuSparkSession(spark => {
      val optimized = materializationOnlyQuery(spark).queryExecution.optimizedPlan
      val aggregates = optimized.collect { case aggregate: Aggregate => aggregate }

      assert(aggregates.size === 1, optimized.treeString)
      assert(optimized.treeString.contains("_rapids_measure_"), optimized.treeString)
    }, conf(enabled = true))
  }

  test("do not rewrite when another aggregate uses a measure input") {
    withCpuSparkSession(spark => {
      val optimized = compositeSumWithOtherMeasure(spark).queryExecution.optimizedPlan

      assert(!optimized.treeString.contains("_rapids_measure_"), optimized.treeString)
    }, conf(enabled = true))
  }

  test("materialize a measure at the first join that supplies all inputs") {
    var expected = Seq.empty[String]
    withCpuSparkSession(spark => {
      expected = normalized(crossJoinMeasureQuery(spark).collect())
    }, conf(enabled = false))

    withCpuSparkSession(spark => {
      val query = crossJoinMeasureQuery(spark)
      val optimized = query.queryExecution.optimizedPlan
      val measureProjects = optimized.collect {
        case project: Project if project.projectList.exists {
              case alias: Alias => alias.name == "amount"
              case _ => false
            } =>
          project
      }

      assert(normalized(query.collect()) === expected)
      assert(measureProjects.size === 1, optimized.treeString)
      assert(measureProjects.head.child.isInstanceOf[Join], optimized.treeString)
      assert(measureProjects.head.child.asInstanceOf[Join].left.output.exists(_.name == "price"),
        optimized.treeString)
      assert(
        measureProjects.head.child.asInstanceOf[Join].right.output.exists(
          _.name == "supply_cost"),
        optimized.treeString)
      val orderJoin = optimized.collectFirst {
        case join: Join if join.right.output.exists(_.name == "order_id") => join
      }.getOrElse(fail(optimized.treeString))
      assert(
        !orderJoin.output.exists(_.name == "price"),
        optimized.treeString)
    }, conf(enabled = true))
  }

  test("push a global sum below its inner join and preserve join multiplicity") {
    var expected = 0.0
    withCpuSparkSession(spark => {
      expected = globalSumJoinQuery(spark).collect().head.getDouble(0)
    }, conf(enabled = false))

    withCpuSparkSession(spark => {
      val query = globalSumJoinQuery(spark)
      val optimized = query.queryExecution.optimizedPlan
      val aggregates = optimized.collect { case aggregate: Aggregate => aggregate }

      assert(math.abs(query.collect().head.getDouble(0) - expected) < 1e-9)
      assert(aggregates.size >= 2, optimized.treeString)
      assert(optimized.treeString.contains("_rapids_pre_sum_"), optimized.treeString)
    }, conf(enabled = true))
  }

  test("push grouped sums through trusted lookup joins") {
    val datasetDir = Files.createTempDirectory("lookup-sum-dataset").toFile
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
         |notNull.customer=c_custkey,c_nationkey
         |notNull.orders=o_orderkey,o_custkey
         |notNull.lineitem=l_orderkey
         |notNull.nation=n_nationkey
         |""".stripMargin
    Files.write(metadataFile, metadata.getBytes(StandardCharsets.UTF_8))
    val testConf = conf(enabled = true)
      .set(GpuOptimizerTrustedMetadata.pathConf, metadataFile.toString)
      .set("spark.rapids.sql.optimizer.reorderSelectiveFactChain.enabled", "true")

    try {
      withCpuSparkSession(spark => {
        import spark.implicits._

        Seq((1L, "customer-1", 0L), (2L, "customer-2", 0L))
          .toDF("c_custkey", "c_name", "c_nationkey")
          .write.parquet(datasetDir.toPath.resolve("customer").toString)
        Seq(
          (10L, 1L, Date.valueOf("1993-11-01")),
          (11L, 1L, Date.valueOf("1993-12-01")),
          (12L, 2L, Date.valueOf("1993-11-01")))
          .toDF("o_orderkey", "o_custkey", "o_orderdate")
          .write.parquet(datasetDir.toPath.resolve("orders").toString)
        Seq((10L, 3.0, "R"), (10L, 5.0, "N"), (11L, 7.0, "R"), (12L, 11.0, "R"))
          .toDF("l_orderkey", "l_value", "l_returnflag")
          .write.parquet(datasetDir.toPath.resolve("lineitem").toString)
        Seq((0L, "nation-0"), (1L, "nation-1"))
          .toDF("n_nationkey", "n_name")
          .write.parquet(datasetDir.toPath.resolve("nation").toString)

        Seq("customer", "orders", "lineitem", "nation").foreach { table =>
          spark.read.parquet(datasetDir.toPath.resolve(table).toString)
            .createOrReplaceTempView(table)
        }
        val sql =
          """SELECT c_custkey, c_name, n_name, SUM(l_value) AS revenue
            |FROM customer
            |JOIN orders ON c_custkey = o_custkey
            |  AND o_orderdate >= DATE '1993-10-01'
            |  AND o_orderdate < DATE '1994-01-01'
            |JOIN lineitem ON o_orderkey = l_orderkey AND l_returnflag = 'R'
            |JOIN nation ON c_nationkey = n_nationkey
            |GROUP BY c_custkey, c_name, n_name
            |""".stripMargin
        spark.conf.set(enabledKey, "false")
        val expected = normalized(spark.sql(sql).collect())
        spark.conf.set(enabledKey, "true")
        val query = spark.sql(sql)
        val optimized = query.queryExecution.optimizedPlan

        assert(normalized(query.collect()) === expected)
        assert(optimized.treeString.contains("_rapids_lookup_pre_sum_"), optimized.treeString)
        assert(optimized.collect { case aggregate: Aggregate => aggregate }.size === 1,
          optimized.treeString)
        val pushedAggregate = optimized.collectFirst {
          case aggregate: Aggregate if aggregate.output.exists(
              _.name.startsWith("_rapids_lookup_pre_sum_")) => aggregate
        }.getOrElse(fail(optimized.treeString))
        assert(!pushedAggregate.groupingExpressions.exists(_.references.exists(
          _.name.startsWith("c_"))), optimized.treeString)

        val coarseSql =
          """SELECT n_name, SUM(l_value) AS revenue
            |FROM customer
            |JOIN orders ON c_custkey = o_custkey
            |  AND o_orderdate >= DATE '1993-10-01'
            |  AND o_orderdate < DATE '1994-01-01'
            |JOIN lineitem ON o_orderkey = l_orderkey AND l_returnflag = 'R'
            |JOIN nation ON c_nationkey = n_nationkey
            |GROUP BY n_name
            |""".stripMargin
        spark.conf.set(enabledKey, "false")
        val coarseExpected = normalized(spark.sql(coarseSql).collect())
        spark.conf.set(enabledKey, "true")
        val coarseQuery = spark.sql(coarseSql)
        val coarseOptimized = coarseQuery.queryExecution.optimizedPlan

        assert(normalized(coarseQuery.collect()) === coarseExpected)
        assert(coarseOptimized.treeString.contains("_rapids_lookup_pre_sum_"),
          coarseOptimized.treeString)
        assert(coarseOptimized.collect { case aggregate: Aggregate => aggregate }.size >= 2,
          coarseOptimized.treeString)
      }, testConf)
    } finally {
      ApacheFileUtils.deleteDirectory(datasetDir)
    }
  }
}
