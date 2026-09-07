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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
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

    sales
      .join(costs, col("part_key") === col("cost_part_key"))
      .join(orders, col("sale_id") === col("order_id"))
      .groupBy(expr("order_id % 2").as("bucket"))
      .agg(sum(expr("price * (1.0 - discount) - supply_cost")).as("profit"))
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
        case project: Project if project.projectList.exists(_.name.startsWith("_rapids_measure_")) =>
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
}
