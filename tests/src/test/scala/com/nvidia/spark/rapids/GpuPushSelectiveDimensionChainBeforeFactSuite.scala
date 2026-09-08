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
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, EqualTo}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner}
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, BROADCAST, Filter, HintInfo, Join}
import org.apache.spark.sql.catalyst.plans.logical.{JoinHint, SHUFFLE_HASH}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Project}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StringType}

class GpuPushSelectiveDimensionChainBeforeFactSuite extends SparkQueryCompareTestSuite {

  private val enabledKey =
    "spark.rapids.sql.optimizer.pushDimensionChainBeforeFact.enabled"
  private val maxChainRowsKey =
    "spark.rapids.sql.optimizer.pushDimensionChainBeforeFact.maxChainRows"
  private val maxChainBytesKey =
    "spark.rapids.sql.optimizer.pushDimensionChainBeforeFact.maxChainBytes"
  private val pushSelectiveKeysetKey =
    "spark.rapids.sql.optimizer.pushSelectiveKeysetToJoinInputs.enabled"

  private def conf: SparkConf = new SparkConf().set(enabledKey, "true")

  test("reorders a dimension chain with an independent selective leaf") {
    withCpuSparkSession(
      spark => {
        val testPlan = q8LikePlan(addCompetingEdge = false)
        val rewritten = GpuPushSelectiveDimensionChainBeforeFact(spark)(testPlan.plan)

        assert(!rewritten.fastEquals(testPlan.plan), rewritten.treeString)
        assert(
          containsJoinedBranches(rewritten, testPlan.customer, testPlan.nation, testPlan.region),
          rewritten.treeString)
        assert(
          !broadcastsAccumulatedBranch(
            rewritten,
            testPlan.customer,
            testPlan.orders,
            testPlan.lineitem),
          rewritten.treeString)
        assert(rewritten.outputSet == testPlan.plan.outputSet, rewritten.treeString)
      },
      conf)
  }

  test("rejects a selective entrance connected to multiple branches") {
    withCpuSparkSession(
      spark => {
        val testPlan = q8LikePlan(addCompetingEdge = true)
        val rewritten = GpuPushSelectiveDimensionChainBeforeFact(spark)(testPlan.plan)

        assert(rewritten.fastEquals(testPlan.plan), rewritten.treeString)
      },
      conf)
  }

  test("preserves a partitioned join boundary while rewriting its child cluster") {
    withCpuSparkSession(
      spark => {
        val testPlan = q8LikePlan(addCompetingEdge = false)
        val stateKey = AttributeReference("state_key", LongType)()
        val state = SelectiveDimensionStatRel(Seq(stateKey), 1000000L)
        val probeKey = testPlan.lineitem.output.find(_.name == "l_orderkey").get
        val partitionedHint = JoinHint(
          None,
          Some(HintInfo(strategy = Some(SHUFFLE_HASH))))
        val original = Join(
          testPlan.plan,
          state,
          Inner,
          Some(EqualTo(probeKey, stateKey)),
          partitionedHint)

        val rewritten = GpuPushSelectiveDimensionChainBeforeFact(spark)(original)
        val rewrittenJoin = rewritten.asInstanceOf[Join]

        assert(!rewrittenJoin.left.fastEquals(original.left), rewritten.treeString)
        assert(rewrittenJoin.right.fastEquals(state), rewritten.treeString)
        assert(rewrittenJoin.hint == partitionedHint, rewritten.treeString)
        assert(
          containsJoinedBranches(
            rewrittenJoin.left,
            testPlan.customer,
            testPlan.nation,
            testPlan.region),
          rewritten.treeString)
      },
      conf)
  }

  test("does not hint a smaller scan whose materialized output exceeds broadcast threshold") {
    withCpuSparkSession(
      spark => {
        val leftKey = AttributeReference("left_key", LongType)()
        val rightKey = AttributeReference("right_key", LongType)()
        val left = SelectiveDimensionStatRel(Seq(leftKey), 100L)
        val right = SelectiveDimensionStatRel(Seq(rightKey), 200L)
        val rule = GpuPushSelectiveDimensionChainBeforeFact(spark)

        val rejected = rule.broadcastSmallerHint(
          left, right, allowLeft = true, allowRight = true)
        assert(rejected == JoinHint.NONE)
      },
      conf.set("spark.sql.autoBroadcastJoinThreshold", "1k"))
  }

  test("hints the smaller materialized output when it fits broadcast threshold") {
    withCpuSparkSession(
      spark => {
        val leftKey = AttributeReference("left_key", LongType)()
        val rightKey = AttributeReference("right_key", LongType)()
        val left = SelectiveDimensionStatRel(Seq(leftKey), 100L)
        val right = SelectiveDimensionStatRel(Seq(rightKey), 200L)
        val rule = GpuPushSelectiveDimensionChainBeforeFact(spark)

        val accepted = rule.broadcastSmallerHint(
          left, right, allowLeft = true, allowRight = true)
        assert(accepted.leftHint.exists(_.strategy.contains(BROADCAST)))
        assert(accepted.rightHint.isEmpty)
      },
      conf.set("spark.sql.autoBroadcastJoinThreshold", "2k"))
  }

  test("does not hint a narrow relation above the broadcast row limit") {
    withCpuSparkSession(
      spark => {
        val leftKey = AttributeReference("left_key", LongType)()
        val rightKey = AttributeReference("right_key", LongType)()
        val left = SelectiveDimensionStatRel(Seq(leftKey), 600000000L)
        val right = SelectiveDimensionStatRel(Seq(rightKey), 1000000000L)
        val rule = GpuPushSelectiveDimensionChainBeforeFact(spark)

        val rejected = rule.broadcastSmallerHint(
          left, right, allowLeft = true, allowRight = true)
        assert(rejected == JoinHint.NONE)
      },
      conf.set("spark.sql.autoBroadcastJoinThreshold", "8g"))
  }

  test("uses trusted path statistics to prebuild an independent selective leaf") {
    val datasetDir = Files.createTempDirectory("trusted-metadata-dataset").toFile
    val partDir = datasetDir.toPath.resolve("part")
    val metadataFile = datasetDir.toPath.resolve("trusted-metadata.properties")
    val metadata =
      s"""dataset.path=${datasetDir.getCanonicalPath}
         |table.part.rowCount=6000000000
         |column.part.p_type.distinctCount=150
         |""".stripMargin
    Files.write(metadataFile, metadata.getBytes(StandardCharsets.UTF_8))
    val trustedConf = conf
      .set(maxChainRowsKey, "50000000")
      .set(maxChainBytesKey, "2g")
      .set(GpuOptimizerTrustedMetadata.pathConf, metadataFile.toString)

    try {
      withCpuSparkSession(
        spark => {
          import spark.implicits._

          Seq((1L, "ECONOMY ANODIZED STEEL"), (2L, "OTHER"))
            .toDF("p_partkey", "p_type")
            .write.parquet(partDir.toString)
          val part = spark.read.parquet(partDir.toString)
            .filter("p_type = 'ECONOMY ANODIZED STEEL'")
            .queryExecution.analyzed
          val testPlan = q8LikePlan(addCompetingEdge = false, Some(part))
          val rewritten = GpuPushSelectiveDimensionChainBeforeFact(spark)(testPlan.plan)

          assert(!rewritten.fastEquals(testPlan.plan), rewritten.treeString)
          assert(
            containsDirectJoin(rewritten, testPlan.lineitem, testPlan.part),
            rewritten.treeString)
          assert(rewritten.outputSet == testPlan.plan.outputSet, rewritten.treeString)
        },
        trustedConf)
    } finally {
      ApacheFileUtils.deleteDirectory(datasetDir)
    }
  }

  test("broadcasts a literal-contains dimension keyset using trusted statistics") {
    val datasetDir = Files.createTempDirectory("trusted-contains-dataset").toFile
    val partDir = datasetDir.toPath.resolve("part")
    val metadataFile = datasetDir.toPath.resolve("trusted-metadata.properties")
    val metadata =
      s"""dataset.path=${datasetDir.getCanonicalPath}
         |table.part.rowCount=1000000000
         |column.part.p_partkey.distinctCount=1000000000
         |""".stripMargin
    Files.write(metadataFile, metadata.getBytes(StandardCharsets.UTF_8))
    val trustedConf = conf
      .set("spark.sql.autoBroadcastJoinThreshold", "12g")
      .set(GpuOptimizerTrustedMetadata.pathConf, metadataFile.toString)

    try {
      withCpuSparkSession(
        spark => {
          import spark.implicits._

          Seq((1L, "forest green"), (2L, "red"))
            .toDF("p_partkey", "p_name")
            .write.parquet(partDir.toString)
          val part = spark.read.parquet(partDir.toString)
            .filter(col("p_name").contains("green"))
            .select("p_partkey")
            .queryExecution.analyzed
          val lPartKey = AttributeReference("l_partkey", LongType)()
          val lineitem = SelectiveDimensionStatRel(Seq(lPartKey), 180000000000L)
          val pPartKey = part.output.find(_.name == "p_partkey").get
          val original = join(lineitem, part, EqualTo(lPartKey, pPartKey))
          val rewritten = GpuBroadcastSelectiveFilteredDimension(spark)(original)

          val rewrittenJoin = rewritten.asInstanceOf[Join]
          assert(rewrittenJoin.hint.rightHint.exists(_.strategy.contains(BROADCAST)),
            rewritten.treeString)
          assert(rewrittenJoin.left.fastEquals(lineitem), rewritten.treeString)
          assert(rewrittenJoin.right.fastEquals(part), rewritten.treeString)
        },
        trustedConf)
    } finally {
      ApacheFileUtils.deleteDirectory(datasetDir)
    }
  }

  test("estimates a filtered PK-FK-like dimension chain from trusted NDVs") {
    val datasetDir = Files.createTempDirectory("trusted-dimension-chain-dataset").toFile
    val regionDir = datasetDir.toPath.resolve("region")
    val nationDir = datasetDir.toPath.resolve("nation")
    val supplierDir = datasetDir.toPath.resolve("supplier")
    val metadataFile = datasetDir.toPath.resolve("trusted-metadata.properties")
    val metadata =
      s"""dataset.path=${datasetDir.getCanonicalPath}
         |table.region.rowCount=5
         |column.region.r_regionkey.distinctCount=5
         |column.region.r_name.distinctCount=5
         |table.nation.rowCount=25
         |column.nation.n_nationkey.distinctCount=25
         |column.nation.n_regionkey.distinctCount=5
         |table.supplier.rowCount=300000000
         |column.supplier.s_suppkey.distinctCount=300000000
         |column.supplier.s_nationkey.distinctCount=25
         |primaryKey.region=r_regionkey
         |primaryKey.nation=n_nationkey
         |primaryKey.supplier=s_suppkey
         |foreignKey.nation.n_regionkey=region.r_regionkey
         |foreignKey.supplier.s_nationkey=nation.n_nationkey
         |""".stripMargin
    Files.write(metadataFile, metadata.getBytes(StandardCharsets.UTF_8))
    val trustedConf = conf
      .set("spark.sql.autoBroadcastJoinThreshold", "12g")
      .set(GpuOptimizerTrustedMetadata.pathConf, metadataFile.toString)

    try {
      withCpuSparkSession(
        spark => {
          import spark.implicits._

          Seq((0L, "ASIA"), (1L, "EUROPE"))
            .toDF("r_regionkey", "r_name").write.parquet(regionDir.toString)
          Seq((0L, 0L, "CHINA"), (1L, 1L, "FRANCE"))
            .toDF("n_nationkey", "n_regionkey", "n_name").write.parquet(nationDir.toString)
          Seq((0L, 0L), (1L, 1L))
            .toDF("s_suppkey", "s_nationkey").write.parquet(supplierDir.toString)
          spark.read.parquet(regionDir.toString).createOrReplaceTempView("trusted_region")
          spark.read.parquet(nationDir.toString).createOrReplaceTempView("trusted_nation")
          spark.read.parquet(supplierDir.toString).createOrReplaceTempView("trusted_supplier")

          val chain = spark.sql(
            """SELECT s_suppkey, n_name
              |FROM trusted_supplier
              |JOIN trusted_nation ON s_nationkey = n_nationkey
              |JOIN trusted_region ON n_regionkey = r_regionkey
              |WHERE r_name = 'ASIA'""".stripMargin).queryExecution.analyzed
          val estimate = GpuOptimizerTrustedMetadata.load(metadataFile.toString).estimate(chain)

          assert(estimate.exists(_.rows == 60000000), s"$estimate\n${chain.treeString}")
          assert(
            estimate.exists(_.sizeInBytes < 12L * 1024L * 1024L * 1024L),
            s"$estimate\n${chain.treeString}")
        },
        trustedConf)
    } finally {
      ApacheFileUtils.deleteDirectory(datasetDir)
    }
  }

  test("estimates a bounded date range from trusted min and max statistics") {
    val datasetDir = Files.createTempDirectory("trusted-date-range-dataset").toFile
    val ordersDir = datasetDir.toPath.resolve("orders")
    val metadataFile = datasetDir.toPath.resolve("trusted-metadata.properties")
    val metadata =
      s"""dataset.path=${datasetDir.getCanonicalPath}
         |table.orders.rowCount=45000000000
         |column.orders.o_orderdate.distinctCount=2382
         |column.orders.o_orderdate.min=1992-01-01
         |column.orders.o_orderdate.max=1998-08-02
         |""".stripMargin
    Files.write(metadataFile, metadata.getBytes(StandardCharsets.UTF_8))

    try {
      withCpuSparkSession(
        spark => {
          import spark.implicits._

          Seq(Date.valueOf("1993-10-01"), Date.valueOf("1994-01-01"))
            .toDF("o_orderdate")
            .write.parquet(ordersDir.toString)
          val orders = spark.read.parquet(ordersDir.toString)
            .filter("o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1994-01-01'")
            .queryExecution.analyzed
          val estimate = GpuOptimizerTrustedMetadata.load(metadataFile.toString).estimate(orders)

          assert(estimate.exists(_.rows == 1721413722L), s"$estimate\n${orders.treeString}")
        },
        conf.set(GpuOptimizerTrustedMetadata.pathConf, metadataFile.toString))
    } finally {
      ApacheFileUtils.deleteDirectory(datasetDir)
    }
  }

  test("does not broadcast a filtered dimension above the broadcast row limit") {
    val datasetDir = Files.createTempDirectory("trusted-row-limit-dataset").toFile
    val customerDir = datasetDir.toPath.resolve("customer")
    val metadataFile = datasetDir.toPath.resolve("trusted-metadata.properties")
    val metadata =
      s"""dataset.path=${datasetDir.getCanonicalPath}
         |table.customer.rowCount=4500000000
         |column.customer.c_mktsegment.distinctCount=5
         |""".stripMargin
    Files.write(metadataFile, metadata.getBytes(StandardCharsets.UTF_8))
    val trustedConf = conf
      .set("spark.sql.autoBroadcastJoinThreshold", "8g")
      .set(GpuOptimizerTrustedMetadata.pathConf, metadataFile.toString)

    try {
      withCpuSparkSession(
        spark => {
          import spark.implicits._

          Seq((1L, "BUILDING"), (2L, "AUTOMOBILE"))
            .toDF("c_custkey", "c_mktsegment")
            .write.parquet(customerDir.toString)
          val customer = spark.read.parquet(customerDir.toString)
            .filter("c_mktsegment = 'BUILDING'")
            .select("c_custkey")
            .queryExecution.analyzed
          val orderCustomerKey = AttributeReference("o_custkey", LongType)()
          val orders = SelectiveDimensionStatRel(Seq(orderCustomerKey), 45000000000L)
          val customerKey = customer.output.find(_.name == "c_custkey").get
          val original = join(customer, orders, EqualTo(customerKey, orderCustomerKey))
          val rewritten = GpuBroadcastSelectiveFilteredDimension(spark)(original)

          assert(rewritten.fastEquals(original), rewritten.treeString)
        },
        trustedConf)
    } finally {
      ApacheFileUtils.deleteDirectory(datasetDir)
    }
  }

  test("pushes an already required selective dimension below a same-fact aggregate") {
    withCpuSparkSession(
      spark => {
        val outerKey = AttributeReference("fact_key", LongType)()
        val outerValue = AttributeReference("fact_value", LongType)()
        val innerKey = AttributeReference("fact_key", LongType)()
        val innerValue = AttributeReference("fact_value", LongType)()
        val dimensionKey = AttributeReference("dimension_key", LongType)()
        val dimensionKind = AttributeReference("dimension_kind", StringType)()
        val outerFact = SelectiveDimensionStatRel(Seq(outerKey, outerValue), 180000000000L)
        val innerFact = SelectiveDimensionStatRel(Seq(innerKey, innerValue), 180000000000L)
        val dimension = Project(
          Seq(dimensionKey),
          Filter(
            EqualTo(dimensionKind, Literal("selected")),
            SelectiveDimensionStatRel(Seq(dimensionKey, dimensionKind), 1000L)))
        val outer = join(outerFact, dimension, EqualTo(outerKey, dimensionKey))
        val average = Alias(Average(innerValue).toAggregateExpression(), "average_value")()
        val aggregate = Aggregate(Seq(innerKey), Seq(innerKey, average), innerFact)
        val original = join(outer, aggregate, EqualTo(dimensionKey, innerKey))
        val rewritten = GpuPushSelectiveDimensionFilterIntoAggregate(spark)(original)

        val rewrittenAggregate = rewritten.collectFirst { case agg: Aggregate => agg }.get
        assert(rewrittenAggregate.child.exists {
          case Join(_, right, LeftSemi, _, hint) =>
            right.fastEquals(dimension) &&
              hint.rightHint.exists(_.strategy.contains(BROADCAST))
          case _ => false
        }, rewritten.treeString)
      },
      conf.set("spark.sql.autoBroadcastJoinThreshold", "12g"))
  }

  test("does not duplicate a dimension above the broadcast row limit") {
    withCpuSparkSession(
      spark => {
        val outerKey = AttributeReference("fact_key", LongType)()
        val outerValue = AttributeReference("fact_value", LongType)()
        val innerKey = AttributeReference("fact_key", LongType)()
        val innerValue = AttributeReference("fact_value", LongType)()
        val dimensionKey = AttributeReference("dimension_key", LongType)()
        val dimensionKind = AttributeReference("dimension_kind", StringType)()
        val outerFact = SelectiveDimensionStatRel(Seq(outerKey, outerValue), 180000000000L)
        val innerFact = SelectiveDimensionStatRel(Seq(innerKey, innerValue), 180000000000L)
        val dimension = Project(
          Seq(dimensionKey),
          Filter(
            EqualTo(dimensionKind, Literal("selected")),
            SelectiveDimensionStatRel(
              Seq(dimensionKey, dimensionKind), 600000000L)))
        val outer = join(outerFact, dimension, EqualTo(outerKey, dimensionKey))
        val average = Alias(Average(innerValue).toAggregateExpression(), "average_value")()
        val aggregate = Aggregate(Seq(innerKey), Seq(innerKey, average), innerFact)
        val original = join(outer, aggregate, EqualTo(dimensionKey, innerKey))
        assert(dimension.collectFirst {
          case leaf: LeafNode => leaf.stats.rowCount.exists(_ >= 512000000L)
        }.contains(true), dimension.treeString)
        assert(dimension.stats.sizeInBytes <= BigInt(12L << 30), dimension.stats.toString)
        val rewritten = GpuPushSelectiveDimensionFilterIntoAggregate(spark)(original)

        assert(rewritten.fastEquals(original), rewritten.treeString)
      },
      conf.set("spark.sql.autoBroadcastJoinThreshold", "12g"))
  }

  test("pushes a large selective keyset to both join inputs with shuffle broadcast") {
    val datasetDir = Files.createTempDirectory("trusted-keyset-fanout-dataset").toFile
    val partDir = datasetDir.toPath.resolve("part")
    val metadataFile = datasetDir.toPath.resolve("trusted-metadata.properties")
    val metadata =
      s"""dataset.path=${datasetDir.getCanonicalPath}
         |table.part.rowCount=6000000000
         |column.part.p_partkey.distinctCount=6000000000
         |""".stripMargin
    Files.write(metadataFile, metadata.getBytes(StandardCharsets.UTF_8))
    val trustedConf = conf
      .set("spark.sql.autoBroadcastJoinThreshold", "8g")
      .set(pushSelectiveKeysetKey, "true")
      .set("spark.rapids.shuffle.broadcast.enabled", "true")
      .set("spark.rapids.shuffle.broadcast.trustSparkPlan.enabled", "true")
      .set("spark.rapids.shuffle.broadcast.maxSize", "12g")
      .set(GpuOptimizerTrustedMetadata.pathConf, metadataFile.toString)

    try {
      withCpuSparkSession(
        spark => {
          import spark.implicits._

          Seq((1L, "forest green"), (2L, "red"))
            .toDF("p_partkey", "p_name")
            .write.parquet(partDir.toString)
          val part = spark.read.parquet(partDir.toString)
            .filter(col("p_name").contains("green"))
            .select("p_partkey")
            .queryExecution.analyzed
          val lPartKey = AttributeReference("l_partkey", LongType)()
          val lSuppKey = AttributeReference("l_suppkey", LongType)()
          val psPartKey = AttributeReference("ps_partkey", LongType)()
          val psSuppKey = AttributeReference("ps_suppkey", LongType)()
          val lineitem = SelectiveDimensionStatRel(
            Seq(lPartKey, lSuppKey), 180000000000L)
          val partsupp = SelectiveDimensionStatRel(
            Seq(psPartKey, psSuppKey), 24000000000L)
          val pPartKey = part.output.find(_.name == "p_partkey").get
          val filteredLineitem = join(lineitem, part, EqualTo(lPartKey, pPartKey))
          val original = join(
            filteredLineitem,
            partsupp,
            And(EqualTo(lPartKey, psPartKey), EqualTo(lSuppKey, psSuppKey)))
          val rewritten = GpuPushSelectiveKeysetToJoinInputs(spark)(original)

          assert(!rewritten.fastEquals(original), rewritten.treeString)
          assert(rewritten.outputSet == original.outputSet, rewritten.treeString)
          assert(rewritten.exists {
            case Join(left, right, LeftSemi, _, hint) =>
              left.fastEquals(partsupp) && right.output.size == 1 &&
                hint.rightHint.exists(_.strategy.contains(BROADCAST))
            case _ => false
          }, rewritten.treeString)
          assert(rewritten.exists {
            case Join(left, right, Inner, _, hint) =>
              left.fastEquals(lineitem) && right.fastEquals(part) &&
                hint.rightHint.exists(_.strategy.contains(BROADCAST))
            case _ => false
          }, rewritten.treeString)
        },
        trustedConf)
    } finally {
      ApacheFileUtils.deleteDirectory(datasetDir)
    }
  }

  test("does not push an oversized keyset through ordinary driver broadcast") {
    val datasetDir = Files.createTempDirectory("trusted-keyset-driver-broadcast-dataset").toFile
    val partDir = datasetDir.toPath.resolve("part")
    val metadataFile = datasetDir.toPath.resolve("trusted-metadata.properties")
    val metadata =
      s"""dataset.path=${datasetDir.getCanonicalPath}
         |table.part.rowCount=6000000000
         |""".stripMargin
    Files.write(metadataFile, metadata.getBytes(StandardCharsets.UTF_8))
    val trustedConf = conf
      .set("spark.sql.autoBroadcastJoinThreshold", "12g")
      .set(pushSelectiveKeysetKey, "true")
      .set("spark.rapids.shuffle.broadcast.enabled", "false")
      .set(GpuOptimizerTrustedMetadata.pathConf, metadataFile.toString)

    try {
      withCpuSparkSession(
        spark => {
          import spark.implicits._

          Seq((1L, "forest green"), (2L, "red"))
            .toDF("p_partkey", "p_name")
            .write.parquet(partDir.toString)
          val part = spark.read.parquet(partDir.toString)
            .filter(col("p_name").contains("green"))
            .select("p_partkey")
            .queryExecution.analyzed
          val lPartKey = AttributeReference("l_partkey", LongType)()
          val psPartKey = AttributeReference("ps_partkey", LongType)()
          val lineitem = SelectiveDimensionStatRel(Seq(lPartKey), 180000000000L)
          val partsupp = SelectiveDimensionStatRel(Seq(psPartKey), 24000000000L)
          val pPartKey = part.output.find(_.name == "p_partkey").get
          val original = join(
            join(lineitem, part, EqualTo(lPartKey, pPartKey)),
            partsupp,
            EqualTo(lPartKey, psPartKey))
          val rewritten = GpuPushSelectiveKeysetToJoinInputs(spark)(original)

          assert(rewritten.fastEquals(original), rewritten.treeString)
        },
        trustedConf)
    } finally {
      ApacheFileUtils.deleteDirectory(datasetDir)
    }
  }

  test("does not infer key equivalence through a full outer join") {
    val datasetDir = Files.createTempDirectory("trusted-keyset-outer-join-dataset").toFile
    val partDir = datasetDir.toPath.resolve("part")
    val metadataFile = datasetDir.toPath.resolve("trusted-metadata.properties")
    val metadata =
      s"""dataset.path=${datasetDir.getCanonicalPath}
         |table.part.rowCount=6000000000
         |column.part.p_partkey.distinctCount=6000000000
         |""".stripMargin
    Files.write(metadataFile, metadata.getBytes(StandardCharsets.UTF_8))
    val trustedConf = conf
      .set("spark.sql.autoBroadcastJoinThreshold", "8g")
      .set(pushSelectiveKeysetKey, "true")
      .set("spark.rapids.shuffle.broadcast.enabled", "true")
      .set("spark.rapids.shuffle.broadcast.trustSparkPlan.enabled", "true")
      .set("spark.rapids.shuffle.broadcast.maxSize", "12g")
      .set(GpuOptimizerTrustedMetadata.pathConf, metadataFile.toString)

    try {
      withCpuSparkSession(
        spark => {
          import spark.implicits._

          Seq((1L, "forest green"), (2L, "red"))
            .toDF("p_partkey", "p_name")
            .write.parquet(partDir.toString)
          val part = spark.read.parquet(partDir.toString)
            .filter(col("p_name").contains("green"))
            .select("p_partkey")
            .queryExecution.analyzed
          val lineitemKey = AttributeReference("l_partkey", LongType)()
          val bridgeKey = AttributeReference("bridge_partkey", LongType)()
          val targetKey = AttributeReference("target_partkey", LongType)()
          val lineitem = SelectiveDimensionStatRel(Seq(lineitemKey), 180000000000L)
          val bridge = SelectiveDimensionStatRel(Seq(bridgeKey), 24000000000L)
          val target = SelectiveDimensionStatRel(Seq(targetKey), 24000000000L)
          val partKey = part.output.find(_.name == "p_partkey").get
          val filteredLineitem = join(lineitem, part, EqualTo(lineitemKey, partKey))
          val outer = Join(
            filteredLineitem,
            bridge,
            FullOuter,
            Some(EqualTo(lineitemKey, bridgeKey)),
            JoinHint.NONE)
          val original = join(outer, target, EqualTo(bridgeKey, targetKey))
          val rewritten = GpuPushSelectiveKeysetToJoinInputs(spark)(original)

          assert(rewritten.fastEquals(original), rewritten.treeString)
        },
        trustedConf)
    } finally {
      ApacheFileUtils.deleteDirectory(datasetDir)
    }
  }

  private case class Q8LikePlan(
      plan: LogicalPlan,
      customer: LogicalPlan,
      nation: LogicalPlan,
      region: LogicalPlan,
      orders: LogicalPlan,
      lineitem: LogicalPlan,
      part: LogicalPlan)

  private def q8LikePlan(
      addCompetingEdge: Boolean,
      partOverride: Option[LogicalPlan] = None): Q8LikePlan = {
    val lOrderKey = AttributeReference("l_orderkey", LongType)()
    val lPartKey = AttributeReference("l_partkey", LongType)()
    val lExtraKey = AttributeReference("l_extra", LongType)()
    val oOrderKey = AttributeReference("o_orderkey", LongType)()
    val oCustKey = AttributeReference("o_custkey", LongType)()
    val oExtraKey = AttributeReference("o_extra", LongType)()
    val cCustKey = AttributeReference("c_custkey", LongType)()
    val cNationKey = AttributeReference("c_nationkey", LongType)()
    val nNationKey = AttributeReference("n_nationkey", LongType)()
    val nRegionKey = AttributeReference("n_regionkey", LongType)()
    val rRegionKey = AttributeReference("r_regionkey", LongType)()
    val rName = AttributeReference("r_name", StringType)()

    val lineitem = SelectiveDimensionStatRel(
      Seq(lOrderKey, lPartKey, lExtraKey), 180000000000L)
    val part = partOverride.getOrElse {
      val pPartKey = AttributeReference("p_partkey", LongType)()
      val pType = AttributeReference("p_type", StringType)()
      Filter(
        EqualTo(pType, Literal("ECONOMY ANODIZED STEEL")),
        SelectiveDimensionStatRel(Seq(pPartKey, pType), 6000000000L))
    }
    val pPartKey = part.output.find(_.name == "p_partkey").get
    val orders = SelectiveDimensionStatRel(
      Seq(oOrderKey, oCustKey, oExtraKey), 45000000000L)
    val customer = SelectiveDimensionStatRel(Seq(cCustKey, cNationKey), 4500000000L)
    val nation = SelectiveDimensionStatRel(Seq(nNationKey, nRegionKey), 25L)
    val region = Filter(
      EqualTo(rName, Literal("AMERICA")),
      SelectiveDimensionStatRel(Seq(rRegionKey, rName), 5L))

    val partCondition = if (addCompetingEdge) {
      And(EqualTo(pPartKey, lPartKey), EqualTo(pPartKey, oExtraKey))
    } else {
      EqualTo(pPartKey, lPartKey)
    }
    val plan = join(
      join(
        join(
          join(join(lineitem, orders, EqualTo(lOrderKey, oOrderKey)), part, partCondition),
          customer,
          EqualTo(oCustKey, cCustKey)),
        nation,
        EqualTo(cNationKey, nNationKey)),
      region,
      EqualTo(nRegionKey, rRegionKey))

    Q8LikePlan(plan, customer, nation, region, orders, lineitem, part)
  }

  private def join(left: LogicalPlan, right: LogicalPlan, condition: Expression): Join =
    Join(left, right, Inner, Some(condition), JoinHint.NONE)

  private def containsJoinedBranches(
      plan: LogicalPlan,
      victim: LogicalPlan,
      chain: LogicalPlan,
      seed: LogicalPlan): Boolean =
    plan.exists {
      case Join(left, right, Inner, _, _) =>
        sameBranch(left, victim) && containsBranch(right, chain) && containsBranch(right, seed)
      case _ => false
    }

  private def containsBranch(plan: LogicalPlan, target: LogicalPlan): Boolean =
    sameBranch(plan, target) || plan.children.exists(containsBranch(_, target))

  private def containsDirectJoin(
      plan: LogicalPlan,
      first: LogicalPlan,
      second: LogicalPlan): Boolean =
    plan.exists {
      case Join(left, right, Inner, _, _) =>
        (containsBranch(left, first) && containsBranch(right, second)) ||
          (containsBranch(left, second) && containsBranch(right, first))
      case _ => false
    }

  private def broadcastsAccumulatedBranch(
      plan: LogicalPlan,
      victim: LogicalPlan,
      firstFact: LogicalPlan,
      secondFact: LogicalPlan): Boolean =
    plan.exists {
      case Join(left, right, Inner, _, hint) =>
        containsBranch(left, victim) && containsBranch(left, firstFact) &&
          containsBranch(right, secondFact) &&
          hint.leftHint.exists(_.strategy.contains(BROADCAST))
      case _ => false
    }

  private def sameBranch(left: LogicalPlan, right: LogicalPlan): Boolean =
    left.fastEquals(right) || left.outputSet == right.outputSet

}

private[rapids] case class SelectiveDimensionStatRel(attrs: Seq[Attribute], rows: Long)
    extends LeafNode {
  override def output: Seq[Attribute] = attrs
  override def computeStats(): Statistics =
    Statistics(sizeInBytes = BigInt(rows) * 16, rowCount = Some(BigInt(rows)))
}
