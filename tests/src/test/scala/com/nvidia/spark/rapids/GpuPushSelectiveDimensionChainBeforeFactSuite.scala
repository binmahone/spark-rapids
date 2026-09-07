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
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, EqualTo}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, Filter, Join, JoinHint, LeafNode}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.types.{LongType, StringType}

class GpuPushSelectiveDimensionChainBeforeFactSuite extends SparkQueryCompareTestSuite {

  private val enabledKey =
    "spark.rapids.sql.optimizer.pushDimensionChainBeforeFact.enabled"
  private val maxChainRowsKey =
    "spark.rapids.sql.optimizer.pushDimensionChainBeforeFact.maxChainRows"
  private val maxChainBytesKey =
    "spark.rapids.sql.optimizer.pushDimensionChainBeforeFact.maxChainBytes"

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

    val lineitem = StatRel(Seq(lOrderKey, lPartKey, lExtraKey), 180000000000L)
    val part = partOverride.getOrElse {
      val pPartKey = AttributeReference("p_partkey", LongType)()
      val pType = AttributeReference("p_type", StringType)()
      Filter(
        EqualTo(pType, Literal("ECONOMY ANODIZED STEEL")),
        StatRel(Seq(pPartKey, pType), 6000000000L))
    }
    val pPartKey = part.output.find(_.name == "p_partkey").get
    val orders = StatRel(Seq(oOrderKey, oCustKey, oExtraKey), 45000000000L)
    val customer = StatRel(Seq(cCustKey, cNationKey), 4500000000L)
    val nation = StatRel(Seq(nNationKey, nRegionKey), 25L)
    val region = Filter(
      EqualTo(rName, Literal("AMERICA")),
      StatRel(Seq(rRegionKey, rName), 5L))

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

  private case class StatRel(attrs: Seq[Attribute], rows: Long) extends LeafNode {
    override def output: Seq[Attribute] = attrs
    override def computeStats(): Statistics =
      Statistics(sizeInBytes = BigInt(rows) * 16, rowCount = Some(BigInt(rows)))
  }
}
