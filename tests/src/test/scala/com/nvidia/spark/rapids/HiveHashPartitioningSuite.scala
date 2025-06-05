/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback._

/**
 * This suite is a supplement to the integration tests for hash partitioning things.
 * HiveHash partitioning supports array of structs, but Murmur3 partitioning doesn't.
 * So this is to cover this case.
 *
 * Besides, HiveHash partitioning is only supported by some customized Spark
 * distributions, so this suite will be executed only when HiveHash is really used
 * for partitioning.
 */
class HiveHashPartitioningSuite extends SparkQueryCompareTestSuite with Logging {

  private def withCapturedPlan[R](f: SparkPlan => R): R = {
    val plans = getResultsWithTimeout()
    assert(plans.length == 1, s"Expected one plan, but got: ${plans.mkString("\n")}")
    val plan = extractExecutedPlan(plans.head)
    f(plan)
  }

  private def findFirstHashPartitioning(plan: SparkPlan): Option[HashPartitioning] = {
    plan match {
      case shuffleStage: ShuffleQueryStageExec =>
        findFirstHashPartitioning(shuffleStage.plan)
      case p =>
        val selfIs = p.isInstanceOf[ShuffleExchangeExec] &&
          p.outputPartitioning.isInstanceOf[HashPartitioning]
        (if (selfIs) {
          Some(p)
        } else {
          p.children.find(findFirstHashPartitioning(_).isDefined)
        }).map(_.outputPartitioning.asInstanceOf[HashPartitioning])
    }
  }

  /** This only cover some simple cases */
  private def genDF(spark: SparkSession): DataFrame = {
    // No null rows because "struct(a,b)" doesn't allow nulls.
    val nullsData: Seq[(Integer, String)] = Seq(
      (null, null), (null, "s2"), (2, null)
    )
    val primitiveData = (0 until 1000).map(i => (Integer.valueOf(i), s"s$i")) ++ nullsData
    val df = spark.createDataFrame(primitiveData).toDF("a", "b")
    df.selectExpr("struct(a, b) as s1", "a")
      .selectExpr("array(s1, s1) as as", "a")
      .repartition(4) // Avoid access to the local scan directly.
  }

  private def maybeHivePartitioningJob(input: DataFrame): DataFrame = {
    // group by the array of structs column, which will involve hash partitioning
    // with Hive on the customized Spark.
    input.selectExpr("as", "a").groupBy(col("as")).max("a")
  }

  test("partitioning with HiveHash on array of structs") {
    val sparkConf = new SparkConf()
      .set(RapidsConf.INCOMPATIBLE_OPS.key, "true")
      .set(RapidsConf.ENABLE_HASH_FUNCTION_IN_PARTITIONING.key, "true")
      .set("spark.sql.hashMode", "HIVE")

    // Infer the hash mode
    val hashMode = withCpuSparkSession(spark => {
      val df = maybeHivePartitioningJob(genDF(spark))
      startCapture()
      df.collect()
      withCapturedPlan { plan =>
        val hp = findFirstHashPartitioning(plan)
        assert(hp.isDefined)
        val rapidsConf = new RapidsConf(spark.sparkContext.getConf)
        GpuHashPartitioningBase.hashModeFromCpu(hp.get, rapidsConf)
      }
    }, sparkConf)

    logInfo(s"Got mode $hashMode for partitioning.")
    // run this test actually iff hive partitioning, since GPU murmur3 does not support
    // array of structs.
    if (hashMode == HiveMode) {
      INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual(
        testName = "partitioning with HiveHash on array of structs",
        df = genDF,
        conf = sparkConf
      )(maybeHivePartitioningJob)
    }
  }
}
