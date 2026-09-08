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
import org.apache.spark.sql.catalyst.plans.logical.Aggregate

class GpuDeduplicateLargeLeftAntiBuildSideSuite extends SparkQueryCompareTestSuite {

  test("deduplicate a repeated left-anti existence key") {
    val datasetDir = Files.createTempDirectory("left-anti-deduplicate-dataset").toFile
    val metadataFile = datasetDir.toPath.resolve("trusted-metadata.properties")
    val metadata =
      s"""dataset.path=${datasetDir.getCanonicalPath}
         |table.orders.rowCount=6
         |column.orders.o_custkey.distinctCount=2
         |""".stripMargin
    Files.write(metadataFile, metadata.getBytes(StandardCharsets.UTF_8))

    try {
      withCpuSparkSession(spark => {
        import spark.implicits._

        Seq(1L, 1L, 1L, 2L, 2L, 2L)
          .toDF("o_custkey")
          .write.parquet(datasetDir.toPath.resolve("orders").toString)
        spark.read.parquet(datasetDir.toPath.resolve("orders").toString)
          .createOrReplaceTempView("orders")
        Seq(1L, 2L, 3L).toDF("c_custkey").createOrReplaceTempView("customer")
        val sql =
          """SELECT c_custkey
            |FROM customer LEFT ANTI JOIN orders
            |ON c_custkey = o_custkey
            |""".stripMargin

        spark.conf.set(GpuOptimizerTrustedMetadata.pathConf, "")
        val expected = spark.sql(sql).collect().toSeq
        spark.conf.set(GpuOptimizerTrustedMetadata.pathConf, metadataFile.toString)
        val query = spark.sql(sql)
        val optimized = query.queryExecution.optimizedPlan

        assert(query.collect().toSeq === expected)
        assert(optimized.collect { case aggregate: Aggregate => aggregate }.nonEmpty,
          optimized.treeString)
      }, new SparkConf())
    } finally {
      ApacheFileUtils.deleteDirectory(datasetDir)
    }
  }
}
