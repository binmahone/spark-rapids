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

/*** spark-rapids-shim-json-lines
{"spark": "353"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids

import java.nio.file.Files
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

class ColdStartQueryPlanningListenerSuite extends AnyFunSuite {
  test("listener records planning phases and input file indexes") {
    val root = Files.createTempDirectory("cold-start-query-planning")
    val input = root.resolve("input").toUri.toString
    val output = root.resolve("output").toUri.toString
    val normalizedOutput = new Path(output).toString
    val observed = new ConcurrentLinkedQueue[String]()
    val outputMetrics = new CountDownLatch(2)
    var spark: SparkSession = null

    ColdStartQueryPlanningListener.setMetricObserver { metric =>
      observed.add(metric)
      if (metric.contains(s"output_path=$normalizedOutput")) {
        outputMetrics.countDown()
      }
    }

    try {
      spark = SparkSession.builder()
        .master("local[1]")
        .appName("cold-start-query-planning-listener-suite")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
      spark.listenerManager.register(new ColdStartQueryPlanningListener)
      spark.range(4).write.parquet(input)

      val inputFrame = spark.read.parquet(input)
      inputFrame.write.parquet(output)

      assert(outputMetrics.await(10, TimeUnit.SECONDS))
      val metrics = observed.asScala.filter(_.contains(s"output_path=$normalizedOutput"))
      assert(metrics.exists(_.startsWith("RAPIDS_QUERY_PLANNING_METRIC")))
      assert(metrics.exists(_.startsWith("RAPIDS_FILE_INDEX_METRIC")))
      assert(metrics.exists(_.contains("phase_analysis_ms=")))
      assert(metrics.exists(_.contains("input_plan_identity_hash=")))
      assert(metrics.exists(_.contains("metadata_ops_time_ns=")))
    } finally {
      ColdStartQueryPlanningListener.resetMetricObserver()
      if (spark != null) {
        spark.stop()
      }
      Utils.deleteRecursively(root.toFile)
    }
  }
}
