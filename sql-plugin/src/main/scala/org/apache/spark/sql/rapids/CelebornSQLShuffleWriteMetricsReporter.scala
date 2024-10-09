/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
package org.apache.spark.sql.rapids

import org.apache.spark.SparkContext
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleWriteMetricsReporter}


// Just to expose metrics as a Java field
class CelebornSQLShuffleWriteMetricsReporter(
    metricsReporter: ShuffleWriteMetricsReporter,
    val metrics: Map[String, SQLMetric])
  extends SQLShuffleWriteMetricsReporter(metricsReporter, metrics)

object CelebornSQLShuffleWriteMetricsReporter {
  val SHUFFLE_WRITE_COMPRESS_TIME = "shuffleWriteCompressTime"
  val SHUFFLE_WRITE_CONGESTION_TIME = "shuffleWriteCongestionTime"
  val SHUFFLE_WRITE_CLOSE_TIME = "shuffleWriteCloseTime"

  /**
   * Create more shuffle write relative metrics and return the Map.
   */
  def createShuffleWriteMetrics(sc: SparkContext): Map[String, SQLMetric] = Map(
    SHUFFLE_WRITE_COMPRESS_TIME ->
      SQLMetrics.createNanoTimingMetric(sc, "rss. shuffle write compress time"),
    SHUFFLE_WRITE_CONGESTION_TIME ->
      SQLMetrics.createNanoTimingMetric(sc, "rss. shuffle write congestion time"),
    SHUFFLE_WRITE_CLOSE_TIME ->
      SQLMetrics.createNanoTimingMetric(sc, "rss. shuffle write close time"))
}