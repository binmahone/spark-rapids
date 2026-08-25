/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.filecache

import com.nvidia.spark.rapids.{GpuMetric, RapidsConf, SparkQueryCompareTestSuite}
import com.nvidia.spark.rapids.shims.GpuBatchScanExec

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.rapids.GpuFileSourceScanExec

class NoFileCacheIntegrationSuite extends SparkQueryCompareTestSuite {
  private val FILE_SPLITS_PARQUET = "file-splits.parquet"
  private val SCHEMA_CAN_PRUNE_ORC = "schema-can-prune.orc"

  def isFileCacheEnabled(conf: SparkConf): Boolean = {
    conf.getBoolean("spark.rapids.filecache.enabled", false)
  }

  test("no filecache no metrics v1 Parquet") {
    val conf = new SparkConf(false)
        .set("spark.rapids.filecache.enabled", "false")
        .set("spark.sql.sources.useV1SourceList", "parquet")
    withGpuSparkSession({ spark =>
      assume(!isFileCacheEnabled(spark.sparkContext.conf))
      val df = frameFromParquet(FILE_SPLITS_PARQUET)(spark)
      checkNoMetricsV1(df)
    }, conf)
  }

  test("v1 multithreaded Parquet reader reports buffer subphase metrics") {
    val conf = new SparkConf(false)
        .set("spark.rapids.filecache.enabled", "false")
        .set("spark.sql.sources.useV1SourceList", "parquet")
        .set(RapidsConf.PARQUET_READER_TYPE.key, "MULTITHREADED")
        .set(RapidsConf.METRICS_LEVEL.key, "DEBUG")
    withGpuSparkSession({ spark =>
      val df = frameFromParquet(FILE_SPLITS_PARQUET)(spark)
      df.collect()
      val gpuScan = df.queryExecution.executedPlan.find(_.isInstanceOf[GpuFileSourceScanExec])
      assert(gpuScan.isDefined)
      val metrics = gpuScan.get.metrics
      Seq(
        GpuMetric.PARQUET_OUTPUT_SIZE_TIME,
        GpuMetric.PARQUET_HOST_BUFFER_ALLOC_TIME,
        GpuMetric.PARQUET_RANGE_PREP_TIME,
        GpuMetric.PARQUET_REMOTE_CACHE_TIME,
        GpuMetric.PARQUET_BLOCK_METADATA_TIME,
        GpuMetric.PARQUET_BLOCK_COPY_TIME,
        GpuMetric.PARQUET_FOOTER_WRITE_TIME,
        GpuMetric.PARQUET_SPILLABLE_WRAP_TIME).foreach { metricName =>
        assert(metrics.contains(metricName), s"missing Parquet reader metric $metricName")
        assert(metrics(metricName).value > 0, s"Parquet reader metric $metricName was not updated")
      }
    }, conf)
  }

  test("no filecache no metrics v1 ORC") {
    val conf = new SparkConf(false)
        .set("spark.rapids.filecache.enabled", "false")
        .set("spark.sql.sources.useV1SourceList", "orc")
    withGpuSparkSession({ spark =>
      assume(!isFileCacheEnabled(spark.sparkContext.conf))
      val df = frameFromOrc(SCHEMA_CAN_PRUNE_ORC)(spark)
      checkNoMetricsV1(df)
    }, conf)
  }

  test("no filecache no metrics v2 Parquet") {
    val conf = new SparkConf(false)
        .set("spark.rapids.filecache.enabled", "false")
        .set("spark.sql.sources.useV1SourceList", "")
    withGpuSparkSession({ spark =>
      assume(!isFileCacheEnabled(spark.sparkContext.conf))
      val df = frameFromParquet(FILE_SPLITS_PARQUET)(spark)
      checkNoMetricsV2(df)
    }, conf)
  }

  test("no filecache no metrics v2 ORC") {
    val conf = new SparkConf(false)
        .set("spark.rapids.filecache.enabled", "false")
        .set("spark.sql.sources.useV1SourceList", "")
    withGpuSparkSession({ spark =>
      assume(!isFileCacheEnabled(spark.sparkContext.conf))
      val df = frameFromOrc(SCHEMA_CAN_PRUNE_ORC)(spark)
      checkNoMetricsV2(df)
    }, conf)
  }

  private def checkNoMetricsV1(df: DataFrame): Unit = {
    df.collect()
    val gpuScan = df.queryExecution.executedPlan.find(_.isInstanceOf[GpuFileSourceScanExec])
    assert(gpuScan.isDefined)
    assert(!gpuScan.get.metrics.keys.exists(_.startsWith("filecache")))
  }

  private def checkNoMetricsV2(df: DataFrame): Unit = {
    df.collect()
    val gpuScan = df.queryExecution.executedPlan.find(_.isInstanceOf[GpuBatchScanExec])
    assert(gpuScan.isDefined)
    assert(!gpuScan.get.metrics.keys.exists(_.startsWith("filecache")))
  }
}
