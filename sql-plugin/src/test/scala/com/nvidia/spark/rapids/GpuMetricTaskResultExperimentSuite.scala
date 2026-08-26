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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.mutable.ArrayBuffer

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession

class GpuMetricTaskResultExperimentSuite extends AnyFunSuite with BeforeAndAfterAll {
  import GpuMetric._

  private case class JobResult(
      totalResultBytes: Long,
      taskCount: Int,
      totalOpTimeAccumulatorUpdates: Int,
      indirectTaskCount: Int)

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[4]")
      .appName(getClass.getSimpleName)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.maxResultSize", "10m")
      .config("spark.rpc.message.maxSize", "1")
      .getOrCreate()
    SparkSession.setActiveSession(spark)
  }

  override def afterAll(): Unit = {
    try {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      spark.stop()
    } finally {
      super.afterAll()
    }
  }

  private def runJob(
      metricCount: Int,
      partitions: Int,
      withCompanions: Boolean): JobResult = {
    val factory = new GpuMetricFactory(MODERATE_LEVEL, spark.sparkContext)
    val metrics = (0 until metricCount).map { index =>
      val metric = if (withCompanions) {
        factory.createOpTime(DESCRIPTION_OP_TIME_NEW)
      } else {
        factory.createNanoTiming(MODERATE_LEVEL, DESCRIPTION_OP_TIME_NEW)
      }
      s"opTime$index" -> metric
    }.toMap
    val accumulators = unwrap(metrics).values.toArray

    val resultSizes = new ArrayBuffer[Long]()
    val accumulatorCounts = new ArrayBuffer[Int]()
    val indirectResults = new ArrayBuffer[Boolean]()
    val latch = new CountDownLatch(partitions)
    val listenerLock = new Object()
    val listener = new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = listenerLock.synchronized {
        resultSizes += taskEnd.taskMetrics.resultSize
        accumulatorCounts += taskEnd.taskInfo.accumulables.count { update =>
          update.name.exists(_.startsWith(DESCRIPTION_OP_TIME_NEW))
        }
        indirectResults += taskEnd.taskInfo.gettingResultTime > 0
        latch.countDown()
      }
    }

    spark.sparkContext.addSparkListener(listener)
    try {
      spark.sparkContext.parallelize(0 until partitions, partitions).mapPartitions { _ =>
        accumulators.foreach(_.add(1L))
        Iterator.single(1)
      }.collect()
      assert(latch.await(60, TimeUnit.SECONDS),
        s"Timed out waiting for $partitions task-end events")
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }

    listenerLock.synchronized {
      JobResult(
        resultSizes.sum,
        resultSizes.size,
        accumulatorCounts.sum,
        indirectResults.count(identity))
    }
  }

  test("measure end-to-end task-result growth") {
    val partitions = 200
    Seq(10, 50, 100).foreach { metricCount =>
      val raw = runJob(metricCount, partitions, withCompanions = false)
      val companion = runJob(metricCount, partitions, withCompanions = true)
      val delta = companion.totalResultBytes - raw.totalResultBytes

      assert(raw.taskCount == partitions)
      assert(companion.taskCount == partitions)
      assert(raw.totalOpTimeAccumulatorUpdates == metricCount * partitions)
      assert(companion.totalOpTimeAccumulatorUpdates == 2 * metricCount * partitions)
      assert(delta > 0)
      info(s"RESULT metricCount=$metricCount partitions=$partitions " +
        s"rawBytes=${raw.totalResultBytes} companionBytes=${companion.totalResultBytes} " +
        s"deltaBytes=$delta " +
        s"bytesPerCompanionPerTask=${delta.toDouble / metricCount / partitions} " +
        s"rawAccumulatorUpdates=${raw.totalOpTimeAccumulatorUpdates} " +
        s"companionAccumulatorUpdates=${companion.totalOpTimeAccumulatorUpdates}")
    }
  }

  test("measure the RPC direct-result boundary") {
    val metricCount = 10000
    val raw = runJob(metricCount, partitions = 1, withCompanions = false)
    val companion = runJob(metricCount, partitions = 1, withCompanions = true)

    assert(raw.totalOpTimeAccumulatorUpdates == metricCount)
    assert(companion.totalOpTimeAccumulatorUpdates == 2 * metricCount)
    assert(raw.indirectTaskCount == 0)
    assert(companion.indirectTaskCount == 1)
    info(s"RPC metricCount=$metricCount rawBytes=${raw.totalResultBytes} " +
      s"companionBytes=${companion.totalResultBytes} " +
      s"rawIndirectTasks=${raw.indirectTaskCount} " +
      s"companionIndirectTasks=${companion.indirectTaskCount}")
  }

  test("measure aggregate direct results above maxResultSize") {
    val metricCount = 100
    val partitions = 1000
    val configuredMaxResultBytes =
      spark.sparkContext.getConf.getSizeAsBytes("spark.driver.maxResultSize")
    val companion = runJob(metricCount, partitions, withCompanions = true)

    assert(companion.taskCount == partitions)
    assert(companion.totalOpTimeAccumulatorUpdates == 2 * metricCount * partitions)
    assert(companion.indirectTaskCount == 0)
    assert(companion.totalResultBytes > configuredMaxResultBytes)
    info(s"MAX_RESULT metricCount=$metricCount partitions=$partitions " +
      s"taskResultBytes=${companion.totalResultBytes} " +
      s"configuredMaxResultBytes=$configuredMaxResultBytes " +
      s"indirectTasks=${companion.indirectTaskCount}")
  }
}
