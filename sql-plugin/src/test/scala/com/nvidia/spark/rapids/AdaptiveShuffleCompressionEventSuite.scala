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

import scala.collection.mutable.ArrayBuffer

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkConf, SparkContext, SparkListenerTestUtils}
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}

class AdaptiveShuffleCompressionEventSuite extends AnyFunSuite {
  test("driver posts executor adaptive compression statistics to the listener bus") {
    val conf = new SparkConf(false)
      .setMaster("local[1]")
      .setAppName("adaptive-shuffle-compression-event-suite")
      .set("spark.ui.enabled", "false")
      .set("spark.driver.host", "127.0.0.1")
    val sc = new SparkContext(conf)
    val manager = new ShuffleCleanupManager(
      sc,
      staleEntryMaxAgeMs = Long.MaxValue,
      cleanupIntervalMs = Long.MaxValue)
    val events = new ArrayBuffer[SparkRapidsAdaptiveShuffleCompressionEvent]
    sc.addSparkListener(new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
        case adaptive: SparkRapidsAdaptiveShuffleCompressionEvent =>
          events.synchronized {
            events += adaptive
          }
        case _ =>
      }
    })

    try {
      manager.handleStats(
        "executor-7",
        Array.empty,
        Array(AdaptiveShuffleCompressionStats(
          shuffleId = 11,
          gpuProposedTaskAttempts = 9,
          gpuSelectedTaskAttempts = 3,
          gpuReservationDeniedTaskAttempts = 6,
          cpuSelectedTaskAttempts = 14,
          gpuRawBytes = 900,
          gpuCompressedBytes = 300,
          gpuCompressionTimeNs = 90,
          gpuReservationTimeNs = 120,
          cpuRawBytes = 1400,
          cpuCompressedBytes = 700,
          cpuCompressionTimeNs = 140)))
      SparkListenerTestUtils.waitUntilEmpty(sc)

      val captured = events.synchronized {
        events.toSeq
      }
      assertResult(Seq(SparkRapidsAdaptiveShuffleCompressionEvent(
        shuffleId = 11,
        executorId = "executor-7",
        gpuProposedTaskAttempts = 9,
        gpuSelectedTaskAttempts = 3,
        gpuReservationDeniedTaskAttempts = 6,
        cpuSelectedTaskAttempts = 14,
        gpuRawBytes = 900,
        gpuCompressedBytes = 300,
        gpuCompressionTimeNs = 90,
        gpuReservationTimeNs = 120,
        cpuRawBytes = 1400,
        cpuCompressedBytes = 700,
        cpuCompressionTimeNs = 140)))(captured)
    } finally {
      manager.shutdown()
      sc.stop()
    }
  }
}
