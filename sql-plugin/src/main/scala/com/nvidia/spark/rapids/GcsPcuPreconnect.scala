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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
 * Runs one fail-closed, run-scoped filesystem write during executor initialization.
 *
 * This diagnostic is disabled unless explicitly configured. Its purpose is to test whether one
 * small write on every executor can initialize the same GCS connector transport used by later
 * PCU component uploads before workload tasks begin.
 */
private[rapids] object GcsPcuPreconnect extends Logging {
  val ENABLED_KEY = "spark.rapids.gcs.pcu.preconnect.enabled"
  val ROOT_KEY = "spark.rapids.gcs.pcu.preconnect.root"
  val BYTES_KEY = "spark.rapids.gcs.pcu.preconnect.bytes"

  private val DefaultBytes = 1
  private val MaxBytes = 1024 * 1024

  def run(sparkConf: SparkConf, hadoopConf: Configuration, executorId: String): Unit = {
    if (sparkConf.getBoolean(ENABLED_KEY, false)) {
      val root = sparkConf.getOption(ROOT_KEY).map(_.trim).filter(_.nonEmpty).getOrElse {
        throw new IllegalArgumentException(s"$ROOT_KEY must be set when $ENABLED_KEY=true")
      }
      val byteCount = sparkConf.getInt(BYTES_KEY, DefaultBytes)
      require(byteCount > 0 && byteCount <= MaxBytes,
        s"$BYTES_KEY must be within [1, $MaxBytes], observed $byteCount")

      val safeExecutorId = executorId.replaceAll("[^A-Za-z0-9_.-]", "_")
      val path = new Path(root.stripSuffix("/"), s"executor-$safeExecutorId/preconnect.bin")
      val totalStart = System.nanoTime()
      val fsStart = System.nanoTime()
      val fs = path.getFileSystem(hadoopConf)
      val fsMs = elapsedMs(fsStart)
      val createStart = System.nanoTime()
      val out = fs.create(path, false)
      val createMs = elapsedMs(createStart)
      var writeMs = 0L
      var closeMs = 0L
      try {
        val writeStart = System.nanoTime()
        out.write(new Array[Byte](byteCount))
        writeMs = elapsedMs(writeStart)
      } finally {
        val closeStart = System.nanoTime()
        out.close()
        closeMs = elapsedMs(closeStart)
      }
      val totalMs = elapsedMs(totalStart)
      logInfo(s"RAPIDS_GCS_PCU_PRECONNECT_METRIC executor_id=$executorId bytes=$byteCount " +
        s"fs_ms=$fsMs create_ms=$createMs write_ms=$writeMs close_ms=$closeMs " +
        s"total_ms=$totalMs path=$path success=true")
    }
  }

  private def elapsedMs(startNanos: Long): Long = {
    (System.nanoTime() - startNanos) / 1000000L
  }
}
