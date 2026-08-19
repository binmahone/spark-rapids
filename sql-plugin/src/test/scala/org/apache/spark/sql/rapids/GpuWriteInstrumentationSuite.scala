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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.{GpuMetric, LocalGpuMetric}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class GpuWriteInstrumentationSuite extends AnyFunSuite {
  test("output phase metrics are independently accumulated") {
    val metricKeys = Seq(
      GpuWriteJobStatsTracker.INPUT_ITERATOR_TIME_KEY,
      GpuWriteJobStatsTracker.WRITER_INIT_TIME_KEY,
      GpuWriteJobStatsTracker.WRITER_CLOSE_TIME_KEY,
      GpuWriteJobStatsTracker.TABLE_WRITER_CLOSE_TIME_KEY,
      GpuWriteJobStatsTracker.CLOSE_BUFFERED_WRITE_TIME_KEY,
      GpuWriteJobStatsTracker.OUTPUT_STREAM_CLOSE_TIME_KEY,
      GpuWriteJobStatsTracker.STATS_CLOSE_FILE_TIME_KEY)
    val metrics: Map[String, GpuMetric] = metricKeys
      .map(_ -> new LocalGpuMetric())
      .toMap
    val tracker = new GpuWriteTaskStatsTracker(new Configuration(), metrics)

    tracker.addInputIteratorTime(1L)
    tracker.addWriterInitTime(2L)
    tracker.addWriterCloseTime(3L)
    tracker.addTableWriterCloseTime(4L)
    tracker.addCloseBufferedWriteTime(5L)
    tracker.addOutputStreamCloseTime(6L)
    tracker.addStatsCloseFileTime(7L)

    assert(metricKeys.map(metrics(_).value) === Seq(1L, 2L, 3L, 4L, 5L, 6L, 7L))
  }
}
