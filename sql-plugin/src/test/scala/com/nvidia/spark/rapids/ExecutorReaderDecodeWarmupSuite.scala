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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf

class ExecutorReaderDecodeWarmupSuite extends AnyFunSuite {
  test("settings describe bounded full-file Parquet reads") {
    val conf = new SparkConf(false)
      .set(ExecutorReaderDecodeWarmup.URIS_KEY,
        "gs://bucket/a.parquet, gs://bucket/b.parquet")
      .set(ExecutorReaderDecodeWarmup.WORKER_COUNT_KEY, "2")
      .set(ExecutorReaderDecodeWarmup.MAX_FILE_BYTES_KEY, "262144")
      .set(ExecutorReaderDecodeWarmup.TIMEOUT_MS_KEY, "12000")
      .set(ExecutorReaderDecodeWarmup.CANCEL_ON_TASK_START_KEY, "false")
      .set(ExecutorReaderDecodeWarmup.EXPECTED_FS_IMPL_KEY, "example.GcsFileSystem")
      .set(ExecutorReaderDecodeWarmup.WAIT_FOR_GCS_WARMUP_KEY, "false")

    val settings = ExecutorReaderDecodeWarmup.parseSettings(conf)

    assert(settings.uris === Seq("gs://bucket/a.parquet", "gs://bucket/b.parquet"))
    assert(settings.workerCount === 2)
    assert(settings.maxFileBytes === 262144)
    assert(settings.timeoutMs === 12000L)
    assert(!settings.cancelOnTaskStart)
    assert(settings.expectedFsImpl === "example.GcsFileSystem")
    assert(!settings.waitForGcsReadWarmup)
  }

  test("URI selection is deterministic and wraps without duplicates") {
    val uris = (0 until 8).map(index => s"gs://bucket/$index.parquet")

    val selected = ExecutorReaderDecodeWarmup.selectUris(uris, 4, "2")
    val repeated = ExecutorReaderDecodeWarmup.selectUris(uris, 4, "2")

    assert(selected === repeated)
    assert(selected.map(_._1).distinct.size === 4)
    assert(selected.forall { case (uri, index) => uri === uris(index) })
  }

  test("worker count cannot exceed the URI count") {
    val conf = new SparkConf(false)
      .set(ExecutorReaderDecodeWarmup.URIS_KEY, "gs://bucket/a.parquet")
      .set(ExecutorReaderDecodeWarmup.WORKER_COUNT_KEY, "2")

    val error = intercept[IllegalArgumentException] {
      ExecutorReaderDecodeWarmup.parseSettings(conf)
    }
    assert(error.getMessage.contains(ExecutorReaderDecodeWarmup.WORKER_COUNT_KEY))
  }
}
