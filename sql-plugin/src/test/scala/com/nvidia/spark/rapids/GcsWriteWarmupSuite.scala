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

import java.net.URI
import java.nio.file.Files
import java.util.concurrent.CountDownLatch

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.RawLocalFileSystem
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf

class TestGcsWriteFileSystem extends RawLocalFileSystem {
  override def getScheme: String = "gs"
  override def getUri: URI = URI.create("gs://test-bucket")
  override def initialize(name: URI, conf: Configuration): Unit = setConf(conf)
}

class GcsWriteWarmupSuite extends AnyFunSuite {
  test("disabled warm-up does not evaluate the Hadoop configuration supplier") {
    var supplierCalled = false
    val handle = GcsWriteWarmup.startAsync(
      new SparkConf(false),
      () => {
        supplierCalled = true
        new Configuration(false)
      },
      "1")

    assert(handle.isEmpty)
    assert(!supplierCalled)
  }

  test("settings require a bounded GCS output root") {
    val conf = new SparkConf(false)
      .set(GcsWriteWarmup.ROOT_URI_KEY, "gs://bucket/warmup")
      .set(GcsWriteWarmup.BYTES_KEY, "17")
      .set(GcsWriteWarmup.TIMEOUT_MS_KEY, "9000")
      .set(GcsWriteWarmup.CANCEL_ON_TASK_START_KEY, "true")
      .set(GcsWriteWarmup.EXPECTED_FS_IMPL_KEY, "example.GcsFileSystem")

    val settings = GcsWriteWarmup.parseSettings(conf)

    assert(settings.rootUri === "gs://bucket/warmup")
    assert(settings.byteCount === 17)
    assert(settings.timeoutMs === 9000L)
    assert(settings.cancelOnTaskStart)
    assert(settings.expectedFsImpl === "example.GcsFileSystem")
  }

  test("run creates closes and deletes the executor-specific object") {
    val directory = Files.createTempDirectory("gcs-write-warmup")
    val rootUri = s"gs://test-bucket${directory.toAbsolutePath}"
    val sparkConf = new SparkConf(false)
      .set(GcsWriteWarmup.ROOT_URI_KEY, rootUri)
      .set(GcsWriteWarmup.BYTES_KEY, "23")
      .set(GcsWriteWarmup.EXPECTED_FS_IMPL_KEY, classOf[TestGcsWriteFileSystem].getName)
      .set("spark.hadoop.fs.gs.impl", classOf[TestGcsWriteFileSystem].getName)
      .set("spark.hadoop.fs.gs.impl.disable.cache", "true")

    val result = GcsWriteWarmup.run(sparkConf, new Configuration(false), "exec 2")

    assert(result.bytesWritten === 23)
    assert(result.deleted)
    assert(result.fsImpl === classOf[TestGcsWriteFileSystem].getName)
    assert(!Files.exists(directory.resolve("executor-exec_2.bin")))
    Files.delete(directory)
  }

  test("cancellation is idempotent and does not wait for the worker") {
    val supplierEntered = new CountDownLatch(1)
    val releaseSupplier = new CountDownLatch(1)
    val conf = new SparkConf(false)
      .set(GcsWriteWarmup.ENABLED_KEY, "true")
      .set(GcsWriteWarmup.ROOT_URI_KEY, "gs://bucket/warmup")
      .set(GcsWriteWarmup.TIMEOUT_MS_KEY, "30000")
    val handle = GcsWriteWarmup.startAsync(conf, () => {
      supplierEntered.countDown()
      releaseSupplier.await()
      new Configuration(false)
    }, "3").get
    assert(supplierEntered.await(5, java.util.concurrent.TimeUnit.SECONDS))

    assert(handle.cancel("test"))
    assert(!handle.cancel("test_again"))
    releaseSupplier.countDown()
    assert(handle.await(5000))
  }
}
