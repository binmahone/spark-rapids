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

import java.util.concurrent.CountDownLatch

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf

class GcsReadWarmupSuite extends AnyFunSuite {
  test("disabled warm-up does not evaluate the Hadoop configuration supplier") {
    var supplierCalled = false
    val handle = GcsReadWarmup.startAsync(
      new SparkConf(false),
      () => {
        supplierCalled = true
        new Configuration(false)
      },
      "1")

    assert(handle.isEmpty)
    assert(!supplierCalled)
  }

  test("enabled warm-up validates required immutable object URIs asynchronously") {
    val conf = new SparkConf(false)
      .set(GcsReadWarmup.ENABLED_KEY, "true")
    val handle = GcsReadWarmup.startAsync(
      conf, () => new Configuration(false), "2").get

    assert(handle.await(5000))
  }

  test("settings select the configured read geometry") {
    val conf = new SparkConf(false)
      .set(GcsReadWarmup.URIS_KEY, "gs://bucket/a, gs://bucket/b")
      .set(GcsReadWarmup.BYTES_KEY, "131072")
      .set(GcsReadWarmup.OFFSET_KEY, "4096")
      .set(GcsReadWarmup.TIMEOUT_MS_KEY, "4000")
      .set(GcsReadWarmup.CANCEL_ON_TASK_START_KEY, "false")
      .set(GcsReadWarmup.EXPECTED_FS_IMPL_KEY, "example.GcsFileSystem")

    val settings = GcsReadWarmup.parseSettings(conf)

    assert(settings.uris === Seq("gs://bucket/a", "gs://bucket/b"))
    assert(settings.byteCount === 131072)
    assert(settings.offset === 4096L)
    assert(settings.timeoutMs === 4000L)
    assert(!settings.cancelOnTaskStart)
    assert(settings.expectedFsImpl === "example.GcsFileSystem")
  }

  test("Spark Hadoop properties override the base Hadoop configuration") {
    val base = new Configuration(false)
    base.set("base.only", "retained")
    base.set("fs.gs.impl", "base.GcsFileSystem")
    val sparkConf = new SparkConf(false)
      .set("spark.hadoop.fs.gs.impl", "configured.GcsFileSystem")

    val effective = GcsReadWarmup.buildEffectiveHadoopConf(sparkConf, base)

    assert(effective.get("base.only") === "retained")
    assert(effective.get("fs.gs.impl") === "configured.GcsFileSystem")
    assert(base.get("fs.gs.impl") === "base.GcsFileSystem")
  }

  test("cancellation is idempotent and does not wait for the worker") {
    val supplierEntered = new CountDownLatch(1)
    val releaseSupplier = new CountDownLatch(1)
    val conf = new SparkConf(false)
      .set(GcsReadWarmup.ENABLED_KEY, "true")
      .set(GcsReadWarmup.URIS_KEY, "gs://bucket/object")
      .set(GcsReadWarmup.TIMEOUT_MS_KEY, "30000")
    val handle = GcsReadWarmup.startAsync(conf, () => {
      supplierEntered.countDown()
      releaseSupplier.await()
      new Configuration(false)
    }, "3").get
    assert(supplierEntered.await(5, java.util.concurrent.TimeUnit.SECONDS))

    val startNanos = System.nanoTime()
    assert(handle.cancel("test"))
    val cancelMs = (System.nanoTime() - startNanos) / 1000000L
    assert(cancelMs < 1000L)
    assert(!handle.cancel("test_again"))

    releaseSupplier.countDown()
    assert(handle.await(5000))
  }
}
