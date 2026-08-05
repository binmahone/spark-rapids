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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, TimeUnit}

import scala.collection.JavaConverters._

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer, HostMemoryBuffer, Rmm, RmmAllocationMode}
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.io.SparkCompressionCodecTestUtils

class NvcompZstdStreamLayoutSuite extends AnyFunSuite {

  test("calculate metadata and frame lengths") {
    assertResult(1)(NvcompZstdStreamLayout.chunkCount(1, 64 * 1024))
    assertResult(2)(NvcompZstdStreamLayout.chunkCount(64 * 1024 + 1, 64 * 1024))
    assertResult(16)(NvcompZstdStreamLayout.metadataBytes(64 * 1024 + 1, 64 * 1024))
    assertResult(84)(NvcompZstdStreamLayout.frameBytes(100, 64 * 1024 + 1, 64 * 1024))
  }

  test("Zstd compression bound includes incompressible-data overhead") {
    assert(NvcompZstdStreamLayout.zstdCompressBound(1) >= 1)
    assert(NvcompZstdStreamLayout.zstdCompressBound(64 * 1024) > 64 * 1024)
    assertResult(128L * 1024 + 512L)(
      NvcompZstdStreamLayout.zstdCompressBound(128L * 1024))
  }

  test("reject invalid sizes") {
    assertThrows[IllegalArgumentException] {
      NvcompZstdStreamLayout.chunkCount(0, 64 * 1024)
    }
    assertThrows[IllegalArgumentException] {
      NvcompZstdStreamLayout.chunkCount(1, 0)
    }
    assertThrows[IllegalArgumentException] {
      NvcompZstdStreamLayout.frameBytes(7, 1, 64 * 1024)
    }
  }

  test("validate compressed-size metadata") {
    withResource(HostMemoryBuffer.allocate(16)) { metadata =>
      metadata.setLong(0, 30)
      metadata.setLong(8, 54)
      NvcompZstdStreamLayout.validateCompressedSizes(metadata, 2, 84)

      assertThrows[IllegalArgumentException] {
        NvcompZstdStreamLayout.validateCompressedSizes(metadata, 2, 83)
      }
    }
  }

  test("coalesce concurrent requests into bounded batches") {
    val observedBatches = new ConcurrentLinkedQueue[Seq[Int]]()
    val runner = new CoalescingBatchRunner[Int, Int](
      maxBatchSize = 4,
      collectWaitMillis = 20,
      inputs => {
        observedBatches.add(inputs.toSeq)
        inputs.map(_ * 2)
      })
    val pool = Executors.newFixedThreadPool(8)
    try {
      val results = (0 until 8).map { input =>
        pool.submit(new java.util.concurrent.Callable[Int] {
          override def call(): Int = runner.submit(input)
        })
      }.map(_.get(10, TimeUnit.SECONDS))

      assertResult((0 until 8).map(_ * 2).sorted)(results.sorted)
      assert(observedBatches.asScala.forall(_.size <= 4))
      assert(observedBatches.asScala.exists(_.size > 1))
      assertResult((0 until 8).sorted)(observedBatches.asScala.flatten.toSeq.sorted)
    } finally {
      pool.shutdownNow()
    }
  }

  test("propagate one batch failure to every caller") {
    val failure = new IllegalStateException("expected batch failure")
    val runner = new CoalescingBatchRunner[Int, Int](
      maxBatchSize = 2,
      collectWaitMillis = 20,
      _ => throw failure)
    val pool = Executors.newFixedThreadPool(2)
    try {
      val results = (0 until 2).map { input =>
        pool.submit(new java.util.concurrent.Callable[Throwable] {
          override def call(): Throwable = {
            intercept[IllegalStateException] {
              runner.submit(input)
            }
          }
        })
      }.map(_.get(10, TimeUnit.SECONDS))
      assert(results.forall(_ eq failure))
    } finally {
      pool.shutdownNow()
    }
  }
}

/**
 * This suite is a focused GPU compatibility probe. It is intentionally separate from the shuffle
 * writer integration tests so it can run against an existing RAPIDS runtime without rebuilding
 * or executing a complete workload.
 */
class GpuZstdStreamCompressorSuite extends AnyFunSuite with BeforeAndAfterAll {
  private val chunkSize = 64 * 1024L
  private val compressor = new GpuZstdStreamCompressor(chunkSize, 64 * 1024 * 1024L)
  private var initializedRmm = false

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    if (!Rmm.isInitialized) {
      Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 512 * 1024 * 1024)
      initializedRmm = true
    }
  }

  override protected def afterAll(): Unit = {
    try {
      if (initializedRmm) {
        Rmm.shutdown()
      }
    } finally {
      super.afterAll()
    }
  }

  private def deterministicBytes(length: Int, seed: Int): Array[Byte] = {
    Array.tabulate(length) { index =>
      ((index * 31 + seed * 17) & 0xff).toByte
    }
  }

  private def compress(input: Array[Byte]): Array[Byte] = {
    withResource(HostMemoryBuffer.allocate(input.length)) { hostInput =>
      hostInput.setBytes(0, input, 0, input.length)
      withResource(compressor.compress(hostInput, Cuda.DEFAULT_STREAM)) { compressed =>
        val result = new Array[Byte](compressed.getLength.toInt)
        compressed.getBytes(result, 0, 0, compressed.getLength)
        result
      }
    }
  }

  private def decompressWithSparkZstd(compressed: Array[Byte]): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    withResource(SparkCompressionCodecTestUtils.zstdInputStream(
      new ByteArrayInputStream(compressed))) { input =>
      val buffer = new Array[Byte](8192)
      var bytesRead = input.read(buffer)
      while (bytesRead >= 0) {
        if (bytesRead > 0) {
          output.write(buffer, 0, bytesRead)
        }
        bytesRead = input.read(buffer)
      }
    }
    output.toByteArray
  }

  private def compressWithSparkZstd(input: Array[Byte]): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    withResource(SparkCompressionCodecTestUtils.zstdOutputStream(output)) { compressed =>
      compressed.write(input)
    }
    output.toByteArray
  }

  test("Spark Zstd decoder reads nvCOMP frames without the private prefix") {
    val expected = deterministicBytes(3 * 64 * 1024 + 317, 1)
    val compressed = compress(expected)
    assertResult(expected.toSeq)(decompressWithSparkZstd(compressed).toSeq)
  }

  test("Spark Zstd decoder reads concatenated CPU-compatible GPU frames") {
    val expectedA = deterministicBytes(2 * 64 * 1024 + 13, 2)
    val expectedB = deterministicBytes(4 * 64 * 1024 + 29, 3)
    val compressed = compress(expectedA) ++ compress(expectedB)
    assertResult((expectedA ++ expectedB).toSeq)(decompressWithSparkZstd(compressed).toSeq)
  }

  test("Spark Zstd decoder reads mixed CPU and GPU task frames") {
    val cpuPayload = deterministicBytes(2 * 64 * 1024 + 41, 5)
    val gpuPayload = deterministicBytes(3 * 64 * 1024 + 73, 6)
    val compressed = compressWithSparkZstd(cpuPayload) ++ compress(gpuPayload)

    assertResult((cpuPayload ++ gpuPayload).toSeq)(
      decompressWithSparkZstd(compressed).toSeq)
  }

  test("device-resident input produces Spark-compatible Zstd frames") {
    val expected = deterministicBytes(5 * 64 * 1024 + 97, 7)
    withResource(HostMemoryBuffer.allocate(expected.length)) { hostInput =>
      hostInput.setBytes(0, expected, 0, expected.length)
      withResource(DeviceMemoryBuffer.allocate(expected.length, Cuda.DEFAULT_STREAM)) {
          deviceInput =>
        deviceInput.copyFromHostBuffer(hostInput, Cuda.DEFAULT_STREAM)
        withResource(compressor.compressDevice(
          Array(deviceInput), Cuda.DEFAULT_STREAM)) { compressed =>
          val actual = new Array[Byte](compressed.head.getLength.toInt)
          compressed.head.getBytes(actual, 0, 0, compressed.head.getLength)
          assertResult(expected.toSeq)(decompressWithSparkZstd(actual).toSeq)
        }
      }
    }
  }

  test("device compression memory estimate covers more than the live input") {
    withResource(DeviceMemoryBuffer.allocate(5 * chunkSize, Cuda.DEFAULT_STREAM)) { input =>
      val estimate = compressor.estimateAdditionalDeviceMemory(Array(input))
      assert(estimate > input.getLength)
    }
  }

  test("truncated GPU frame cannot reproduce the complete payload") {
    val expected = deterministicBytes(2 * 64 * 1024 + 11, 4)
    val compressed = compress(expected)
    val actual = decompressWithSparkZstd(compressed.dropRight(7))
    assert(actual.length < expected.length)
    assertResult(expected.take(actual.length).toSeq)(actual.toSeq)
  }
}
