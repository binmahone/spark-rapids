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

import java.util.concurrent.{Callable, Executors}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{BaseDeviceMemoryBuffer, Cuda, DeviceMemoryBuffer, HostMemoryBuffer}
import ai.rapids.cudf.nvcomp.BatchedZstdDecompressor
import com.github.luben.zstd.ZstdInputStreamNoFinalizer
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.internal.Logging

/**
 * Measures CPU and GPU decompression for real GPU-serialized shuffle partitions.
 *
 * This is a bounded diagnostic probe. It does not change the shuffle reader or publish its
 * measurements as production metrics.
 */
private[rapids] object GpuZstdDecompressionProbe extends Logging {
  private val claimedSamples = new AtomicInteger(0)
  private val ReadBufferSize = 1024 * 1024

  private def claimSample(maxSamples: Int): Option[Int] = {
    var claimed = false
    var sampleId = -1
    while (!claimed && sampleId < 0) {
      val current = claimedSamples.get()
      if (current >= maxSamples) {
        sampleId = current
      } else if (claimedSamples.compareAndSet(current, current + 1)) {
        claimed = true
        sampleId = current
      }
    }
    if (claimed) {
      Some(sampleId)
    } else {
      None
    }
  }

  def runBatchIfNeeded(
      enabled: Boolean,
      maxSamples: Int,
      iterations: Int,
      requestedBatchSize: Int,
      chunkSize: Long,
      originalInputs: Array[BaseDeviceMemoryBuffer],
      stitchedCompressed: Array[BaseDeviceMemoryBuffer],
      standardFrames: Array[HostMemoryBuffer],
      stream: Cuda.Stream): Unit = {
    if (enabled) {
      require(maxSamples > 0, "GPU decompression probe maxSamples must be positive")
      require(iterations > 0, "GPU decompression probe iterations must be positive")
      require(requestedBatchSize > 0, "GPU decompression probe batchSize must be positive")
      require(originalInputs.length == stitchedCompressed.length,
        "GPU decompression probe input and compressed counts differ")
      require(originalInputs.length == standardFrames.length,
        "GPU decompression probe input and standard-frame counts differ")
      claimSample(maxSamples).foreach { sampleId =>
        val actualBatchSize = math.min(requestedBatchSize, originalInputs.length)
        runBatch(
          sampleId,
          iterations,
          chunkSize,
          originalInputs.take(actualBatchSize),
          stitchedCompressed.take(actualBatchSize),
          standardFrames.take(actualBatchSize),
          stream)
      }
    }
  }

  def runIfNeeded(
      enabled: Boolean,
      maxSamples: Int,
      iterations: Int,
      chunkSize: Long,
      originalInput: BaseDeviceMemoryBuffer,
      stitchedCompressed: BaseDeviceMemoryBuffer,
      standardFrames: HostMemoryBuffer,
      stream: Cuda.Stream): Unit = {
    if (enabled) {
      require(maxSamples > 0, "GPU decompression probe maxSamples must be positive")
      require(iterations > 0, "GPU decompression probe iterations must be positive")
      claimSample(maxSamples).foreach { sampleId =>
        run(
          sampleId,
          iterations,
          chunkSize,
          originalInput,
          stitchedCompressed,
          standardFrames,
          stream)
      }
    }
  }

  private def run(
      sampleId: Int,
      iterations: Int,
      chunkSize: Long,
      originalInput: BaseDeviceMemoryBuffer,
      stitchedCompressed: BaseDeviceMemoryBuffer,
      standardFrames: HostMemoryBuffer,
      stream: Cuda.Stream): Unit = {
    require(stream == Cuda.DEFAULT_STREAM,
      "GPU decompression probe requires the CUDA default stream")
    require(originalInput.getLength <= Int.MaxValue,
      s"GPU decompression probe input is too large: ${originalInput.getLength}")

    val cpuDecodeNs = measureCpuDecode(
      standardFrames, originalInput.getLength, iterations)
    val decompressor = new BatchedZstdDecompressor(chunkSize)

    withResource(HostMemoryBuffer.allocate(stitchedCompressed.getLength, false)) {
      hostStitched =>
        hostStitched.copyFromDeviceBuffer(stitchedCompressed, stream)
        withResource(DeviceMemoryBuffer.allocate(stitchedCompressed.getLength, stream)) {
          deviceCompressed =>
            withResource(DeviceMemoryBuffer.allocate(originalInput.getLength, stream)) {
              deviceOutput =>
                val h2dNs = new ArrayBuffer[Long](iterations)
                val gpuDecodeNs = new ArrayBuffer[Long](iterations)
                var iteration = 0
                while (iteration < iterations) {
                  val h2dStart = System.nanoTime()
                  deviceCompressed.copyFromHostBuffer(hostStitched, stream)
                  stream.sync()
                  h2dNs += System.nanoTime() - h2dStart

                  deviceCompressed.incRefCount()
                  val decompressorInput =
                    Array(deviceCompressed.asInstanceOf[BaseDeviceMemoryBuffer])
                  val decompressorOutput =
                    Array(deviceOutput.asInstanceOf[BaseDeviceMemoryBuffer])
                  val decodeStart = System.nanoTime()
                  var submitted = false
                  try {
                    decompressor.decompressAsync(decompressorInput, decompressorOutput, stream)
                    submitted = true
                  } finally {
                    if (!submitted) {
                      decompressorInput.safeClose()
                    }
                  }
                  stream.sync()
                  gpuDecodeNs += System.nanoTime() - decodeStart
                  iteration += 1
                }

                require(deviceBuffersEqual(originalInput, deviceOutput, stream),
                  "GPU Zstd decompression probe produced bytes that differ from the input")

                logInfo(
                  "GPU_ZSTD_DECOMPRESSION_PROBE " +
                    s"sample=$sampleId iterations=$iterations " +
                    s"rawBytes=${originalInput.getLength} " +
                    s"standardFrameBytes=${standardFrames.getLength} " +
                    s"stitchedBytes=${stitchedCompressed.getLength} " +
                    s"cpuDecodeNs=${cpuDecodeNs.mkString(",")} " +
                    s"h2dCompressedNs=${h2dNs.mkString(",")} " +
                    s"gpuDecodeNs=${gpuDecodeNs.mkString(",")} " +
                    "verified=true")
            }
        }
    }
  }

  private def runBatch(
      sampleId: Int,
      iterations: Int,
      chunkSize: Long,
      originalInputs: Array[BaseDeviceMemoryBuffer],
      stitchedCompressed: Array[BaseDeviceMemoryBuffer],
      standardFrames: Array[HostMemoryBuffer],
      stream: Cuda.Stream): Unit = {
    require(stream == Cuda.DEFAULT_STREAM,
      "GPU decompression probe requires the CUDA default stream")
    require(originalInputs.nonEmpty, "GPU decompression probe batch must not be empty")
    require(originalInputs.forall(_.getLength <= Int.MaxValue),
      "GPU decompression probe input is too large")

    val cpuDecodeNs = measureCpuDecodeBatch(
      standardFrames, originalInputs.map(_.getLength), iterations)
    val cpuParallelThreads = math.min(originalInputs.length, 16)
    val cpuParallelDecodeNs = measureCpuDecodeBatchParallel(
      standardFrames, originalInputs.map(_.getLength), iterations, cpuParallelThreads)
    val decompressor = new BatchedZstdDecompressor(chunkSize)
    val hostStitched = stitchedCompressed.safeMap { input =>
      val host = HostMemoryBuffer.allocate(input.getLength, false)
      host.copyFromDeviceBuffer(input, stream)
      host
    }
    withResource(hostStitched) { hostCompressed =>
      val deviceCompressed = stitchedCompressed.safeMap { input =>
        DeviceMemoryBuffer.allocate(input.getLength, stream)
      }
      withResource(deviceCompressed) { compressedOnDevice =>
        val deviceOutputs = originalInputs.safeMap { input =>
          DeviceMemoryBuffer.allocate(input.getLength, stream)
        }
        withResource(deviceOutputs) { outputs =>
          val h2dNs = new ArrayBuffer[Long](iterations)
          val gpuDecodeNs = new ArrayBuffer[Long](iterations)
          var iteration = 0
          while (iteration < iterations) {
            val h2dStart = System.nanoTime()
            compressedOnDevice.zip(hostCompressed).foreach {
              case (deviceBuffer, hostBuffer) =>
                deviceBuffer.copyFromHostBuffer(hostBuffer, stream)
            }
            stream.sync()
            h2dNs += System.nanoTime() - h2dStart

            val decompressorInputs = compressedOnDevice.map { input =>
              input.incRefCount()
              input.asInstanceOf[BaseDeviceMemoryBuffer]
            }
            val decompressorOutputs =
              outputs.map(_.asInstanceOf[BaseDeviceMemoryBuffer])
            val decodeStart = System.nanoTime()
            var submitted = false
            try {
              decompressor.decompressAsync(
                decompressorInputs, decompressorOutputs, stream)
              submitted = true
            } finally {
              if (!submitted) {
                decompressorInputs.safeClose()
              }
            }
            stream.sync()
            gpuDecodeNs += System.nanoTime() - decodeStart
            iteration += 1
          }

          originalInputs.zip(outputs).foreach { case (expected, actual) =>
            require(deviceBuffersEqual(expected, actual, stream),
              "GPU Zstd batch decompression probe produced bytes that differ from the input")
          }

          logInfo(
            "GPU_ZSTD_BATCH_DECOMPRESSION_PROBE " +
              s"sample=$sampleId iterations=$iterations " +
              s"batchSize=${originalInputs.length} " +
              s"rawBytesTotal=${originalInputs.map(_.getLength).sum} " +
              s"standardFrameBytesTotal=${standardFrames.map(_.getLength).sum} " +
              s"stitchedBytesTotal=${stitchedCompressed.map(_.getLength).sum} " +
              s"cpuSequentialDecodeNs=${cpuDecodeNs.mkString(",")} " +
              s"cpuParallelThreads=$cpuParallelThreads " +
              s"cpuParallelDecodeNs=${cpuParallelDecodeNs.mkString(",")} " +
              s"h2dCompressedNs=${h2dNs.mkString(",")} " +
              s"gpuBatchDecodeNs=${gpuDecodeNs.mkString(",")} " +
              "verified=true")
        }
      }
    }
  }

  private def measureCpuDecodeBatch(
      standardFrames: Array[HostMemoryBuffer],
      expectedBytes: Array[Long],
      iterations: Int): Seq[Long] = {
    require(standardFrames.length == expectedBytes.length,
      "CPU Zstd probe frame and expected-size counts differ")
    val timings = new ArrayBuffer[Long](iterations)
    val readBuffer = new Array[Byte](ReadBufferSize)
    var iteration = 0
    while (iteration < iterations) {
      val start = System.nanoTime()
      standardFrames.zip(expectedBytes).foreach { case (frames, expected) =>
        val decodedBytes = decodeCpuOnce(frames, readBuffer)
        require(decodedBytes == expected,
          s"CPU Zstd probe decoded $decodedBytes bytes, expected $expected")
      }
      timings += System.nanoTime() - start
      iteration += 1
    }
    timings.toSeq
  }

  private def measureCpuDecodeBatchParallel(
      standardFrames: Array[HostMemoryBuffer],
      expectedBytes: Array[Long],
      iterations: Int,
      threads: Int): Seq[Long] = {
    require(standardFrames.length == expectedBytes.length,
      "CPU Zstd probe frame and expected-size counts differ")
    require(threads > 0, "CPU Zstd probe thread count must be positive")
    val timings = new ArrayBuffer[Long](iterations)
    val pool = Executors.newFixedThreadPool(threads)
    val readBuffers = new ThreadLocal[Array[Byte]] {
      override def initialValue(): Array[Byte] = new Array[Byte](ReadBufferSize)
    }
    try {
      var iteration = 0
      while (iteration < iterations) {
        val tasks = standardFrames.indices.map { index =>
          new Callable[Long] {
            override def call(): Long = {
              decodeCpuOnce(standardFrames(index), readBuffers.get())
            }
          }
        }.asJava
        val start = System.nanoTime()
        val decodedBytes = pool.invokeAll(tasks).asScala.map(_.get())
        timings += System.nanoTime() - start
        decodedBytes.zip(expectedBytes).foreach { case (decoded, expected) =>
          require(decoded == expected,
            s"CPU Zstd probe decoded $decoded bytes, expected $expected")
        }
        iteration += 1
      }
    } finally {
      pool.shutdownNow()
    }
    timings.toSeq
  }

  private def measureCpuDecode(
      standardFrames: HostMemoryBuffer,
      expectedBytes: Long,
      iterations: Int): Seq[Long] = {
    val timings = new ArrayBuffer[Long](iterations)
    val readBuffer = new Array[Byte](ReadBufferSize)
    var iteration = 0
    while (iteration < iterations) {
      val start = System.nanoTime()
      val decodedBytes = decodeCpuOnce(standardFrames, readBuffer)
      timings += System.nanoTime() - start
      require(decodedBytes == expectedBytes,
        s"CPU Zstd probe decoded $decodedBytes bytes, expected $expectedBytes")
      iteration += 1
    }
    timings.toSeq
  }

  private def decodeCpuOnce(
      standardFrames: HostMemoryBuffer,
      readBuffer: Array[Byte]): Long = {
    withResource(new ZstdInputStreamNoFinalizer(
        new HostMemoryInputStream(standardFrames, standardFrames.getLength))
        .setContinuous(true)) { input =>
      var totalBytes = 0L
      var bytesRead = input.read(readBuffer)
      while (bytesRead >= 0) {
        totalBytes += bytesRead
        bytesRead = input.read(readBuffer)
      }
      totalBytes
    }
  }

  private def deviceBuffersEqual(
      expected: BaseDeviceMemoryBuffer,
      actual: BaseDeviceMemoryBuffer,
      stream: Cuda.Stream): Boolean = {
    require(expected.getLength == actual.getLength,
      s"buffer lengths differ: expected=${expected.getLength}, actual=${actual.getLength}")
    withResource(HostMemoryBuffer.allocate(expected.getLength, false)) { expectedHost =>
      expectedHost.copyFromDeviceBuffer(expected, stream)
      withResource(HostMemoryBuffer.allocate(actual.getLength, false)) { actualHost =>
        actualHost.copyFromDeviceBuffer(actual, stream)
        expectedHost.asByteBuffer() == actualHost.asByteBuffer()
      }
    }
  }
}
