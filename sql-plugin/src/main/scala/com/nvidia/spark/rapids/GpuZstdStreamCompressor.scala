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

import java.util.concurrent.{CompletableFuture, ExecutionException}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import ai.rapids.cudf.{BaseDeviceMemoryBuffer, Cuda, DeviceMemoryBuffer, HostMemoryBuffer}
import ai.rapids.cudf.nvcomp.BatchedZstdCompressor
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

/**
 * Coalesces concurrent requests into bounded batches without owning a background thread.
 *
 * The first caller becomes the batch leader. Other callers enqueue their request and wait for
 * the leader to complete the shared batch. A short collection window lets concurrently running
 * shuffle writer threads join the same GPU launch.
 */
private[rapids] class CoalescingBatchRunner[T: ClassTag, R](
    maxBatchSize: Int,
    collectWaitMillis: Long,
    runBatch: Array[T] => Array[R]) {
  require(maxBatchSize > 0, "maxBatchSize must be positive")
  require(collectWaitMillis >= 0, "collectWaitMillis must not be negative")

  private case class Request(input: T, result: CompletableFuture[R])
  private val pending = new ArrayBuffer[Request]()
  private var leaderActive = false

  def submit(input: T): R = {
    val request = Request(input, new CompletableFuture[R]())
    val isLeader = synchronized {
      pending += request
      val takeLeadership = !leaderActive
      if (takeLeadership) {
        leaderActive = true
      }
      if (pending.size >= maxBatchSize) {
        notifyAll()
      }
      takeLeadership
    }

    if (isLeader) {
      runAsLeader()
    }

    try {
      request.result.get()
    } catch {
      case e: ExecutionException => throw e.getCause
    }
  }

  private def runAsLeader(): Unit = {
    var continue = true
    while (continue) {
      val batch = synchronized {
        if (pending.size < maxBatchSize && collectWaitMillis > 0) {
          wait(collectWaitMillis)
        }
        val count = math.min(pending.size, maxBatchSize)
        val selected = pending.take(count).toVector
        pending.remove(0, count)
        selected
      }

      try {
        val outputs = runBatch(batch.map(_.input).toArray)
        require(outputs.length == batch.length,
          s"batch output count ${outputs.length} did not match input count ${batch.length}")
        batch.zip(outputs).foreach { case (request, output) =>
          request.result.complete(output)
        }
      } catch {
        case t: Throwable =>
          batch.foreach(_.result.completeExceptionally(t))
      }

      continue = synchronized {
        if (pending.nonEmpty) {
          true
        } else {
          leaderActive = false
          false
        }
      }
    }
  }
}

/**
 * Describes the private envelope produced by cuDF's BatchedZstdCompressor.
 *
 * The compressor splits one input into fixed-size chunks and returns one buffer containing:
 *
 *   [compressed-size: 8 bytes] * chunk-count
 *   [standard Zstd frame] * chunk-count
 *
 * Spark's Zstd input stream must receive only the concatenated Zstd frames. It must never receive
 * the private compressed-size prefix.
 */
private[rapids] object NvcompZstdStreamLayout {
  val CompressedSizeBytesPerChunk: Long = java.lang.Long.BYTES

  def chunkCount(uncompressedBytes: Long, chunkSize: Long): Int = {
    require(uncompressedBytes > 0, "uncompressedBytes must be positive")
    require(chunkSize > 0, "chunkSize must be positive")
    val count = (uncompressedBytes + chunkSize - 1) / chunkSize
    require(count <= Int.MaxValue, s"too many Zstd chunks: $count")
    count.toInt
  }

  def metadataBytes(uncompressedBytes: Long, chunkSize: Long): Long = {
    Math.multiplyExact(
      chunkCount(uncompressedBytes, chunkSize).toLong,
      CompressedSizeBytesPerChunk)
  }

  def frameBytes(stitchedBufferBytes: Long, uncompressedBytes: Long, chunkSize: Long): Long = {
    val metadataLength = metadataBytes(uncompressedBytes, chunkSize)
    require(stitchedBufferBytes >= metadataLength,
      s"nvCOMP output is shorter than its metadata: output=$stitchedBufferBytes, " +
        s"metadata=$metadataLength")
    stitchedBufferBytes - metadataLength
  }

  def validateCompressedSizes(
      metadata: HostMemoryBuffer,
      expectedChunkCount: Int,
      expectedFrameBytes: Long): Unit = {
    val expectedMetadataBytes =
      Math.multiplyExact(expectedChunkCount.toLong, CompressedSizeBytesPerChunk)
    require(metadata.getLength == expectedMetadataBytes,
      s"unexpected nvCOMP metadata length: actual=${metadata.getLength}, " +
        s"expected=$expectedMetadataBytes")

    var totalCompressedBytes = 0L
    var chunkIndex = 0
    while (chunkIndex < expectedChunkCount) {
      val compressedBytes = metadata.getLong(
        chunkIndex.toLong * CompressedSizeBytesPerChunk)
      require(compressedBytes > 0,
        s"nvCOMP returned an empty compressed chunk at index $chunkIndex")
      totalCompressedBytes = Math.addExact(totalCompressedBytes, compressedBytes)
      chunkIndex += 1
    }

    require(totalCompressedBytes == expectedFrameBytes,
      s"nvCOMP compressed-size metadata does not match frame bytes: " +
        s"metadataTotal=$totalCompressedBytes, frameBytes=$expectedFrameBytes")
  }
}

/**
 * Compresses one already-serialized host buffer with nvCOMP Zstd and returns only standard Zstd
 * frames in host memory.
 *
 * The returned buffer is compressed exactly once. Callers must write it directly to the shuffle
 * storage path and must not pass it through Spark's compression wrapper.
 */
class GpuZstdStreamCompressor(
    chunkSize: Long,
    maxIntermediateBufferSize: Long,
    runDecompressionProbe: Boolean = false,
    decompressionProbeSamples: Int = 1,
    decompressionProbeIterations: Int = 3,
    decompressionProbeBatchSize: Int = 16) {

  private val compressor = new BatchedZstdCompressor(chunkSize, maxIntermediateBufferSize)
  private val batchRunner = new CoalescingBatchRunner[HostMemoryBuffer, HostMemoryBuffer](
    maxBatchSize = 32,
    collectWaitMillis = 1,
    compressBatch)

  def compress(input: HostMemoryBuffer, stream: Cuda.Stream): HostMemoryBuffer = {
    require(input.getLength > 0, "GPU Zstd compression does not accept an empty input")
    require(stream == Cuda.DEFAULT_STREAM,
      "batched GPU Zstd compression currently requires the CUDA default stream")
    batchRunner.submit(input)
  }

  /**
   * Compresses device-resident serialized partitions without first copying the raw bytes to host.
   *
   * The compressor consumes one retained reference for each input. Callers retain ownership of
   * the buffers they pass to this method.
   */
  def compressDevice(
      inputs: Array[BaseDeviceMemoryBuffer],
      stream: Cuda.Stream): Array[HostMemoryBuffer] = {
    require(inputs.nonEmpty, "GPU Zstd compression requires at least one input")
    require(inputs.forall(_.getLength > 0),
      "GPU Zstd compression does not accept an empty input")
    require(stream == Cuda.DEFAULT_STREAM,
      "batched GPU Zstd compression currently requires the CUDA default stream")

    val compressorInputs = inputs.safeMap { input =>
      input.incRefCount()
      input
    }
    withResource(compressor.compress(compressorInputs, stream)) { compressedOutputs =>
      require(compressedOutputs.length == inputs.length,
        s"expected ${inputs.length} nvCOMP outputs, found ${compressedOutputs.length}")
      val hostFrames = compressedOutputs.zip(inputs).safeMap { case (stitchedOutput, input) =>
        copyStandardFramesToHost(stitchedOutput, input, stream)
      }
      closeOnExcept(hostFrames) { frames =>
        GpuZstdDecompressionProbe.runBatchIfNeeded(
          runDecompressionProbe,
          decompressionProbeSamples,
          decompressionProbeIterations,
          decompressionProbeBatchSize,
          chunkSize,
          inputs,
          compressedOutputs.map(_.asInstanceOf[BaseDeviceMemoryBuffer]),
          frames,
          stream)
        frames
      }
    }
  }

  private def copyStandardFramesToHost(
      stitchedOutput: BaseDeviceMemoryBuffer,
      originalInput: BaseDeviceMemoryBuffer,
      stream: Cuda.Stream): HostMemoryBuffer = {
    val uncompressedBytes = originalInput.getLength
    val chunkCount = NvcompZstdStreamLayout.chunkCount(uncompressedBytes, chunkSize)
    val metadataBytes = NvcompZstdStreamLayout.metadataBytes(uncompressedBytes, chunkSize)
    val frameBytes = NvcompZstdStreamLayout.frameBytes(
      stitchedOutput.getLength, uncompressedBytes, chunkSize)

    withResource(stitchedOutput.slice(0, metadataBytes)
        .asInstanceOf[BaseDeviceMemoryBuffer]) { deviceMetadata =>
      withResource(HostMemoryBuffer.allocate(metadataBytes)) { hostMetadata =>
        hostMetadata.copyFromDeviceBuffer(deviceMetadata, stream)
        NvcompZstdStreamLayout.validateCompressedSizes(
          hostMetadata, chunkCount, frameBytes)
      }
    }

    closeOnExcept(HostMemoryBuffer.allocate(frameBytes)) { hostFrames =>
      withResource(stitchedOutput.slice(metadataBytes, frameBytes)
          .asInstanceOf[BaseDeviceMemoryBuffer]) { deviceFrames =>
        hostFrames.copyFromDeviceBuffer(deviceFrames, stream)
      }
      hostFrames
    }
  }

  private def compressBatch(inputs: Array[HostMemoryBuffer]): Array[HostMemoryBuffer] = {
    val stream = Cuda.DEFAULT_STREAM
    val deviceInputs = inputs.map(input => DeviceMemoryBuffer.allocate(input.getLength, stream))
    try {
      deviceInputs.zip(inputs).foreach { case (deviceInput, hostInput) =>
        deviceInput.copyFromHostBuffer(hostInput, stream)
      }

      // BatchedZstdCompressor takes ownership of its input references. Retain the locally owned
      // references until all output frames have been copied back to host memory.
      val compressorInputs = deviceInputs.map { input =>
        input.incRefCount()
        input.asInstanceOf[BaseDeviceMemoryBuffer]
      }
      withResource(compressor.compress(compressorInputs, stream)) { compressedOutputs =>
        require(compressedOutputs.length == inputs.length,
          s"expected ${inputs.length} nvCOMP outputs, found ${compressedOutputs.length}")

        compressedOutputs.zip(deviceInputs).safeMap { case (stitchedOutput, input) =>
          copyStandardFramesToHost(stitchedOutput, input, stream)
        }
      }
    } finally {
      deviceInputs.foreach(_.safeClose())
    }
  }
}
