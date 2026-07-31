/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{BaseDeviceMemoryBuffer, ContiguousTable, Cuda, DeviceMemoryBuffer,
  HostMemoryBuffer, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.jni.kudo.KudoGpuSerializer

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{GpuShuffleEnv, RapidsShuffleInternalManagerBase}
import org.apache.spark.sql.vectorized.ColumnarBatch

trait GpuPartitioning extends Partitioning with Logging {
  private[this] val (
    maxCpuBatchSize, maxCompressionBatchSize, _useGPUShuffle,
        _useKudoGPUSlicing, _useMultiThreadedShuffle, _useGpuShuffleCompression,
        gpuCompressionMaxConcurrentTasks, gpuCompressionMaxGpuSemaphoreWaiters,
        zstdChunkSize) = {
    val rapidsConf = new RapidsConf(SQLConf.get)
    (rapidsConf.shuffleParitioningMaxCpuBatchSize,
      rapidsConf.shuffleCompressionMaxBatchMemory,
      GpuShuffleEnv.useGPUShuffle(rapidsConf),
      rapidsConf.shuffleKudoGpuSerializerEnabled,
      GpuShuffleEnv.useMultiThreadedShuffle(rapidsConf),
      rapidsConf.isMultithreadedShuffleAdaptiveGpuCompressionEnabled,
      rapidsConf.multithreadedShuffleAdaptiveGpuCompressionMaxConcurrentTasks,
      rapidsConf.multithreadedShuffleAdaptiveGpuCompressionMaxGpuSemaphoreWaiters,
      rapidsConf.shuffleCompressionZstdChunkSize)
  }
  ExecutorGpuCompressionReservation.configure(gpuCompressionMaxConcurrentTasks)
  private lazy val gpuZstdCompressor =
    new GpuZstdStreamCompressor(zstdChunkSize, maxCompressionBatchSize)

  private case class AdaptiveCompressionSelection(
      plan: TaskCompressionPlan,
      state: TaskCompressionPlanState)

  // Lift once GPU shuffle supports long (64-bit) serialized-slice offsets.
  // protected[rapids] so tests can override it to exercise the guard below.
  protected[rapids] def maxGpuSerializedSliceBytes: Long = Int.MaxValue

  final def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    throw new IllegalStateException(
      "Partitioners do not support columnarEval, only columnarEvalAny")
  }

  def usesGPUShuffle: Boolean = _useGPUShuffle

  def usesKudoGPUSlicing: Boolean = _useKudoGPUSlicing

  def usesMultiThreadedShuffle: Boolean = _useMultiThreadedShuffle

  def sliceBatch(vectors: Array[RapidsHostColumnVector], start: Int, end: Int): ColumnarBatch = {
    var ret: ColumnarBatch = null
    val count = end - start
    if (count > 0) {
      ret = new ColumnarBatch(vectors.map(vec => new SlicedGpuColumnVector(vec, start, end)))
      ret.setNumRows(count)
    }
    ret
  }

  def sliceInternalOnGpuAndClose(numRows: Int, partitionIndexes: Array[Int],
      partitionColumns: Array[GpuColumnVector]): Array[ColumnarBatch] = {
    // The first index will always be 0, so we need to skip it.
    val batches = if (numRows > 0) {
      val parts = partitionIndexes.slice(1, partitionIndexes.length)
      closeOnExcept(new ArrayBuffer[ColumnarBatch](numPartitions)) { splits =>
        val contiguousTables = withResource(partitionColumns) { _ =>
          withResource(new Table(partitionColumns.map(_.getBase).toArray: _*)) { table =>
            table.contiguousSplit(parts: _*)
          }
        }
        GpuShuffleEnv.rapidsShuffleCodec match {
          case Some(codec) =>
            compressSplits(splits, codec, contiguousTables)
          case None =>
            // GpuPackedTableColumn takes ownership of the contiguous tables
            closeOnExcept(contiguousTables) { cts =>
              cts.foreach { ct => splits.append(GpuPackedTableColumn.from(ct)) }
            }
        }
        // synchronize our stream to ensure we have caught up with contiguous split
        // as downstream consumers (RapidsShuffleManager) will add hundreds of buffers
        // to the spill framework, this makes it so here we synchronize once.
        Cuda.DEFAULT_STREAM.sync()
        splits.toArray
      }
    } else {
      Array[ColumnarBatch]()
    }

    GpuSemaphore.releaseIfNecessary(TaskContext.get())
    batches
  }

  private def reslice(batch: ColumnarBatch, numSlices: Int): Seq[ColumnarBatch] = {
    if (batch.numCols() > 0) {
      withResource(batch) { _ =>
        val totalRows = batch.numRows()
        val rowsPerBatch = math.ceil(totalRows.toDouble / numSlices).toInt
        val first = batch.column(0).asInstanceOf[SlicedGpuColumnVector]
        val startOffset = first.getStart
        val endOffset = first.getEnd
        val hostColumns = (0 until batch.numCols()).map { index =>
          batch.column(index).asInstanceOf[SlicedGpuColumnVector].getWrap
        }.toArray

        startOffset.until(endOffset, rowsPerBatch).map { startIndex =>
          val end = math.min(startIndex + rowsPerBatch, endOffset)
          sliceBatch(hostColumns, startIndex, end)
        }.toList
      }
    } else {
      // This should never happen, but...
      Seq(batch)
    }
  }

  def sliceInternalOnCpuAndClose(numRows: Int, partitionIndexes: Array[Int],
      partitionColumns: Array[GpuColumnVector]): Array[(ColumnarBatch, Int)] = {
    // We need to make sure that we have a null count calculated ahead of time.
    // This should be a temp work around.
    partitionColumns.foreach(_.getBase.getNullCount)
    val totalInputSize = GpuColumnVector.getTotalDeviceMemoryUsed(partitionColumns)
    val mightNeedToSplit = totalInputSize > maxCpuBatchSize

    // We have to wrap the NvtxWithMetrics over both copyToHostAsync and corresponding CudaSync,
    // because the copyToHostAsync calls above are not guaranteed to be asynchronous (e.g.: when
    // the copy is from pageable memory, and we're not guaranteed to be using pinned memory).
    val hostPartColumns = NvtxIdWithMetrics(NvtxRegistry.PARTITION_D2H, memCopyTime) {
      val hostColumns = withResource(partitionColumns) { _ =>
        withRetryNoSplit {
          partitionColumns.safeMap(_.copyToHostAsync(Cuda.DEFAULT_STREAM))
        }
      }
      closeOnExcept(hostColumns) { _ =>
        Cuda.DEFAULT_STREAM.sync()
      }
      hostColumns
    }

    withResource(hostPartColumns) { _ =>
      // Leaving the GPU for a while
      GpuSemaphore.releaseIfNecessary(TaskContext.get())

      val origParts = new Array[ColumnarBatch](numPartitions)
      var start = 0
      for (i <- 1 until Math.min(numPartitions, partitionIndexes.length)) {
        val idx = partitionIndexes(i)
        origParts(i - 1) = sliceBatch(hostPartColumns, start, idx)
        start = idx
      }
      origParts(numPartitions - 1) = sliceBatch(hostPartColumns, start, numRows)
      val tmp = origParts.zipWithIndex.filter(_._1 != null)
      // Spark CPU shuffle in some cases has limits on the size of the data a single
      //  row can have. It is a little complicated because the limit is on the compressed
      //  and encrypted buffer, but for now we are just going to assume it is about the same
      // size.
      if (mightNeedToSplit) {
        tmp.flatMap {
          case (batch, part) =>
            val totalSize = if (batch.numCols() > 0) {
              batch.column(0) match {
                case _: SlicedGpuColumnVector =>
                  SlicedGpuColumnVector.getTotalHostMemoryUsed(batch)
                case _: SlicedSerializedColumnVector =>
                  SlicedSerializedColumnVector.getTotalHostMemoryUsed(batch)
                case _ =>
                  0L
              }
            } else {
              0L
            }
            val numOutputBatches =
              math.ceil(totalSize.toDouble / maxCpuBatchSize).toInt
            if (numOutputBatches > 1) {
              // For now we are going to slice it on number of rows instead of looking
              // at each row to try and decide. If we get in trouble we can probably
              // make this recursive and keep splitting more until it is small enough.
              reslice(batch, numOutputBatches).map { subBatch =>
                (subBatch, part)
              }
            } else {
              Seq((batch, part))
            }
        }
      } else {
        tmp
      }
    }
  }

  private def gpuSplitAndSerialize(table: Table, slices: Int*): Array[DeviceMemoryBuffer] = {
    NvtxRegistry.GPU_KUDO_SERIALIZE {
      withRetryNoSplit {
        KudoGpuSerializer.splitAndSerializeToDevice(table, slices: _*)
      }
    }
  }

  private def sliceAndSerializeOnGpu(numRows: Int, partitionIndexes: Array[Int],
      partitionColumns: Array[GpuColumnVector]): Array[(ColumnarBatch, Int)] = {
    val selection = adaptiveCompressionSelection()
    selection match {
      case Some(value) if value.plan.useGpuCompressor =>
        sliceSerializeAndCompressOnGpu(
          numRows, partitionIndexes, partitionColumns, value)
      case _ =>
        partitionColumns.foreach(_.getBase.getNullCount)
        sliceAndSerializeToHost(numRows, partitionIndexes, partitionColumns, selection)
    }
  }

  private def adaptiveCompressionSelection(): Option[AdaptiveCompressionSelection] = {
    if (!_useGpuShuffleCompression) {
      None
    } else {
      val taskContext = TaskContext.get()
      require(taskContext != null, "adaptive GPU compression requires a task context")
      val pressure = RapidsShuffleInternalManagerBase.adaptiveCompressionPressure
      val state = AdaptiveTaskCompressionPlans.getOrCreate(taskContext)
      val proposedBackend = if (
        pressure.proposesGpu(gpuCompressionMaxGpuSemaphoreWaiters)) {
        ShuffleCompressionBackend.NvcompGpuZstd
      } else {
        ShuffleCompressionBackend.SparkCpuZstd
      }
      val plan = state.getOrFreeze(
        adaptiveGpuCompressionEnabled = true,
        proposedBackend)
      if (state.markDecisionForLogging()) {
        logInfo(s"Adaptive GPU shuffle compression decision for task " +
          s"${taskContext.taskAttemptId()}: proposed=${plan.proposedBackend}, " +
          s"selected=${plan.backend}, writerPoolSize=${pressure.writerPoolSize}, " +
          s"activeWriterThreads=${pressure.activeWriterThreads}, " +
          s"queuedWriterTasks=${pressure.queuedWriterTasks}, " +
          s"gpuSemaphoreWaiters=${pressure.gpuSemaphoreWaiters}, " +
          s"gpuReservationDenied=${plan.gpuReservationDenied}")
      }
      Some(AdaptiveCompressionSelection(plan, state))
    }
  }

  private def sliceAndSerializeToHost(numRows: Int, partitionIndexes: Array[Int],
      partitionColumns: Array[GpuColumnVector],
      selection: Option[AdaptiveCompressionSelection]): Array[(ColumnarBatch, Int)] = {
    val (dataHost, offsetsHost) = withResource(partitionColumns) { _ =>
      withResource(new Table(partitionColumns.map(_.getBase).toArray: _*)) { table =>
        withResource(gpuSplitAndSerialize(table,
          partitionIndexes.tail: _*)) { dmbs =>
          val data = dmbs(0)
          val offsets = dmbs(1)
          // This bound keeps the later Long->Int narrowings lossless:
          // offsetsHost.getLong(..).toInt and dataHost.getLength.toInt
          // (dataHost is sized to data.getLength).
          require(data.getLength <= maxGpuSerializedSliceBytes,
            s"GPU-serialized shuffle batch is ${data.getLength} bytes, exceeding the " +
            s"$maxGpuSerializedSliceBytes-byte (2GB) limit addressable by the Int " +
            s"serialized-slice offsets; reduce spark.rapids.sql.batchSizeBytes")
          closeOnExcept(Seq(HostMemoryBuffer.allocate(data.getLength),
            HostMemoryBuffer.allocate(offsets.getLength))) { seq =>
            val dataHost = seq(0)
            val offsetsHost = seq(1)
            NvtxRegistry.GPU_KUDO_COPY_TO_HOST {
              dataHost.copyFromDeviceBufferAsync(data, Cuda.DEFAULT_STREAM)
              offsetsHost.copyFromDeviceBufferAsync(offsets, Cuda.DEFAULT_STREAM)
              Cuda.DEFAULT_STREAM.sync()
            }
            (dataHost, offsetsHost)
          }
        }
      }
    }
    GpuSemaphore.releaseIfNecessary(TaskContext.get())

    NvtxRegistry.GPU_KUDO_SLICE_BUFFERS {
      withResource(Seq(dataHost, offsetsHost)) { _ =>
        val numSlices = numPartitions + 1
        val elemSize = offsetsHost.getLength / numSlices

        val res = new Array[ColumnarBatch](numPartitions)
        var reportDecision =
          selection.exists(value => numRows > 0 && value.state.markDecisionForReporting())
        var start = 0
        var prevIndex: Int = 0
        for (i <- 1 until numPartitions) {
          val idx = offsetsHost.getLong((i) * elemSize).toInt
          val partNumRows = partitionIndexes(i) - prevIndex
          if (partNumRows > 0) {
            val vector = selection.map { value =>
              val adaptiveVector = new AdaptiveSerializedColumnVector(
                dataHost,
                start,
                idx,
                value.plan.proposedBackend == ShuffleCompressionBackend.NvcompGpuZstd,
                false,
                value.plan.gpuReservationDenied,
                reportDecision,
                0L)
              reportDecision = false
              adaptiveVector
            }.getOrElse(new SlicedSerializedColumnVector(dataHost, start, idx))
            res(i - 1) = new ColumnarBatch(Array(vector))
            res(i - 1).setNumRows(partNumRows)
          }
          prevIndex = partitionIndexes(i)
          start = idx
        }
        val partNumRows = numRows - prevIndex
        if (partNumRows > 0) {
          val vector = selection.map { value =>
            new AdaptiveSerializedColumnVector(
              dataHost,
              start,
              dataHost.getLength.toInt,
              value.plan.proposedBackend == ShuffleCompressionBackend.NvcompGpuZstd,
              false,
              value.plan.gpuReservationDenied,
              reportDecision,
              0L)
          }.getOrElse(
            new SlicedSerializedColumnVector(dataHost, start, dataHost.getLength.toInt))
          res(numPartitions - 1) = new ColumnarBatch(Array(vector))
          res(numPartitions - 1).setNumRows(partNumRows)
        }

        res.zipWithIndex.filter(_._1 != null)
      }
    }
  }

  private case class SerializedDevicePartition(
      partitionId: Int,
      numRows: Int,
      uncompressedBytes: Long,
      byteOffset: Long)

  private def sliceSerializeAndCompressOnGpu(
      numRows: Int,
      partitionIndexes: Array[Int],
      partitionColumns: Array[GpuColumnVector],
      selection: AdaptiveCompressionSelection): Array[(ColumnarBatch, Int)] = {
    try {
      withResource(partitionColumns) { _ =>
        if (numRows == 0) {
          Array.empty[(ColumnarBatch, Int)]
        } else {
          partitionColumns.foreach(_.getBase.getNullCount)
          require(_useMultiThreadedShuffle,
            "GPU-resident Spark-compatible compression requires MULTITHREADED shuffle")
          require(SQLConf.get.getConfString("spark.io.compression.codec", "lz4")
            .equalsIgnoreCase("zstd"),
            "GPU-resident Spark-compatible compression requires spark.io.compression.codec=zstd")
          require(SQLConf.get.getConfString("spark.shuffle.compress", "true").toBoolean,
            "GPU-resident Spark-compatible compression requires spark.shuffle.compress=true")
          withResource(new Table(partitionColumns.map(_.getBase).toArray: _*)) { table =>
            withResource(gpuSplitAndSerialize(table, partitionIndexes.tail: _*)) { dmbs =>
              val data = dmbs(0)
              val offsets = dmbs(1)
              require(data.getLength <= maxGpuSerializedSliceBytes,
                s"GPU-serialized shuffle batch is ${data.getLength} bytes, exceeding the " +
                  s"$maxGpuSerializedSliceBytes-byte (2GB) limit")

              withResource(HostMemoryBuffer.allocate(offsets.getLength)) { offsetsHost =>
                offsetsHost.copyFromDeviceBufferAsync(offsets, Cuda.DEFAULT_STREAM)
                Cuda.DEFAULT_STREAM.sync()

                val numOffsets = numPartitions + 1
                require(offsetsHost.getLength ==
                  Math.multiplyExact(numOffsets.toLong, java.lang.Long.BYTES.toLong),
                  s"unexpected Kudo offset buffer length ${offsetsHost.getLength} for " +
                    s"$numPartitions partitions")
                val offsetElementSize = java.lang.Long.BYTES.toLong
                val partitions = new ArrayBuffer[SerializedDevicePartition](numPartitions)
                var partitionId = 0
                var previousRowIndex = 0
                var previousByteOffset = 0L
                while (partitionId < numPartitions) {
                  val nextRowIndex =
                    if (partitionId + 1 < partitionIndexes.length) {
                      partitionIndexes(partitionId + 1)
                    } else {
                      numRows
                    }
                  val nextByteOffset = offsetsHost.getLong(
                    (partitionId + 1L) * offsetElementSize)
                  require(nextRowIndex >= previousRowIndex && nextRowIndex <= numRows,
                    s"invalid row boundary $nextRowIndex for shuffle partition $partitionId")
                  require(nextByteOffset >= previousByteOffset &&
                      nextByteOffset <= data.getLength,
                    s"invalid byte boundary $nextByteOffset for shuffle partition $partitionId")

                  val partitionRows = nextRowIndex - previousRowIndex
                  val partitionBytes = nextByteOffset - previousByteOffset
                  if (partitionRows > 0) {
                    require(partitionBytes > 0,
                      s"non-empty shuffle partition $partitionId has no serialized bytes")
                    partitions += SerializedDevicePartition(
                      partitionId,
                      partitionRows,
                      partitionBytes,
                      previousByteOffset)
                  } else {
                    require(partitionBytes == 0,
                      s"empty shuffle partition $partitionId has $partitionBytes serialized bytes")
                  }
                  previousRowIndex = nextRowIndex
                  previousByteOffset = nextByteOffset
                  partitionId += 1
                }
                require(previousRowIndex == numRows,
                  s"final shuffle row boundary $previousRowIndex did not match $numRows rows")
                require(previousByteOffset == data.getLength,
                  s"final shuffle byte boundary $previousByteOffset did not match " +
                    s"${data.getLength} serialized bytes")
                require(partitions.nonEmpty,
                  "non-empty shuffle batch produced no serialized partitions")

                val deviceSlices = closeOnExcept(
                    new ArrayBuffer[BaseDeviceMemoryBuffer](partitions.length)) { result =>
                  partitions.foreach { partition =>
                    result += data.slice(partition.byteOffset, partition.uncompressedBytes)
                      .asInstanceOf[BaseDeviceMemoryBuffer]
                  }
                  result.toArray
                }
                withResource(deviceSlices) { _ =>
                  val compressionStartNs = System.nanoTime()
                  val hostFrames = gpuZstdCompressor.compressDevice(
                    deviceSlices, Cuda.DEFAULT_STREAM)
                  val compressionTimeNs = System.nanoTime() - compressionStartNs
                  try {
                    require(hostFrames.length == partitions.length,
                      s"expected ${partitions.length} compressed partitions, " +
                        s"found ${hostFrames.length}")
                    val batches = closeOnExcept(
                        new ArrayBuffer[ColumnarBatch](partitions.length)) { result =>
                      var reportDecision = selection.state.markDecisionForReporting()
                      hostFrames.zip(partitions).foreach { case (frames, partition) =>
                        require(frames.getLength <= Int.MaxValue,
                          s"compressed shuffle partition ${partition.partitionId} exceeds " +
                            s"the JVM slice limit: ${frames.getLength}")
                        val vector = new PrecompressedSerializedColumnVector(
                          frames,
                          0,
                          frames.getLength.toInt,
                          partition.uncompressedBytes,
                          selection.plan.proposedBackend ==
                            ShuffleCompressionBackend.NvcompGpuZstd,
                          selection.plan.gpuReservationDenied,
                          reportDecision,
                          if (reportDecision) compressionTimeNs else 0L)
                        reportDecision = false
                        val batch = closeOnExcept(vector) { _ =>
                          new ColumnarBatch(Array(vector))
                        }
                        batch.setNumRows(partition.numRows)
                        result += batch
                      }
                      result.toArray
                    }
                    batches.zip(partitions.map(_.partitionId))
                  } finally {
                    hostFrames.foreach(_.safeClose())
                  }
                }
              }
            }
          }
        }
      }
    } finally {
      GpuSemaphore.releaseIfNecessary(TaskContext.get())
    }
  }

  def sliceInternalGpuOrCpuAndClose(numRows: Int, partitionIndexes: Array[Int],
      partitionColumns: Array[GpuColumnVector]): Array[(ColumnarBatch, Int)] = {
    if (usesKudoGPUSlicing) {
      sliceAndSerializeOnGpu(numRows, partitionIndexes, partitionColumns)
    } else {
      val sliceOnGpu = usesGPUShuffle
      val nvtxId = if (sliceOnGpu) {
        NvtxRegistry.SLICE_INTERNAL_GPU
      } else {
        NvtxRegistry.SLICE_INTERNAL_CPU
      }
      // If we are not using the Rapids shuffle we fall back to CPU splits way to avoid the hit
      // for large number of small splits.
      nvtxId {
        if (sliceOnGpu) {
          val tmp = sliceInternalOnGpuAndClose(numRows, partitionIndexes, partitionColumns)
          tmp.zipWithIndex.filter(_._1 != null)
        } else {
          sliceInternalOnCpuAndClose(numRows, partitionIndexes, partitionColumns)
        }
      }
    }
  }

  /**
   * Compress contiguous tables representing the splits into compressed columnar batches.
   * Contiguous tables corresponding to splits with no data will not be compressed.
   * @param outputBatches where to collect the corresponding columnar batches for the splits
   * @param codec compression codec to use
   * @param contiguousTables contiguous tables to compress
   */
  def compressSplits(
      outputBatches: ArrayBuffer[ColumnarBatch],
      codec: TableCompressionCodec,
      contiguousTables: Array[ContiguousTable]): Unit = {
    withResource(codec.createBatchCompressor(maxCompressionBatchSize,
        Cuda.DEFAULT_STREAM)) { compressor =>
      // tracks batches with no data and the corresponding output index for the batch
      val emptyBatches = new ArrayBuffer[(ColumnarBatch, Int)]

      // add each table either to the batch to be compressed or to the empty batch tracker
      contiguousTables.zipWithIndex.foreach { case (ct, i) =>
        if (ct.getRowCount == 0) {
          emptyBatches.append((GpuPackedTableColumn.from(ct), i))
        } else {
          compressor.addTableToCompress(ct)
        }
      }

      withResource(compressor.finish()) { compressedTables =>
        var compressedTableIndex = 0
        var outputIndex = 0
        emptyBatches.foreach { case (emptyBatch, emptyOutputIndex) =>
          require(emptyOutputIndex >= outputIndex)
          // add any compressed batches that need to appear before the next empty batch
          val numCompressedToAdd = emptyOutputIndex - outputIndex
          (0 until numCompressedToAdd).foreach { _ =>
            val compressedTable = compressedTables(compressedTableIndex)
            outputBatches.append(GpuCompressedColumnVector.from(compressedTable))
            compressedTableIndex += 1
          }
          outputBatches.append(emptyBatch)
          outputIndex = emptyOutputIndex + 1
        }

        // add any compressed batches that remain after the last empty batch
        (compressedTableIndex until compressedTables.length).foreach { i =>
          val ct = compressedTables(i)
          outputBatches.append(GpuCompressedColumnVector.from(ct))
        }
      }
    }
  }

  private var memCopyTime: GpuMetric = NoopMetric

  /**
   * Setup sub-metrics for the performance debugging of GpuPartition. This method is expected to
   * be called at the query planning stage. Therefore, this method is NOT thread safe.
   */
  def setupDebugMetrics(metrics: Map[String, GpuMetric]): Unit = {
    metrics.get(GpuMetric.COPY_TO_HOST_TIME).foreach(memCopyTime = _)
  }
}
