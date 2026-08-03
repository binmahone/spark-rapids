/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

import java.io._
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import ai.rapids.cudf.{Cuda, HostColumnVector, HostMemoryBuffer, JCudfSerialization}
import ai.rapids.cudf.JCudfSerialization.SerializedTableHeader
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.KudoSerializedTableColumn.SER_CHECK_SCHEMA
import com.nvidia.spark.rapids.RapidsConf.ShuffleKudoMode
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{withRetryNoSplit, SizeProvider}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.jni.kudo.{KudoSerializer, KudoTableHeader, WriteInput}

import org.apache.spark.TaskContext
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase.{
  METRIC_DATA_SIZE,
  METRIC_SHUFFLE_DESER_STREAM_TIME,
  METRIC_SHUFFLE_KUDO_BUFFER_ACQUIRE_CPU_TIME,
  METRIC_SHUFFLE_KUDO_BUFFER_ACQUIRE_TIME,
  METRIC_SHUFFLE_KUDO_HEADER_READ_CPU_TIME,
  METRIC_SHUFFLE_KUDO_HEADER_READ_TIME,
  METRIC_SHUFFLE_KUDO_PAYLOAD_READ_BYTES,
  METRIC_SHUFFLE_KUDO_PAYLOAD_READ_CPU_TIME,
  METRIC_SHUFFLE_KUDO_PAYLOAD_READ_TIME,
  METRIC_SHUFFLE_KUDO_TASK_DEDICATED_BUFFER_COUNT,
  METRIC_SHUFFLE_KUDO_TASK_HOST_ALLOCATION_BYTES,
  METRIC_SHUFFLE_KUDO_TASK_HOST_ALLOCATION_COUNT,
  METRIC_SHUFFLE_KUDO_TASK_HOST_ALLOCATION_TIME,
  METRIC_SHUFFLE_KUDO_TASK_SHARED_BUFFER_CREATE_COUNT,
  METRIC_SHUFFLE_KUDO_TASK_SHARED_BUFFER_SLICE_COUNT,
  METRIC_SHUFFLE_KUDO_TASK_SHARED_LOCK_WAIT_TIME,
  METRIC_SHUFFLE_KUDO_TASK_SHARED_SAMPLE_COUNT,
  METRIC_SHUFFLE_KUDO_TASK_SHARED_THRESHOLD_REJECT_COUNT,
  METRIC_SHUFFLE_SER_COPY_BUFFER_TIME,
  METRIC_SHUFFLE_SER_STREAM_TIME,
  METRIC_SHUFFLE_STALLED_BY_INPUT_STREAM}
import org.apache.spark.sql.types.{DataType, NullType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A wrapper around InputStream that tracks time spent on all operations
 * to measure how much time is stalled by input stream operations.
 * 
 * This wrapper uses manual timing with += instead of the .ns{} method for better performance.
 * The .ns{} method has function call overhead and additional logic for semaphore wait time
 * tracking, while manual timing with System.nanoTime() and += is more lightweight for
 * high-frequency operations like InputStream reads.
 * 
 * Note: We wrap at the InputStream level (not DataInputStream) because DataInputStream
 * methods are mostly final and cannot be overridden.
 */
class InputStreamWrapper(underlying: InputStream, stalledByInputStreamMetric: GpuMetric)
  extends InputStream {
  
  @inline
  private def timeOperation[T](operation: => T): T = {
    val start = System.nanoTime()
    try {
      operation
    } finally {
      stalledByInputStreamMetric += (System.nanoTime() - start)
    }
  }
  
  override def read(): Int = timeOperation(underlying.read())
  override def read(b: Array[Byte]): Int = timeOperation(underlying.read(b))
  override def read(b: Array[Byte], off: Int, len: Int): Int =
    timeOperation(underlying.read(b, off, len))
  override def skip(n: Long): Long = timeOperation(underlying.skip(n))
  override def available(): Int = timeOperation(underlying.available())
  override def close(): Unit = timeOperation(underlying.close())
  override def mark(readlimit: Int): Unit = timeOperation(underlying.mark(readlimit))
  override def reset(): Unit = timeOperation(underlying.reset())
  override def markSupported(): Boolean = timeOperation(underlying.markSupported())
}

/**
 * Iterator that reads serialized tables from a stream.
 */
trait BaseSerializedTableIterator extends Iterator[(Int, ColumnarBatch)] {
  /**
   * Attempt to read the next batch size from the stream.
   * @return the length of the data to read, or None if the stream is closed or ended
   */
  def peekNextBatchSize(): Option[Long]
}

object ThreadCpuTime {
  private val bean = ManagementFactory.getThreadMXBean

  def now(): Long = {
    if (bean.isCurrentThreadCpuTimeSupported) {
      val value = bean.getCurrentThreadCpuTime
      if (value >= 0L) value else 0L
    } else {
      0L
    }
  }
}

class SerializedBatchIterator(dIn: DataInputStream, deserTime: GpuMetric)
  extends BaseSerializedTableIterator {
  private[this] var nextHeader: Option[SerializedTableHeader] = None
  private[this] var streamClosed: Boolean = false

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      dIn.close()
      streamClosed = true
    }
  }

  override def peekNextBatchSize(): Option[Long] = deserTime.ns {
    if (streamClosed) {
      None
    } else {
      if (nextHeader.isEmpty) {
        NvtxRegistry.READ_HEADER {
          val header = new SerializedTableHeader(dIn)
          if (header.wasInitialized) {
            nextHeader = Some(header)
          } else {
            dIn.close()
            streamClosed = true
            nextHeader = None
          }
        }
      }
      nextHeader.map(_.getDataLen)
    }
  }

  private def readNextBatch(): ColumnarBatch = deserTime.ns {
    NvtxRegistry.READ_BATCH {
      val header = nextHeader.get
      nextHeader = None
      if (header.getNumColumns > 0) {
        // This buffer will later be concatenated into another host buffer before being
        // sent to the GPU, so no need to use pinned memory for these buffers.
        closeOnExcept(
          HostMemoryBuffer.allocate(header.getDataLen, false)) { hostBuffer =>
          JCudfSerialization.readTableIntoBuffer(dIn, header, hostBuffer)
          SerializedTableColumn.from(header, hostBuffer)
        }
      } else {
        SerializedTableColumn.from(header)
      }
    }
  }

  override def hasNext: Boolean = {
    peekNextBatchSize()
    nextHeader.isDefined
  }

  override def next(): (Int, ColumnarBatch) = {
    if (!hasNext) {
      throw new NoSuchElementException("Walked off of the end...")
    }
    (0, readNextBatch())
  }
}

/**
 * Serializer for serializing `ColumnarBatch`s for use during normal shuffle.
 *
 * The serialization write path takes the cudf `Table` that is described by the `ColumnarBatch`
 * and uses cudf APIs to serialize the data into a sequence of bytes on the host. The data is
 * returned to the Spark shuffle code where it is compressed by the CPU and written to disk.
 *
 * The serialization read path is notably different. The sequence of serialized bytes IS NOT
 * deserialized into a cudf `Table` but rather tracked in host memory by a `ColumnarBatch`
 * that contains a [[SerializedTableColumn]]. During query planning, each GPU columnar shuffle
 * exchange is followed by a [[GpuShuffleCoalesceExec]] that expects to receive only these
 * custom batches of [[SerializedTableColumn]]. [[GpuShuffleCoalesceExec]] coalesces the smaller
 * shuffle partitions into larger tables before placing them on the GPU for further processing.
 *
 * @note The RAPIDS shuffle does not use this code.
 */
class GpuColumnarBatchSerializer(metrics: Map[String, GpuMetric], dataTypes: Array[DataType],
                                 kudoMode: ShuffleKudoMode.Value, useKudo: Boolean,
                                 kudoMeasureBufferCopy: Boolean,
                                 kudoTaskSharedBufferEnabled: Boolean = false,
                                 kudoTaskSharedBufferTriggerSize: Int = 1 << 20,
                                 kudoTaskSharedBufferTargetTableCount: Int = 10,
                                 kudoTaskSharedBufferMaxSize: Int = 20 << 20,
                                 kudoTaskSharedBufferStripeCount: Int = 1)
  extends Serializer with Serializable {

  override def newInstance(): SerializerInstance = {
    if (useKudo) {
      if (kudoMode == ShuffleKudoMode.GPU) {
        new KudoGpuSerializerInstance(
          metrics, dataTypes, kudoTaskSharedBufferEnabled, kudoTaskSharedBufferTriggerSize,
          kudoTaskSharedBufferTargetTableCount, kudoTaskSharedBufferMaxSize,
          kudoTaskSharedBufferStripeCount)
      } else {
        new KudoSerializerInstance(metrics, dataTypes, kudoMeasureBufferCopy,
          kudoTaskSharedBufferEnabled, kudoTaskSharedBufferTriggerSize,
          kudoTaskSharedBufferTargetTableCount, kudoTaskSharedBufferMaxSize,
          kudoTaskSharedBufferStripeCount)
      }
    } else {
      new GpuColumnarBatchSerializerInstance(metrics)
    }
  }

  override def supportsRelocationOfSerializedObjects: Boolean = true
}

private class GpuColumnarBatchSerializerInstance(metrics: Map[String, GpuMetric]) extends
  SerializerInstance {
  private val dataSize = metrics(METRIC_DATA_SIZE)
  private val serTime = metrics(METRIC_SHUFFLE_SER_STREAM_TIME)
  private val deserTime = metrics(METRIC_SHUFFLE_DESER_STREAM_TIME)
  private val stalledByInputStream = metrics(METRIC_SHUFFLE_STALLED_BY_INPUT_STREAM)


  override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {
    private[this] val dOut: DataOutputStream =
      new DataOutputStream(new BufferedOutputStream(out))

    override def writeValue[T: ClassTag](value: T): SerializationStream = serTime.ns {
      val batch = value.asInstanceOf[ColumnarBatch]
      val numColumns = batch.numCols()
      val columns: Array[HostColumnVector] = new Array(numColumns)
      val toClose = new ArrayBuffer[AutoCloseable](numColumns)
      try {
        var startRow = 0
        val numRows = batch.numRows()
        if (batch.numCols() > 0) {
          val firstCol = batch.column(0)
          if (firstCol.isInstanceOf[SlicedGpuColumnVector]) {
            // We don't have control over ColumnarBatch to put in the slice, so we have to do it
            // for each column.  In this case we are using the first column.
            startRow = firstCol.asInstanceOf[SlicedGpuColumnVector].getStart
            for (i <- 0 until numColumns) {
              columns(i) = batch.column(i).asInstanceOf[SlicedGpuColumnVector].getBase
            }
          } else {
            for (i <- 0 until numColumns) {
              batch.column(i) match {
                case gpu: GpuColumnVector =>
                  val cpu = gpu.copyToHost()
                  toClose += cpu
                  columns(i) = cpu.getBase
                case cpu: RapidsHostColumnVector =>
                  columns(i) = cpu.getBase
              }
            }
          }

          dataSize += JCudfSerialization.getSerializedSizeInBytes(columns, startRow, numRows)
          NvtxRegistry.COLUMNAR_BATCH_SERIALIZE {
            JCudfSerialization.writeToStream(columns, dOut, startRow, numRows)
          }
        } else {
          NvtxRegistry.COLUMNAR_BATCH_SERIALIZE_ROW_ONLY {
            JCudfSerialization.writeRowsToStream(dOut, numRows)
          }
        }
      } finally {
        toClose.safeClose()
      }
      this
    }

    override def writeKey[T: ClassTag](key: T): SerializationStream = {
      // The key is only needed on the map side when computing partition ids. It does not need to
      // be shuffled.
      assert(null == key || key.isInstanceOf[Int])
      this
    }

    override def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def writeObject[T: ClassTag](t: T): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def flush(): Unit = {
      dOut.flush()
    }

    override def close(): Unit = {
      dOut.close()
    }
  }


  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {
      private[this] val wrappedIn = new InputStreamWrapper(in, stalledByInputStream)
      private[this] val dIn: DataInputStream =
        new DataInputStream(new BufferedInputStream(wrappedIn))

      override def asKeyValueIterator: Iterator[(Int, ColumnarBatch)] = {
        new SerializedBatchIterator(dIn, deserTime)
      }

      override def asIterator: Iterator[Any] = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def readKey[T]()(implicit classType: ClassTag[T]): T = {
        // We skipped serialization of the key in writeKey(), so just return a dummy value since
        // this is going to be discarded anyways.
        null.asInstanceOf[T]
      }

      override def readValue[T]()(implicit classType: ClassTag[T]): T = {
        // This method should never be called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def readObject[T]()(implicit classType: ClassTag[T]): T = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def close(): Unit = {
        dIn.close()
      }
    }
  }

  // These methods are never called by shuffle code.
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException
}

/**
 * A special `ColumnVector` that describes a serialized table read from shuffle.
 * This appears in a `ColumnarBatch` to pass serialized tables to [[GpuShuffleCoalesceExec]]
 * which should always appear in the query plan immediately after a shuffle.
 */
class SerializedTableColumn(
    val header: SerializedTableHeader,
    val hostBuffer: HostMemoryBuffer) extends GpuColumnVectorBase(NullType) with SizeProvider {
  override def close(): Unit = {
    if (hostBuffer != null) {
      hostBuffer.close()
    }
  }

  override def hasNull: Boolean = throw new IllegalStateException("should not be called")

  override def numNulls(): Int = throw new IllegalStateException("should not be called")

  override def sizeInBytes: Long = if (hostBuffer == null) {
    0L
  } else {
    hostBuffer.getLength
  }
}

object SerializedTableColumn {
  /**
   * Build a `ColumnarBatch` consisting of a single [[SerializedTableColumn]] describing
   * the specified serialized table.
   *
   * @param header     header for the serialized table
   * @param hostBuffer host buffer containing the table data
   * @return columnar batch to be passed to [[GpuShuffleCoalesceExec]]
   */
  def from(
      header: SerializedTableHeader,
      hostBuffer: HostMemoryBuffer = null): ColumnarBatch = {
    val column = new SerializedTableColumn(header, hostBuffer)
    new ColumnarBatch(Array(column), header.getNumRows)
  }

  def getMemoryUsed(batch: ColumnarBatch): Long = {
    var sum: Long = 0
    if (batch.numCols == 1) {
      val cv = batch.column(0)
      cv match {
        case serializedTableColumn: SerializedTableColumn =>
          sum += Option(serializedTableColumn.hostBuffer).map(_.getLength).getOrElse(0L)
        case kudo: KudoSerializedTableColumn =>
          sum += Option(kudo.spillableKudoTable).map(_.length).getOrElse(0L)
        case _ =>
          throw new IllegalStateException(s"Unexpected column type: ${cv.getClass}" )
      }
    }
    sum
  }
}

/**
 * Allocates host buffers across all Kudo block iterators owned by one serializer instance.
 *
 * A threaded shuffle reader creates one serializer instance per Spark task but one
 * deserialization stream per shuffle block. Keeping the sampling and active root buffer here
 * allows single-table block streams to share allocations without sharing memory across tasks.
 * Slices retain the root buffer through the cudf reference count, so rotating or closing the
 * allocator root does not invalidate batches that are still owned by downstream consumers.
 */
private[rapids] class KudoTaskSharedHostBuffer(
    metrics: Map[String, GpuMetric],
    triggerSize: Int,
    sampleCount: Int = 5,
    minBufferSize: Int = 1 << 20,
    maxBufferSize: Int = 20 << 20,
    targetTableCount: Int = 10,
    testHostAllocator: Option[Int => HostMemoryBuffer] = None) extends AutoCloseable {
  require(triggerSize > 0, "triggerSize must be positive")
  require(sampleCount > 0, "sampleCount must be positive")
  require(minBufferSize > 0, "minBufferSize must be positive")
  require(maxBufferSize >= minBufferSize,
    "maxBufferSize must be greater than or equal to minBufferSize")
  require(targetTableCount > 0, "targetTableCount must be positive")

  private[this] val sampledMetric =
    metrics.getOrElse(METRIC_SHUFFLE_KUDO_TASK_SHARED_SAMPLE_COUNT, NoopMetric)
  private[this] val thresholdRejectMetric =
    metrics.getOrElse(METRIC_SHUFFLE_KUDO_TASK_SHARED_THRESHOLD_REJECT_COUNT, NoopMetric)
  private[this] val sharedBufferCreateMetric =
    metrics.getOrElse(METRIC_SHUFFLE_KUDO_TASK_SHARED_BUFFER_CREATE_COUNT, NoopMetric)
  private[this] val sharedBufferSliceMetric =
    metrics.getOrElse(METRIC_SHUFFLE_KUDO_TASK_SHARED_BUFFER_SLICE_COUNT, NoopMetric)
  private[this] val dedicatedBufferMetric =
    metrics.getOrElse(METRIC_SHUFFLE_KUDO_TASK_DEDICATED_BUFFER_COUNT, NoopMetric)
  private[this] val hostAllocationCountMetric =
    metrics.getOrElse(METRIC_SHUFFLE_KUDO_TASK_HOST_ALLOCATION_COUNT, NoopMetric)
  private[this] val hostAllocationBytesMetric =
    metrics.getOrElse(METRIC_SHUFFLE_KUDO_TASK_HOST_ALLOCATION_BYTES, NoopMetric)
  private[this] val hostAllocationTimeMetric =
    metrics.getOrElse(METRIC_SHUFFLE_KUDO_TASK_HOST_ALLOCATION_TIME, NoopMetric)
  private[this] val lockWaitTimeMetric =
    metrics.getOrElse(METRIC_SHUFFLE_KUDO_TASK_SHARED_LOCK_WAIT_TIME, NoopMetric)

  private[this] val samples = new ArrayBuffer[Int](sampleCount)
  private[this] var decisionMade = false
  private[this] var sharingEnabled = false
  private[this] var sharedBufferSize = 0
  private[this] var currentRoot: HostMemoryBuffer = null
  private[this] var currentOffset = 0
  private[this] var closed = false

  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      close()
    }
  }

  private def allocateHostWithRetry(size: Int): HostMemoryBuffer = {
    val startNs = System.nanoTime()
    try {
      testHostAllocator.map { allocator =>
        hostAllocationCountMetric += 1
        hostAllocationBytesMetric += size
        allocator(size)
      }.getOrElse {
        withRetryNoSplit[HostMemoryBuffer] {
          hostAllocationCountMetric += 1
          hostAllocationBytesMetric += size
          // These buffers are concatenated before reaching the GPU, so pinned memory is
          // unnecessary.
          HostMemoryBuffer.allocate(size, false)
        }
      }
    } finally {
      hostAllocationTimeMetric += System.nanoTime() - startNs
    }
  }

  private def allocateDedicated(size: Int): HostMemoryBuffer = {
    dedicatedBufferMetric += 1
    allocateHostWithRetry(size)
  }

  private def finishSampling(): Unit = {
    decisionMade = true
    val maxSample = samples.max
    if (maxSample < triggerSize) {
      val average = samples.foldLeft(0L)(_ + _) / samples.length
      sharedBufferSize = math.min(maxBufferSize,
        math.max(minBufferSize,
          math.min(Int.MaxValue.toLong, average * targetTableCount).toInt))
      sharingEnabled = true
    } else {
      thresholdRejectMetric += 1
    }
  }

  private def rotateRoot(): Unit = {
    if (currentRoot != null) {
      currentRoot.close()
    }
    currentRoot = allocateHostWithRetry(sharedBufferSize)
    sharedBufferCreateMetric += 1
    currentOffset = 0
  }

  def acquire(size: Int): HostMemoryBuffer = {
    val waitStart = System.nanoTime()
    synchronized {
      lockWaitTimeMetric += System.nanoTime() - waitStart
      acquireLocked(size)
    }
  }

  private def acquireLocked(size: Int): HostMemoryBuffer = {
    require(size >= 0, "buffer size must be non-negative")
    if (closed) {
      throw new IllegalStateException("Kudo task shared host buffer is closed")
    }

    if (!decisionMade) {
      val result = allocateDedicated(size)
      samples += size
      sampledMetric += 1
      if (samples.length == sampleCount) {
        finishSampling()
      }
      result
    } else if (!sharingEnabled || size == 0 || size > sharedBufferSize / 2) {
      allocateDedicated(size)
    } else {
      if (currentRoot == null || currentOffset + size > sharedBufferSize) {
        rotateRoot()
      }
      val result = currentRoot.slice(currentOffset, size)
      currentOffset += size
      sharedBufferSliceMetric += 1
      result
    }
  }

  override def close(): Unit = synchronized {
    if (!closed) {
      if (currentRoot != null) {
        currentRoot.close()
        currentRoot = null
      }
      closed = true
    }
  }
}

/**
 * Routes background shuffle-reader threads to independently locked host-buffer allocators.
 * A small number of stripes reduces allocator monitor contention while bounding the amount of
 * live task-scoped root memory.
 */
private[rapids] class KudoTaskSharedHostBufferSet(
    metrics: Map[String, GpuMetric],
    triggerSize: Int,
    targetTableCount: Int,
    maxBufferSize: Int,
    stripeCount: Int) extends AutoCloseable {
  require(stripeCount > 0, "stripeCount must be positive")

  private[this] val stripes = Array.fill(stripeCount) {
    new KudoTaskSharedHostBuffer(metrics, triggerSize,
      targetTableCount = targetTableCount, maxBufferSize = maxBufferSize)
  }

  private[rapids] def stripeIndex(threadId: Long): Int =
    Math.floorMod(threadId, stripeCount.toLong).toInt

  def acquire(size: Int): HostMemoryBuffer =
    stripes(stripeIndex(Thread.currentThread().getId)).acquire(size)

  override def close(): Unit = stripes.foreach(_.close())
}

/**
 * Serializer instance for serializing `ColumnarBatch`s for use during shuffle with
 * [[KudoSerializer]]
 * @param metrics map of GpuMetric contains serializer specific metrics
 * @param dataTypes data types of the columns in the batch
 * @param measureBufferCopyTime whether to measure the buffer copy time
 */
private class KudoSerializerInstance(
    val metrics: Map[String, GpuMetric],
    val dataTypes: Array[DataType],
    val measureBufferCopyTime: Boolean,
    val taskSharedBufferEnabled: Boolean,
    val taskSharedBufferTriggerSize: Int,
    val taskSharedBufferTargetTableCount: Int,
    val taskSharedBufferMaxSize: Int,
    val taskSharedBufferStripeCount: Int
) extends SerializerInstance {
  private val dataSize = metrics(METRIC_DATA_SIZE)
  private val serTime = metrics(METRIC_SHUFFLE_SER_STREAM_TIME)
  private val serCopyBufferTime = metrics(METRIC_SHUFFLE_SER_COPY_BUFFER_TIME)
  private val deserTime = metrics(METRIC_SHUFFLE_DESER_STREAM_TIME)
  private val stalledByInputStream = metrics(METRIC_SHUFFLE_STALLED_BY_INPUT_STREAM)
  private val taskSharedBuffer = if (taskSharedBufferEnabled) {
    Some(new KudoTaskSharedHostBufferSet(metrics, taskSharedBufferTriggerSize,
      targetTableCount = taskSharedBufferTargetTableCount,
      maxBufferSize = taskSharedBufferMaxSize,
      stripeCount = taskSharedBufferStripeCount))
  } else {
    None
  }

  private lazy val kudo = if (dataTypes.nonEmpty) {
    Some(new KudoSerializer(GpuColumnVector.from(dataTypes)))
  } else {
    None
  }

  override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {

    override def writeValue[T: ClassTag](value: T): SerializationStream = serTime.ns {
      val batch = value.asInstanceOf[ColumnarBatch]
      if (SER_CHECK_SCHEMA) {
        // The written batch should always has the same schema with the one used to create
        // the kudo serializer. But if they become different in some cases unintentionally,
        // the kudo shuffle can easily run into errors. e.g. the negative "numRows" error.
        // Then enable this to figure out if the error is caused by mismatched batches.
        val actualTypes = GpuColumnVector.extractTypes(batch)
        if (!dataTypes.sameElements(actualTypes)) {
          throw new IllegalStateException(s"Kudo writer got a mismatched batch, types: " +
            s"[${actualTypes.mkString("; ")}], but expected types: " +
            s"[${dataTypes.mkString("; ")}].")
        }
      }
      val numColumns = batch.numCols()
      val columns: Array[HostColumnVector] = new Array(numColumns)
      withResource(new ArrayBuffer[AutoCloseable](numColumns)) { toClose =>
        var startRow = 0
        val numRows = batch.numRows()
        if (batch.numCols() > 0) {
          val firstCol = batch.column(0)
          if (firstCol.isInstanceOf[SlicedGpuColumnVector]) {
            // We don't have control over ColumnarBatch to put in the slice, so we have to do it
            // for each column.  In this case we are using the first column.
            startRow = firstCol.asInstanceOf[SlicedGpuColumnVector].getStart
            for (i <- 0 until numColumns) {
              columns(i) = batch.column(i).asInstanceOf[SlicedGpuColumnVector].getBase
            }
          } else {
            for (i <- 0 until numColumns) {
              batch.column(i) match {
                case gpu: GpuColumnVector =>
                  val cpu = gpu.copyToHostAsync(Cuda.DEFAULT_STREAM)
                  toClose += cpu
                  columns(i) = cpu.getBase
                case cpu: RapidsHostColumnVector =>
                  columns(i) = cpu.getBase
              }
            }

            Cuda.DEFAULT_STREAM.sync()
          }

          NvtxRegistry.SERIALIZE_BATCH {
            val writeInput = WriteInput.builder
              .setColumns(columns)
              .setOutputStream(out)
              .setNumRows(numRows)
              .setRowOffset(startRow)
              .setMeasureCopyBufferTime(measureBufferCopyTime)
              .build
            val writeMetric = kudo
              .getOrElse(throw new IllegalStateException("Kudo serializer not initialized."))
              .writeToStreamWithMetrics(writeInput)

            dataSize += writeMetric.getWrittenBytes
            if (measureBufferCopyTime) {
              // These metrics will not show up in the UI if it's not modified
              serCopyBufferTime += writeMetric.getCopyBufferTime
            }
          }
        } else {
          NvtxRegistry.SERIALIZE_ROW_ONLY_BATCH {
            dataSize += KudoSerializer.writeRowCountToStream(out, numRows)
          }
        }
        this
      }
    }

    override def writeKey[T: ClassTag](key: T): SerializationStream = {
      // The key is only needed on the map side when computing partition ids. It does not need to
      // be shuffled.
      assert(null == key || key.isInstanceOf[Int])
      this
    }

    override def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def writeObject[T: ClassTag](t: T): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def flush(): Unit = {
      out.flush()
    }

    override def close(): Unit = {
      out.close()
    }
  }

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {
      private[this] val wrappedIn = new InputStreamWrapper(in, stalledByInputStream)
      private[this] val dIn: DataInputStream =
        new DataInputStream(new BufferedInputStream(wrappedIn))

      override def asKeyValueIterator: Iterator[(Int, ColumnarBatch)] = {
        new KudoSerializedBatchIterator(dIn, deserTime, taskSharedBuffer, metrics)
      }

      override def asIterator: Iterator[Any] = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def readKey[T]()(implicit classType: ClassTag[T]): T = {
        // We skipped serialization of the key in writeKey(), so just return a dummy value since
        // this is going to be discarded anyways.
        null.asInstanceOf[T]
      }

      override def readValue[T]()(implicit classType: ClassTag[T]): T = {
        // This method should never be called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def readObject[T]()(implicit classType: ClassTag[T]): T = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def close(): Unit = {
        dIn.close()
      }
    }
  }

  // These methods are never called by shuffle code.
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException
}

private class KudoGpuSerializerInstance(
    val metrics: Map[String, GpuMetric],
    val dataTypes: Array[DataType],
    val taskSharedBufferEnabled: Boolean,
    val taskSharedBufferTriggerSize: Int,
    val taskSharedBufferTargetTableCount: Int,
    val taskSharedBufferMaxSize: Int,
    val taskSharedBufferStripeCount: Int,
) extends SerializerInstance {
  private val dataSize = metrics(METRIC_DATA_SIZE)
  private val serTime = metrics(METRIC_SHUFFLE_SER_STREAM_TIME)
  private val deserTime = metrics(METRIC_SHUFFLE_DESER_STREAM_TIME)
  private val stalledByInputStream = metrics(METRIC_SHUFFLE_STALLED_BY_INPUT_STREAM)
  private val taskSharedBuffer = if (taskSharedBufferEnabled) {
    Some(new KudoTaskSharedHostBufferSet(metrics, taskSharedBufferTriggerSize,
      targetTableCount = taskSharedBufferTargetTableCount,
      maxBufferSize = taskSharedBufferMaxSize,
      stripeCount = taskSharedBufferStripeCount))
  } else {
    None
  }

  override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {

    override def writeValue[T: ClassTag](value: T): SerializationStream = serTime.ns {
      NvtxRegistry.GPU_KUDO_WRITE_BUFFERS {
        val batch = value.asInstanceOf[ColumnarBatch]
        if (batch.numCols() > 0) {
          val firstCol = batch.column(0)
          firstCol match {
            case vector: SlicedSerializedColumnVector =>
              val data = vector.getWrap
              val dataLen = data.getLength
              dataSize += dataLen
              var remaining = dataLen.toInt
              val temp = new Array[Byte](math.min(8192, remaining))
              var at = 0
              while (remaining > 0) {
                val read = math.min(remaining, temp.length)
                data.getBytes(temp, 0, at, read)
                out.write(temp, 0, read)
                at = at + read
                remaining = remaining - read
              }
              flush()
            case _ =>
          }
        } else {
          dataSize += KudoSerializer.writeRowCountToStream(out, batch.numRows())
        }
        this
      }
    }

    override def writeKey[T: ClassTag](key: T): SerializationStream = {
      // The key is only needed on the map side when computing partition ids. It does not need to
      // be shuffled.
      assert(null == key || key.isInstanceOf[Int])
      this
    }

    override def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def writeObject[T: ClassTag](t: T): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def flush(): Unit = {
      out.flush()
    }

    override def close(): Unit = {
      out.close()
    }
  }

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {
      private[this] val wrappedIn = new InputStreamWrapper(in, stalledByInputStream)
      private[this] val dIn: DataInputStream =
        new DataInputStream(new BufferedInputStream(wrappedIn))

      override def asKeyValueIterator: Iterator[(Int, ColumnarBatch)] = {
        new KudoSerializedBatchIterator(dIn, deserTime, taskSharedBuffer, metrics)
      }

      override def asIterator: Iterator[Any] = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def readKey[T]()(implicit classType: ClassTag[T]): T = {
        // We skipped serialization of the key in writeKey(), so just return a dummy value since
        // this is going to be discarded anyways.
        null.asInstanceOf[T]
      }

      override def readValue[T]()(implicit classType: ClassTag[T]): T = {
        // This method should never be called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def readObject[T]()(implicit classType: ClassTag[T]): T = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def close(): Unit = {
        dIn.close()
      }
    }
  }

  // These methods are never called by shuffle code.
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException
}

/**
 * A special `ColumnVector` that describes a serialized table read from shuffle using
 * [[KudoSerializer]].
 *
 * This appears in a `ColumnarBatch` to pass serialized tables to [[GpuShuffleCoalesceExec]]
 * which should always appear in the query plan immediately after a shuffle.
 */
case class KudoSerializedTableColumn(spillableKudoTable: SpillableKudoTable)
  extends GpuColumnVectorBase(NullType) with SizeProvider {
  override def close(): Unit = {
    if (spillableKudoTable != null) {
      spillableKudoTable.close()
    }
  }

  override def hasNull: Boolean = throw new IllegalStateException("should not be called")

  override def numNulls(): Int = throw new IllegalStateException("should not be called")

  override def sizeInBytes: Long = if (spillableKudoTable == null) {
    0L
  } else {
    spillableKudoTable.length
  }
}

object KudoSerializedTableColumn {
  /**
   * Build a `ColumnarBatch` consisting of a single [[KudoSerializedTableColumn]] describing
   * the specified serialized table.
   *
   * @param header the header of the kudo table
   * @param hostBuffer the buffer for the kudo table
   * @return columnar batch to be passed to [[GpuShuffleCoalesceExec]]
   */
  def from(header: KudoTableHeader, hostBuffer: HostMemoryBuffer): ColumnarBatch = {
    val kudoTable = SpillableKudoTable(header, hostBuffer)
    val column = new KudoSerializedTableColumn(kudoTable)
    new ColumnarBatch(Array(column), kudoTable.header.getNumRows)
  }

  /**
   * This is for debugging kudo Shuffle errors, e.g. numRows < 0 in kudo Shuffle read.
   * It is disabled by default for performance.
   */
  val SER_CHECK_SCHEMA: Boolean =
    java.lang.Boolean.getBoolean("ai.rapids.kudo.debug.ser.checkSchema")
}

class KudoSerializedBatchIterator(
    dIn: DataInputStream,
    deserTime: GpuMetric,
    taskSharedBuffer: Option[KudoTaskSharedHostBufferSet] = None,
    metrics: Map[String, GpuMetric] = Map.empty)
  extends BaseSerializedTableIterator {
  private[this] var nextHeader: Option[KudoTableHeader] = None
  private[this] var streamClosed: Boolean = false

  // When KudoSerializedBatchIterator is often handling small kudo tables, it is more efficient
  // to use a shared HostMemoryBuffer to avoid the overhead of allocating and deallocating, check
  // SparkResourceAdaptor.cpuDeallocate to understand the overhead.

  // Used to track the first 5 batch sizes and decide if to use a shared HostMemoryBuffer.
  // After seeing 5 batches, we estimate the shared buffer size based on average batch size.
  private[this] val firstBatchesSizes = new ArrayBuffer[Long](5)
  private[this] val sharedBufferSampleCount: Int = 5
  private[this] var sharedBuffer: Option[HostMemoryBuffer] = None
  private[this] val sharedBufferTriggerSize: Int = 1 << 20 // 1MB
  private[this] val sharedBufferMinSize: Int = 1 << 20 // 1MB minimum
  private[this] val sharedBufferMaxSize: Int = 20 << 20 // 20MB maximum
  private[this] var sharedBufferAllocatedSize: Int = 0 // actual allocated size (dynamic)
  private[this] var sharedBufferCurrentUse: Int = 0
  private[this] val headerReadTime = metrics.get(METRIC_SHUFFLE_KUDO_HEADER_READ_TIME)
  private[this] val headerReadCpuTime = metrics.get(METRIC_SHUFFLE_KUDO_HEADER_READ_CPU_TIME)
  private[this] val bufferAcquireTime = metrics.get(METRIC_SHUFFLE_KUDO_BUFFER_ACQUIRE_TIME)
  private[this] val bufferAcquireCpuTime =
    metrics.get(METRIC_SHUFFLE_KUDO_BUFFER_ACQUIRE_CPU_TIME)
  private[this] val payloadReadTime = metrics.get(METRIC_SHUFFLE_KUDO_PAYLOAD_READ_TIME)
  private[this] val payloadReadCpuTime = metrics.get(METRIC_SHUFFLE_KUDO_PAYLOAD_READ_CPU_TIME)
  private[this] val payloadReadBytes = metrics.get(METRIC_SHUFFLE_KUDO_PAYLOAD_READ_BYTES)

  private def measureWallAndCpu[T](wallMetric: Option[GpuMetric], cpuMetric: Option[GpuMetric])(
      operation: => T): T = {
    val wallStart = System.nanoTime()
    val cpuStart = ThreadCpuTime.now()
    try {
      operation
    } finally {
      wallMetric.foreach(_ += System.nanoTime() - wallStart)
      cpuMetric.foreach(_ += math.max(0L, ThreadCpuTime.now() - cpuStart))
    }
  }

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      Seq[AutoCloseable](
        () => dIn.close(),
        () => streamClosed = true,
        () => sharedBuffer.map(_.close())
      ).safeClose()
    }
  }

  override def peekNextBatchSize(): Option[Long] = deserTime.ns {
    if (streamClosed) {
      None
    } else {
      if (nextHeader.isEmpty) {
        NvtxRegistry.READ_HEADER {
          val header = measureWallAndCpu(headerReadTime, headerReadCpuTime) {
            Option(KudoTableHeader.readFrom(dIn).orElse(null))
          }
          if (header.isDefined) {
            nextHeader = header
          } else {
            dIn.close()
            sharedBuffer.foreach(_.close())
            sharedBuffer = None
            streamClosed = true
            nextHeader = None
          }
        }
      }
      nextHeader.map(_.getTotalDataLen)
    }
  }

  private def allocateHostWithRetry(size: Int): HostMemoryBuffer = {
    withRetryNoSplit[HostMemoryBuffer] {
      // This buffer will later be concatenated into another host buffer before being
      // sent to the GPU, so no need to use pinned memory for these buffers.
      HostMemoryBuffer.allocate(size, false)
    }
  }

  private def readNextBatch(): ColumnarBatch = deserTime.ns {
    NvtxRegistry.READ_BATCH {
      val header = nextHeader.get
      nextHeader = None
      if (header.getNumColumns > 0) {

        // When allocation fails, will roll back to the beginning of this withRetryNoSplit,
        // with previous batches saved in HostCoalesceIteratorBase.serializedTables.
        // The previous batches should be able to be spilled by itself.
        val buffer = measureWallAndCpu(bufferAcquireTime, bufferAcquireCpuTime) {
          taskSharedBuffer.map(_.acquire(header.getTotalDataLen)).getOrElse {
            if (sharedBuffer.isEmpty) {
              closeOnExcept(allocateHostWithRetry(header.getTotalDataLen)) { allocated =>

                if (firstBatchesSizes.length < sharedBufferSampleCount) {
                  firstBatchesSizes += header.getTotalDataLen
                  if (firstBatchesSizes.length == sharedBufferSampleCount) {
                    // After seeing enough samples, decide if to use a shared buffer.
                    val maxSize = firstBatchesSizes.max
                    if (maxSize < sharedBufferTriggerSize) {
                      // All sampled batches are small, use shared buffer with dynamic size.
                      // Estimate buffer size: 10 * average batch size, clamped to [1MB, 20MB].
                      val avgSize = firstBatchesSizes.sum / firstBatchesSizes.length
                      sharedBufferAllocatedSize = math.min(sharedBufferMaxSize,
                        math.max(sharedBufferMinSize, (avgSize * 10).toInt))
                      sharedBuffer = Some(allocateHostWithRetry(sharedBufferAllocatedSize))
                      sharedBufferCurrentUse = 0
                    }
                  }
                }

                allocated
              }
            } else {
              if (header.getTotalDataLen > sharedBufferAllocatedSize / 2) {
                // Too big to use shared buffer, this should rarely happen since
                // sampled batches were all much smaller.
                allocateHostWithRetry(header.getTotalDataLen)
              } else {
                if (sharedBufferCurrentUse + header.getTotalDataLen > sharedBufferAllocatedSize) {
                  // If not enough room left, we need to allocate a new shared buffer.
                  sharedBuffer.get.close()
                  sharedBufferCurrentUse = 0
                  sharedBuffer = Some(allocateHostWithRetry(sharedBufferAllocatedSize))
                }
                val ret = sharedBuffer.get.slice(sharedBufferCurrentUse,
                  header.getTotalDataLen)
                sharedBufferCurrentUse += header.getTotalDataLen
                ret
              }
            }
          }
        }

        closeOnExcept(buffer) { _ =>
          measureWallAndCpu(payloadReadTime, payloadReadCpuTime) {
            buffer.copyFromStream(0, dIn, header.getTotalDataLen)
          }
          payloadReadBytes.foreach(_ += header.getTotalDataLen)
          KudoSerializedTableColumn.from(header, buffer)
        }
      } else {
        KudoSerializedTableColumn.from(header, null)
      }
    }
  }

  override def hasNext: Boolean = {
    peekNextBatchSize()
    nextHeader.isDefined
  }

  override def next(): (Int, ColumnarBatch) = {
    if (!hasNext) {
      throw new NoSuchElementException("Walked off of the end...")
    }
    (0, readNextBatch())
  }
}
