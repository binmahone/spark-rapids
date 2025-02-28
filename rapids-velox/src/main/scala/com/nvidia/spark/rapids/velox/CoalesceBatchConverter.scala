/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.velox

import scala.collection.mutable

import ai.rapids.cudf.{DType, HostColumnVector, HostColumnVectorCore, HostMemoryBuffer, PinnedMemoryPool}
import ai.rapids.cudf.DType.DTypeEnum
import io.glutenproject.columnarbatch.IndicatorVector
import io.glutenproject.rapids.GlutenJniWrapper

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch


case class RapidsHostColumn(vector: HostColumnVector, usePinnedMemory: Boolean, totalBytes: Long)

private[velox] case class HostBufferInfo(buffer: HostMemoryBuffer, isPinned: Boolean)

/**
 * The helper class represents the pre-allocated HostColumnVector, which contains all logical
 * buffers(dataBuffer/nullBuffer/offsetBuffer) in a shared physical buffer(HostBufferInfo).
 *
 * The rootBufferInfo is only set for the top-level VectorBuilder, since the sub-level builders
 * share the same buffer with the top-level one.
 */
case class VectorBuilder(rootBufferInfo: Option[HostBufferInfo],
                         posInSchema: Int,
                         field: StructField,
                         nullBufOffset: Option[Long],
                         dataBufOffset: Option[Long],
                         offsetBufOffset: Option[Long],
                         children: Seq[VectorBuilder]) {

  // Builds HostColumnVector (with its children) recursively. After clipping the vectors, returns
  // the total sizeInBytes of the clipped vectors (including children's size).
  def build(tailInfo: Array[Long],
            sharedBuffer: HostMemoryBuffer): (HostColumnVectorCore, Long) = {

    var finalTotalBytes = 0L
    var childVecs = new java.util.ArrayList[HostColumnVectorCore]()
    children.foreach { b =>
      val (childVec, childTotalSize)  = b.build(tailInfo, sharedBuffer)
      childVecs.add(childVec)
      finalTotalBytes += childTotalSize
    }

    val dType = CoalesceBatchConverter.mapSparkTypeToDType(field.dataType)
    val rowCount = tailInfo(TailInfo.flattenedPos(posInSchema, 2))
    val nullCount = java.util.Optional.of(java.lang.Long.valueOf(
      tailInfo(TailInfo.flattenedPos(posInSchema, 3))))
    // Clips the pre-allocated buffers with the actual used sizes
    val dataBuffer = dataBufOffset.map { offset =>
      val finalLength = tailInfo(TailInfo.flattenedPos(posInSchema, 0))
      finalTotalBytes += finalLength
      sharedBuffer.slice(offset, finalLength)
    }
    val offsetBuffer = offsetBufOffset.map { offset =>
      val finalLength = tailInfo(TailInfo.flattenedPos(posInSchema, 1))
      finalTotalBytes += finalLength
      sharedBuffer.slice(offset, finalLength)
    }
    val nullBuffer = nullBufOffset.map { offset =>
      val finalLength = CoalesceBatchConverter.sizeOfNullMask(rowCount.toInt)
      finalTotalBytes += finalLength
      sharedBuffer.slice(offset, finalLength)
    }

    // Cast Map[child0, child1] => List[Struct[child0, child1]]
    if (field.dataType.isInstanceOf[MapType]) {
      val structCol = new HostColumnVectorCore(DType.STRUCT,
        childVecs.get(0).getRowCount, java.util.Optional.of(0L),
        null, null, null, childVecs)
      childVecs = new java.util.ArrayList[HostColumnVectorCore]()
      childVecs.add(structCol)
    }

    val vector: HostColumnVectorCore = if (rootBufferInfo.nonEmpty) {
      new HostColumnVector(dType, rowCount, nullCount,
        dataBuffer.orNull, nullBuffer.orNull, offsetBuffer.orNull,
        childVecs)
    } else {
      new HostColumnVectorCore(dType, rowCount, nullCount,
        dataBuffer.orNull, nullBuffer.orNull, offsetBuffer.orNull,
        childVecs)
    }
    (vector, finalTotalBytes)
  }
}

object VectorBuilder {
  // Builds RapidsHostColumn for each top-level field from corresponding top-level VectorBuilders
  def buildRapidsHostColumn(tailInfo: Array[Long],
                            rootBuilder: VectorBuilder): RapidsHostColumn = {
    require(rootBuilder.rootBufferInfo.nonEmpty, "NOT a root builder")

    val rootBuf = rootBuilder.rootBufferInfo.get
    try {
      val (vector, actualTotalBytes) = rootBuilder.build(tailInfo, rootBuf.buffer)
      RapidsHostColumn(vector.asInstanceOf[HostColumnVector], rootBuf.isPinned, actualTotalBytes)
    } finally {
      // Close the shared root buffer after all child buffers have been built
      rootBuf.buffer.close()
    }
  }
}

/**
 * The frontend of Coalesce C2C Converter, talking to the backend through RapidsVeloxJniWrapper,
 * holds the ownership of the backend counterpart with the nativeHandle.
 *
 * 4 major methods proceed the lifecycle of C2C Converter:
 *   -> tryAppendBatch
 *   -> setupTargetVectors
 *   -> flush
 *   -> close
 */
class CoalesceBatchConverter(runtime: GlutenJniWrapper,
                             nativeHandle: Long,
                             schema: StructType,
                             targetBatchSize: Long,
                             metrics: Map[String, SQLMetric]) extends Logging {

  private val columnBuilders = mutable.ArrayBuffer[VectorBuilder]()

  // The shadow variable track the status of source batch deck in the backend
  private var deckFilled = true

  private var eclipsed: Long = 0L

  // initialize the coalesce converter, creating TargetBuffers for the first target batch
  {
    setupTargetVectors()
  }

  /**
   * Checks whether the source batch deck is filled or not
   */
  def isDeckFilled: Boolean = deckFilled

  def hasProceedingBuilders: Boolean = columnBuilders.nonEmpty

  def eclipsedNanoSecond: Long = eclipsed

  /**
   * Checks and safely closes the native backend. Meanwhile, collects the aggregated metrics
   * of this converter during its lifecycle.
   */
  def close(): String = {
    require(!deckFilled, "The deck is NOT empty")
    require(columnBuilders.isEmpty, "Please flush existing ColumnBuilders at first")
    // We will get the final metrics for each column (and subColumn) before cleaning up.
    // Then, we will beautify and dump the metrics for performance inspection
    val nativeMetrics = runtime.closeCoalesceConverter(nativeHandle)
    CoalesceBatchConverter.dumpMetrics(nativeMetrics, schema)
  }

  /**
   * The main entry to enroll the next input (Gluten) batch
   *
   * Behind the scene, the backend will make the input batch conversion-ready (building up
   * corresponding PreparedVectors) firstly.
   * Then, puts the PreparedVectors onto the deck and returns false if target vectors do NOT have
   * enough space remaining for this batch. Otherwise, performs the appending-only conversion and
   * returns true (the deck is empty).
   *
   * Please ensure the deck is empty before calling this method. And the input batch is consumed
   * by Coalesce C2C Converter whatever despite the method returns true or false.
   */
  def tryAppendBatch(cb: ColumnarBatch): Boolean = {
    require(!deckFilled, "The deck is NOT empty")
    val start: Long = System.nanoTime()

    val handle = CoalesceBatchConverter.getNativeBatchHandle(cb)
    val ret = if (runtime.appendBatch(nativeHandle, handle)) {
      true
    } else {
      deckFilled = true
      false
    }

    eclipsed += System.nanoTime() - start
    ret
  }

  /**
   * Pre-allocates target vectors for the next output batch. And update TargetVectorInfo for the
   * backend converter.
   */
  def setupTargetVectors(): Unit = {
    require(columnBuilders.isEmpty, "Please flush existing ColumnBuilders at first")
    require(deckFilled, "There is NO sample batch which should be on the deck")
    val start: Long = System.nanoTime()

    // Collect the size distribution of buffers from the sample batch
    val sampleMsg = runtime.encodeSampleInfo(nativeHandle)
    // Estimate the capacity of targetBatchSize in the number of source batch
    val estimatedBatchNum: Double = targetBatchSize.toDouble / sampleMsg(1)
    // Deserialize the message of sample distribution, getting SampleInfo for each top-level fields
    val sampleInfo = SampleColumnMsg.deserialize(sampleMsg, schema)

    // Create ColumnBuilders in the meantime encoding the bufferPtrs
    val bufferPtrs = mutable.ArrayBuffer[Long]()
    bufferPtrs.append(0L)
    sampleInfo.foreach { topLevelInfo: SampleColumnInfo =>
      columnBuilders += createVectorBuilder(
        bufferPtrs,
        estimatedBatchNum,
        topLevelInfo
      )
    }
    bufferPtrs(0) = bufferPtrs.length

    // reset the native reference of target Buffers.
    // NOTE: The method will consume the sample batch on the deck. So, the caller can
    // run `tryAppendBatch` right after this method.
    runtime.resetTargetRef(nativeHandle, bufferPtrs.toArray)
    deckFilled = false

    eclipsed += System.nanoTime() - start
  }

  /**
   * Flushes the stacking target vectors.
   *
   * To be specific, finalizes VectorBuilders to create corresponding HostColumnVectors. During
   * the finalization, logical buffers will be truncated according to TailInfo. And group up these
   * clipped (logical) buffers to form the desired HostColumnVectors.
   */
  def flush(): Array[RapidsHostColumn] = {
    require(columnBuilders.nonEmpty, "ColumnBuilders has NOT been setup")
    val start: Long = System.nanoTime()
    // Fetches serialized TailInfo from the backend through JNI
    val tailInfo = runtime.flush(nativeHandle)
    metrics("C2COutputSize") += tailInfo(1)
    // TODO: Print for debug. Makes the LogLevel configurable from outside
    CoalesceBatchConverter.nativeMetaPrettyPrint("TRACE",
      "TailInfoFlush", tailInfo, TailInfo.headerLength, TailInfo.perFieldLength
    )
    // Finalizes VectorBuilders with TailInfo
    val ret = columnBuilders.map(rootBuilder =>
      VectorBuilder.buildRapidsHostColumn(tailInfo, rootBuilder)
    )
    columnBuilders.clear()

    eclipsed += System.nanoTime() - start
    ret.toArray
  }

  /**
   * Pre-allocates a top-level target ColumnVector (and sub-vectors) for the next output batch.
   * The memory sizes of pre-allocated buffers are determined by the actual size of the sampled
   * batch and estimatedBatchNum of the next output batch.
   *
   * Instead of allocating memory for each buffer, allocating a united memory buffer which can be
   * shared by all buffers (including buffers for nested children). With this approach, we can
   * easily distinguish if a (top-level) field based on PinnedMemory or PageableMemory. And setup
   * different metrics specialized for PinnedMemory_H2D and PageableMemory_H2D.
   */
  private def createVectorBuilder(bufferPtrs: mutable.ArrayBuffer[Long],
                                  estimatedBatchNum: Double,
                                  rootInfo: SampleColumnInfo): VectorBuilder = {
    // This method is used to compute the local offsets for each logical buffer recursively.
    def impl(localOffset: Long, info: SampleColumnInfo): (VectorBuilder, Long) = {
      require(info.veloxType.canConvert(info.readType.dataType),
        s"can NOT convert ${info.veloxType} to ${info.readType.dataType}")

      var offset = localOffset

      // 1. push the TypeIndex into the fieldDeck
      TargetVectorsMsg.setDataType(info)
      // 2. figure out the offset and length for dataBuffer
      val dataOffset: Option[Long] = if (info.dataSize > 0) {
        val estDataSize = (info.dataSize * estimatedBatchNum).toLong
        TargetVectorsMsg.setDataBuffer(offset, estDataSize)
        offset += estDataSize
        Some(offset - estDataSize)
      } else {
        TargetVectorsMsg.setMissingDataBuffer()
        None
      }
      // 3. figure out the offset and length for nullBuffer
      val nullOffset: Option[Long] = if (info.readType.nullable) {
        val estimatedRows = (info.numRows * estimatedBatchNum).toInt
        val estNullMaskBytes = CoalesceBatchConverter.sizeOfNullMask(estimatedRows).toLong
        TargetVectorsMsg.setNullBuffer(offset, estNullMaskBytes)
        offset += estNullMaskBytes
        Some(offset - estNullMaskBytes)
      } else {
        TargetVectorsMsg.setMissingNullBuffer()
        None
      }
      // 4. figure out the offset and length for offsetsBuffer
      val offsetOffset: Option[Long] = if (info.offsetsSize > 0) {
        val estOffsetSize = (info.offsetsSize * estimatedBatchNum).toLong
        TargetVectorsMsg.setOffsetsBuffer(offset, estOffsetSize)
        offset += estOffsetSize
        Some(offset - estOffsetSize)
      } else {
        TargetVectorsMsg.setMissingOffsetsBuffer()
        None
      }
      // 5. Finally commit this field
      TargetVectorsMsg.commitField(bufferPtrs)

      // recursively traverses children in the manner of preorder
      val childBuilders = info.children.map { ch =>
        val (builder, newOffset) = impl(offset, ch)
        offset = newOffset
        builder
      }

      // Constructs the VectorBuilder with all local offsets regarding to the shared rootBuffer
      // to be allocated afterwards.
      val curBuilder = VectorBuilder(None, info.posInSchema, info.readType,
        nullOffset, dataOffset, offsetOffset, childBuilders
      )
      (curBuilder, offset)
    }

    // Record the start point of bufferPtrs
    val bufferPtrsStart = bufferPtrs.length
    // Create non-root builders while computing the total size
    val (tmpRootBuilder, totalBytes) = impl(0, rootInfo)

    // Allocates the united RootBuffer shared by all logical buffers.
    // Firstly try allocate from PinnedMemoryPool. Fallback to PageableMemory if failed.
    val bufferInfo = PinnedMemoryPool.tryAllocate(totalBytes) match {
      case buf if buf == null =>
        HostBufferInfo(HostMemoryBuffer.allocate(totalBytes, false), isPinned = false)
      case buf =>
        HostBufferInfo(buf, isPinned = true)
    }

    // Rebasing memory offsets for all (children) vectors with the memory address of shared buffer
    TargetVectorsMsg.localOffsetsToMemoryAddress(
      bufferInfo.buffer.getAddress,
      bufferPtrs,
      bufferPtrsStart,
      bufferPtrs.length
    )

    VectorBuilder(Some(bufferInfo),
      tmpRootBuilder.posInSchema,
      tmpRootBuilder.field,
      tmpRootBuilder.nullBufOffset,
      tmpRootBuilder.dataBufOffset,
      tmpRootBuilder.offsetBufOffset,
      tmpRootBuilder.children
    )
  }
}

object CoalesceBatchConverter extends Logging {

  def apply(firstBatch: ColumnarBatch,
            targetBatchSize: Long,
            schema: StructType,
            metrics: Map[String, SQLMetric]): CoalesceBatchConverter = {
    // Serialize Nullable Info of each field to create the backend part of converter. Nullable
    // Info is PlanTime metadata which cannot be accessed directly in the backend.
    val nullableInfo = CoalesceBatchConverter.encodeNullableInfo(schema)
    logDebug(s"CoalesceBatchConverter nullableInfo: ${nullableInfo.mkString(" | ")}")

    // Build the backend part of converter through RapidsVeloxJniWrapper
    val firstHandle = getNativeBatchHandle(firstBatch)
    val runtime = VeloxBackendApis.getRuntime.getOrElse(
      throw new AssertionError("VeloxBackendApis has NOT been initialized"))
    val handle = runtime.buildCoalesceConverter(firstHandle, nullableInfo)

    new CoalesceBatchConverter(runtime, handle, schema, targetBatchSize, metrics)
  }

  private def getNativeBatchHandle(cb: ColumnarBatch): Long = {
    cb.column(0) match {
      case indicator: IndicatorVector =>
        indicator.handle()
      case cv =>
        throw new IllegalArgumentException(
          s"Expecting IndicatorVector, but got ${cv.getClass}")
    }
  }

  /**
   * Dump detailed metrics for each field and sub-field recursively. The metrics are accumulated
   * statistics over all processed data. (The contents is recorded along with the definition of
   * VectorBuilder.FIELD_METRIC_STRIDE.) The metrics are mainly for the performance inspection on
   * each field and sub-field.
   *
   * The metrics is produced by the native method CoalesceBatchConverter::collectMetrics, so it is
   * encoded as a flat array for the transfer.
   */
  private def dumpMetrics(metrics: Array[Long], schema: StructType): String = {
    val alignment = "****"
    val builder = new mutable.StringBuilder()
    // Process preparation time
    builder.append(s"pre-convert time ${metrics(1) / 1000}ms\n")

    // Traverse the pre-order flattened schema to decode the "flattened metrics"
    val stack = mutable.Stack[(StructField, Int)]()
    schema.fields.reverseIterator.foreach(f => stack.push(f -> 1))
    var offset = ConverterMetrics.headerLength
    while (stack.nonEmpty) {
      val (f, depth) = stack.pop()
      // Use the prefix alignment of different length to distinguish field with their sub-fields.
      (1 to depth).foreach(_ => builder.append(alignment))
      // Process current field
      builder
          .append(' ')
          .append(f.toString())
          .append(ConverterMetrics.dumpFieldMetrics(metrics, offset))
          .append('\n')
      offset += ConverterMetrics.perFieldLength
      // Process sub-fields of current field if there exists.
      f.dataType match {
        case at: ArrayType =>
          val elem = StructField(f.name + "_elem", at.elementType, at.containsNull)
          stack.push(elem -> (depth + 1))
        case mt: MapType =>
          val valF = StructField(f.name + "_val", mt.valueType, mt.valueContainsNull)
          stack.push(valF -> (depth + 1))
          val keyF = StructField(f.name + "_key", mt.keyType, nullable = false)
          stack.push(keyF -> (depth + 1))
        case st: StructType =>
          st.fields.reverseIterator.foreach(ch => stack.push(ch -> (depth + 1)))
        case _ =>
      }
    }

    builder.toString()
  }

  // Utility method prints out serialized data (from/to backend) readably for Debugging
  def nativeMetaPrettyPrint(loggingLevel: String,
                            title: String,
                            array: Array[Long],
                            offset: Int,
                            step: Int): Unit = {
    lazy val message: String = {
      val sb = new mutable.StringBuilder()
      sb.append(title).append('\n')
      sb.append("==HEAD== ").append((0 until offset).map(array).mkString(" | ")).append('\n')
      (offset until array.length by step).foreach { i =>
        sb.append(s"  (${(i - offset) / step + 1}) ")
        sb.append((i until i + step).map(array).mkString(" | "))
        sb.append('\n')
      }
      sb.toString()
    }
    // The message will only be materialized when the logger decides to print out it.
    loggingLevel match {
      case "ERROR" => logError(message)
      case "WARNING" => logWarning(message)
      case "INFO" => logInfo(message)
      case "DEBUG" => logDebug(message)
      case "TRACE" => logTrace(message)
      case level =>
        throw new IllegalArgumentException(s"Illegal LoggingLevel $level")
    }
  }

  // Uses the same approach as ColumnView.getValidityBufferSize
  def sizeOfNullMask(rowNum: Int): Int = {
    val actualBytes = (rowNum + 7) >> 3
    ((actualBytes + 63) >> 6) << 6
  }

  // Since we cannot depend spark-rapids here, rebuild a simpler mapping from SparkType to DType
  def mapSparkTypeToDType(dt: DataType): DType = dt match {
    case _: BooleanType => DType.BOOL8
    case _: ByteType => DType.INT8
    case _: ShortType => DType.INT16
    case _: IntegerType => DType.INT32
    case _: LongType => DType.INT64
    case _: FloatType => DType.FLOAT32
    case _: DoubleType => DType.FLOAT64
    case _: StringType => DType.STRING
    case _: DateType => DType.TIMESTAMP_DAYS
    case _: TimestampType => DType.TIMESTAMP_MICROSECONDS
    case _: ArrayType => DType.LIST
    case _: MapType => DType.LIST
    case _: StructType => DType.STRUCT
    case d: DecimalType if DecimalType.is32BitDecimalType(d) =>
      DType.create(DTypeEnum.DECIMAL32, -d.scale)
    case d: DecimalType if DecimalType.is64BitDecimalType(d) =>
      DType.create(DTypeEnum.DECIMAL64, -d.scale)
    case d: DecimalType =>
      DType.create(DTypeEnum.DECIMAL128, -d.scale)
    case dt => throw new IllegalArgumentException(s"unexpected $dt")
  }

  // Runs preorder traversal on the table schema to extract the nullable info of each StructField.
  private def encodeNullableInfo(root: StructType): Array[Int] = {
    val flattened = mutable.ArrayBuffer.empty[Int]
    val stack = mutable.Stack[StructField]()
    root.reverseIterator.foreach(stack.push)
    while (stack.nonEmpty) {
      val field = stack.pop()
      flattened += (if (field.nullable) 1 else 0)
      field.dataType match {
        case at: ArrayType =>
          stack.push(StructField("ArrayElem", at.elementType, nullable = at.containsNull))
        case mt: MapType =>
          stack.push(StructField("MapValue", mt.valueType, nullable = mt.valueContainsNull))
          stack.push(StructField("MapKey", mt.keyType, nullable = false))
        case st: StructType =>
          st.reverseIterator.foreach(stack.push)
        case _ =>
      }
    }
    flattened.toArray
  }

}
