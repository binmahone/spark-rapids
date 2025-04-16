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

import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}

/**
 * The definition of serializable message which can be easily transferred between frontend and
 * backend through JNIWrapper. Serializable messages follow the same preorder flattened layout:
 *    [(header), (field0_unit), (field1_unit), ...]
 *
 * The header length and the size of the field depends on the specific definitions. And usually
 * the first element of header stores the total length of the message, which can be used to verify
 * to the correctness of received message.
 */
trait SerializableMessage {
  val headerLength: Int
  val perFieldLength: Int
}

/**
 * The message serialized by the backend method `flushTargetVectors`, which carries the exact
 * memory usages for each target vector (including children vectors). It is called as "tail info",
 * because currently it is used to flush the stacking target vectors.
 *
 * Header Layout:
 *  1. ArrayLength
 *  2. Total sizeInBytes of stacking target vectors
 *
 * Field-wise Body Layout:
 *  1. the tail position of dataBuffer
 *  2. the tail position of offsetBuffer
 *  3. total row count
 *  4. total null count
 */
object TailInfo extends SerializableMessage {
  override val headerLength: Int = 2
  override val perFieldLength: Int = 4

  def flattenedPos(fieldIndex: Int, infoIndex: Int): Int = {
    fieldIndex * perFieldLength + headerLength + infoIndex
  }
}

/**
 * The message serialized by the backend method `collectMetrics`, which carries detailed metrics
 * over each vector and their potential children. The metrics are updated by the backend when
 * `appendBatch` occurs, which are mainly used for performance inspection.
 *
 * Header Layout:
 *  1. ArrayLength
 *  2. pre-convert time (in mircoSecond)
 *  3. (deprecated) char count time (in mircoSecond)
 *
 * Field-wise Body Layout:
 *  1. convert time (in mircoSecond)
 *  2. number of output batches
 *  3. number of output rows
 *  4. number of output size (in Bytes)
 *  5. number of null records
 *  6. number of unique records
 *  7. number of constant batches
 *  8. number of identity batches
 *  9. number of shuffle batches
 *  10. number of array-range batches
 */
object ConverterMetrics extends SerializableMessage {
  override val headerLength: Int = 3
  override val perFieldLength: Int = 10

  def dumpFieldMetrics(metrics: Array[Long], fieldOffset: Int): String = {
    val timeMs = metrics(fieldOffset) / 1000 // Millisecond
    val batches = metrics(fieldOffset + 1)
    val rows = metrics(fieldOffset + 2)
    val bytes = metrics(fieldOffset + 3) / 1024 // KB
    val numNulls = metrics(fieldOffset + 4)
    val rowsWithDict = metrics(fieldOffset + 5)
    val constBatches = metrics(fieldOffset + 6)
    val identityBatches = metrics(fieldOffset + 7)
    val rangeBatches = metrics(fieldOffset + 8)
    val dictBatches = metrics(fieldOffset + 9)

    s" ${timeMs}ms ${rows}rows ${bytes}KB ${batches}batches(C:$constBatches|I:$identityBatches|" +
      s"R:$rangeBatches|S:$dictBatches) ${numNulls}nullRows ${rowsWithDict}rowsWithDict"
  }
}


/**
 * Data class holds the decoded sample information of a specific field.
 * posInSchema means the position of this field in the preorder flattened schema.
 */
case class SampleColumnInfo(posInSchema: Int,
                            veloxType: VeloxDataType,
                            readType: StructField,
                            numRows: Int,
                            offsetsSize: Int,
                            dataSize: Int,
                            children: Seq[SampleColumnInfo])

/**
 * The message serialized by the backend method `encodeSampleInfo`, which carries buffer sizes
 * and other useful statistics for each target vector regarding to the sample batch on the deck.
 *
 * Header Layout:
 *  1. ArrayLength
 *  2. Total target sizeInBytes given the sample batch
 *
 * Field-wise Body Layout:
 *  1. nestRangeEnd: exclusive end of the fragment consists of this field and its sub-fields,
 *     which is used to build the links between parent vectors and their children.
 *  2. veloxTypeKind
 *  3. row count
 *  4. sizeInBytes of offsets buffer
 *  5. sizeInBytes of data buffer
 */
object SampleColumnMsg extends SerializableMessage {
  override val headerLength: Int = 2
  override val perFieldLength: Int = 5

  def deserialize(msg: Array[Long], schema: StructType): Array[SampleColumnInfo] = {
    require(msg(0) == msg.length, "The message header should be equal to the message length")
    require((msg.length - headerLength) % perFieldLength == 0, "The message is corrupted")

    case class DecodeHelper(var progress: Int,
                            head: Int,
                            bound: Int,
                            parent: DecodeHelper,
                            targetType: StructField,
                            children: mutable.Queue[SampleColumnInfo])

    val vectorSize = (msg.length - headerLength) / perFieldLength
    // TODO: Print for debug. Makes the LogLevel configurable from outside
    CoalesceBatchConverter.nativeMetaPrettyPrint("TRACE",
      "decodeSampleInfo", msg, headerLength, perFieldLength
    )

    val buildAllocInfo = (helper: DecodeHelper, children: Seq[SampleColumnInfo]) => {
      val offset = helper.head * perFieldLength + headerLength

      SampleColumnInfo(
        posInSchema = helper.head,
        veloxType = VeloxDataType.decodeVeloxType(msg(offset + 1).toInt),
        readType = helper.targetType,
        numRows = msg(offset + 2).toInt,
        offsetsSize = msg(offset + 3).toInt,
        dataSize = msg(offset + 4).toInt,
        children = children
      )
    }

    val stack = mutable.Stack[DecodeHelper]()
    val virtualRoot = DecodeHelper(0, -1, vectorSize, null,
      targetType = StructField("virtualRoot", schema, nullable = false),
      children = mutable.Queue.empty[SampleColumnInfo]
    )
    stack.push(virtualRoot)

    while (stack.nonEmpty) {
      val cursor = stack.top
      require(cursor.progress <= cursor.bound)
      if (cursor.progress == cursor.bound) {
        stack.pop()
        if (cursor.parent != null) {
          require(cursor.parent.progress < cursor.bound)
          cursor.parent.progress = cursor.bound
          cursor.parent.children.enqueue(buildAllocInfo(cursor, cursor.children.toSeq))
        }
      } else {
        val children = mutable.ArrayBuffer[DecodeHelper]()
        val childFields = mutable.Queue[StructField]()
        cursor.targetType.dataType match {
          case ArrayType(et, hasNull) =>
            childFields.enqueue(StructField("", et, hasNull))
          case MapType(kt, vt, hasNull) =>
            childFields.enqueue(StructField("", kt, nullable = false))
            childFields.enqueue(StructField("", vt, hasNull))
          case StructType(f) =>
            // enqueue an array
            childFields ++= f
        }
        var i = cursor.progress
        while (i < cursor.bound) {
          val rangeEnd = msg(i * perFieldLength + headerLength).toInt
          children += DecodeHelper(i + 1, i, rangeEnd, cursor,
            targetType = childFields.dequeue(),
            children = mutable.Queue.empty[SampleColumnInfo]
          )
          i = rangeEnd
        }
        // Reverse the childIterator to ensure children being handled in the original order.
        // Otherwise, the update of progress will NOT work.
        children.reverseIterator.foreach(stack.push)
      }
    }

    virtualRoot.children.toArray
  }
}

/**
 * TargetVectorsMsg holds the metadata of cuDF Vectors to be converted from corresponding Velox
 * Vectors, which are about to be passed to the native backend by the JNI method `resetTargetRef`.
 *
 * Header Layout:
 *  1. ArrayLength
 *
 * Field-wise Body Layout:
 *  1. TypeIndex: the index of Spark DataType encoded by VeloxDataTypes.encodeSparkType, keeping
 *     align to the backend enumeration SparkTypeKind defined in `ConversionBasics.h`
 *  2. The physical memoryAddress starts the DataBuffer
 *  3. The memory length of the DataBuffer
 *  4. The physical memoryAddress starts the nullBuffer
 *  5. The memory length of the nullBuffer
 *  6. The physical memoryAddress starts the offsetBuffer
 *  7. The memory length of the offsetBuffer
 *
 * NOTE: This helper methods of TargetVectorsMsg are NOT thread-safe. Please ensure they are NOT
 * called by multiple threads simultaneously.
 */
object TargetVectorsMsg extends SerializableMessage {
  override val headerLength: Int = 1
  override val perFieldLength: Int = 7

  // The status trackers should be ThreadLocal variables since the building of TargetVectorsMsg is
  // Task-Local instead of Global. (Multiple Spark Tasks might be doing this job simultaneously.
  // TODO: replace thread-local building with purely-local building based on independent instances
  private val fieldDeck: ThreadLocal[Array[Long]] = {
    ThreadLocal.withInitial(() => Array.ofDim[Long](7))
  }
  private val mask: ThreadLocal[Int] = ThreadLocal.withInitial(() => 0)

  // Flushes the fully filled field deck into target buffer
  def commitField(buffer: mutable.ArrayBuffer[Long]): Unit = {
    require(mask.get() == (1 | 2 | 4 | 8),
      s"current field has NOT been fully set: MASK_VAL(${mask.get()})"
    )
    buffer ++= fieldDeck.get()
    mask.set(0)
  }

  // Writes TypeIndex into the fieldDeck
  def setDataType(info: SampleColumnInfo): Unit = {
    fieldDeck.get()(0) = VeloxDataType.encodeSparkType(info.readType.dataType).toLong
    mask.set(mask.get() | 1)
  }

  // Writes localOffset and size of DataBuffer into the fieldDeck
  def setDataBuffer(offset: Long, size: Long): Unit = {
    fieldDeck.get()(1) = offset
    fieldDeck.get()(2) = size
    mask.set(mask.get() | 2)
  }

  // Marks DataBuffer missing
  def setMissingDataBuffer(): Unit = {
    fieldDeck.get()(1) = -1
    fieldDeck.get()(2) = 0
    mask.set(mask.get() | 2)
  }

  // Writes localOffset and size of NullBuffer into the fieldDeck
  def setNullBuffer(offset: Long, size: Long): Unit = {
    fieldDeck.get()(3) = offset
    fieldDeck.get()(4) = size
    mask.set(mask.get() | 4)
  }

  // Marks NullBuffer missing
  def setMissingNullBuffer(): Unit = {
    fieldDeck.get()(3) = -1
    fieldDeck.get()(4) = 0
    mask.set(mask.get() | 4)
  }

  // Writes localOffset and size of OffsetsBuffer into the fieldDeck
  def setOffsetsBuffer(offset: Long, size: Long): Unit = {
    fieldDeck.get()(5) = offset
    fieldDeck.get()(6) = size
    mask.set(mask.get() | 8)
  }

  // Marks OffsetsBuffer missing
  def setMissingOffsetsBuffer(): Unit = {
    fieldDeck.get()(5) = -1
    fieldDeck.get()(6) = 0
    mask.set(mask.get() | 8)
  }

  // Replaces local offsets with absolute memory addresses per field
  def localOffsetsToMemoryAddress(baseMemoryAddr: Long,
                                  buffer: mutable.ArrayBuffer[Long],
                                  start: Int,
                                  length: Int): Unit = {
    require(length % perFieldLength == 0,
      s"length($length) is not a multiple of Field Width($perFieldLength)")
    (start until start + length by perFieldLength).foreach { i =>
      // dataOffset -> dataAddr
      buffer(i + 1) = buffer(i + 1) match {
        case -1L => 0L
        case localOffset => localOffset + baseMemoryAddr
      }
      // nullOffset -> nullAddr
      buffer(i + 3) = buffer(i + 3) match {
        case -1L => 0L
        case localOffset => localOffset + baseMemoryAddr
      }
      // offsetOffset -> offsetsAddr
      buffer(i + 5) = buffer(i + 5) match {
        case -1L => 0L
        case localOffset => localOffset + baseMemoryAddr
      }
    }
  }
}
