/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import java.math.RoundingMode
import java.util.{Collections, Optional}

import ai.rapids.cudf.{ColumnVector, Cuda, DType, HostColumnVector, HostMemoryBuffer, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.jni.kudo.DumpOption
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{DecimalType, DoubleType, IntegerType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch


class GpuShuffleCoalesceSuite extends AnyFunSuite with BeforeAndAfterEach {
  var rapidsConf = new RapidsConf(Map[String, String]())

  private def buildBatch(): ColumnarBatch = {
    withResource(new Table.TestBuilder()
        .column(5, null.asInstanceOf[java.lang.Integer], 3, 1, 1, 1, 1, 1, 1, 1)
        .column("five", "two", null, null, "one", "one", "one", "one", "one", "one")
        .column(5.0, 2.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
        .decimal64Column(-3, RoundingMode.UNNECESSARY ,
          5.1, null, 3.3, 4.4e2, 0, -2.1e-1, 1.111, 2.345, null, 1.23e3)
        .build()) { table =>
      GpuColumnVector.from(table, Array(IntegerType, StringType, DoubleType,
        DecimalType(DType.DECIMAL64_MAX_PRECISION, 3)))
    }
  }

  private def buildWideStringBatch(): ColumnarBatch = {
    withResource(HostColumnVector.fromStrings("abc", null, "def", "")) { narrow =>
      val offsets = HostMemoryBuffer.allocate(40, false)
      Seq(0L, 3L, 3L, 6L, 6L).zipWithIndex.foreach { case (end, i) =>
        offsets.setLong(i * 8L, end)
      }
      narrow.getData.incRefCount()
      narrow.getValidity.incRefCount()
      withResource(new HostColumnVector(DType.STRING, 4, Optional.of[java.lang.Long](1L),
          narrow.getData, narrow.getValidity, offsets, Collections.emptyList(), DType.INT64)) {
        wide =>
          withResource(wide.copyToDevice()) { column =>
            assertResult(DType.INT64)(column.getOffsetsType)
            withResource(new Table(column)) { table =>
              GpuColumnVector.from(table, Array(StringType))
            }
          }
      }
    }
  }

  /**
   * Retrieves the underlying column vectors for a batch. It increments the reference counts for
   * them if needed so the results need to be closed.
   */
  private def extractColumnVectors(batch: ColumnarBatch): Array[ColumnVector] = {
    if (TestUtils.isBatchPacked(batch)) {
      val packedColumn = batch.column(0).asInstanceOf[GpuPackedTableColumn]
      val table = packedColumn.getContiguousTable.getTable
      // The contiguous table is still responsible for closing these columns.
      (0 until table.getNumberOfColumns).map(i => table.getColumn(i).incRefCount()).toArray
    } else if (GpuCompressedColumnVector.isBatchCompressed(batch)) {
      val compressedColumn = batch.column(0).asInstanceOf[GpuCompressedColumnVector]
      val descr = compressedColumn.getTableMeta.bufferMeta.codecBufferDescrs(0)
      val codec = TableCompressionCodec.getCodec(
        descr.codec, TableCompressionCodec.makeCodecConfig(rapidsConf))
      withResource(codec.createBatchDecompressor(100 * 1024 * 1024L,
        Cuda.DEFAULT_STREAM)) { decompressor =>
        compressedColumn.getTableBuffer.incRefCount()
        decompressor.addBufferToDecompress(compressedColumn.getTableBuffer,
          compressedColumn.getTableMeta.bufferMeta)
        withResource(decompressor.finishAsync()) { outputBuffers =>
          val outputBuffer = outputBuffers.head
          // There should be only one
          withResource(
            MetaUtils.getTableFromMeta(outputBuffer, compressedColumn.getTableMeta)) { table =>
            (0 until table.getNumberOfColumns).map(i => table.getColumn(i).incRefCount()).toArray
          }
        }
      }
    } else {
      GpuColumnVector.extractBases(batch).map(_.incRefCount())
    }
  }

  private def compareBatches(expected: ColumnarBatch, actual: ColumnarBatch): Unit = {
    assertResult(expected.numRows)(actual.numRows)
    withResource(extractColumnVectors(expected)) { expectedColumns =>
      withResource(extractColumnVectors(actual)) { actualColumns =>
        assertResult(expectedColumns.length)(actualColumns.length)
        expectedColumns.zip(actualColumns).foreach { case (expected, actual) =>
          if (expected.getRowCount == 0) {
            assertResult(expected.getType)(actual.getType)
          } else {
            withResource(expected.equalToNullAware(actual)) { compareVector =>
              withResource(compareVector.all()) { compareResult =>
                assert(compareResult.getBoolean)
              }
            }
          }
        }
      }
    }
  }

  private def createSparkConf(): SparkConf = {
    val conf = new SparkConf()
        .set(RapidsConf.SHUFFLE_COMPRESSION_CODEC.key, "none")
        .set(RapidsConf.SHUFFLE_KUDO_WRITE_MODE.key, "GPU")
        .set(RapidsConf.SHUFFLE_KUDO_READ_MODE.key, "GPU")
    Seq(
      "rapids.test.gpu.allocFraction" -> RapidsConf.RMM_ALLOC_FRACTION.key,
      "rapids.test.gpu.minAllocFraction" -> RapidsConf.RMM_ALLOC_MIN_FRACTION.key,
      "rapids.test.gpu.maxAllocFraction" -> RapidsConf.RMM_ALLOC_MAX_FRACTION.key
    ).foreach { case (property, key) =>
      Option(System.getProperty(property)).foreach(value => conf.set(key, value))
    }
    conf
  }

  private def createGpuPartitioning(partitionIndices: Array[Int]): GpuPartitioning = {
    new GpuPartitioning {
      override val numPartitions: Int = partitionIndices.length
    }
  }

  private def serializeBatchesToStream(
      serializedPartitions: Array[ColumnarBatch],
      dataTypes: Array[org.apache.spark.sql.types.DataType]): Array[Byte] = {
    val byteOutputStream = new java.io.ByteArrayOutputStream()
    val serializerMetrics = Map(
      "dataSize" -> com.nvidia.spark.rapids.NoopMetric,
      "shuffleSerTime" -> com.nvidia.spark.rapids.NoopMetric,
      "shuffleSerCopyBufferTime" -> com.nvidia.spark.rapids.NoopMetric
    ).withDefaultValue(com.nvidia.spark.rapids.NoopMetric)
    val serializer = new GpuColumnarBatchSerializer(serializerMetrics, dataTypes,
      RapidsConf.ShuffleKudoMode.GPU, true, false)
    withResource(serializer.newInstance().serializeStream(byteOutputStream)) {
      serializationStream =>
        serializedPartitions.foreach { batch =>
          serializationStream.writeKey(0)
          serializationStream.writeValue(batch)
        }
    }
    byteOutputStream.toByteArray
  }

  private def deserializeStreamToIterator(
      serializedData: Array[Byte],
      dataTypes: Array[org.apache.spark.sql.types.DataType]):
      Iterator[ColumnarBatch] = {
    val serializerMetrics = Map(
      "dataSize" -> com.nvidia.spark.rapids.NoopMetric,
      "shuffleSerTime" -> com.nvidia.spark.rapids.NoopMetric,
      "shuffleSerCopyBufferTime" -> com.nvidia.spark.rapids.NoopMetric
    ).withDefaultValue(com.nvidia.spark.rapids.NoopMetric)
    val serializer = new GpuColumnarBatchSerializer(serializerMetrics, dataTypes,
      RapidsConf.ShuffleKudoMode.GPU, true, false)
    val byteInputStream = new java.io.ByteArrayInputStream(serializedData)
    val deserializationStream = serializer.newInstance().deserializeStream(byteInputStream)
    deserializationStream.asKeyValueIterator.map(_._2).asInstanceOf[Iterator[ColumnarBatch]]
  }

  private def createMetricsMap(): Map[String, GpuMetric] = {
    Map(
      GpuMetric.CONCAT_TIME -> com.nvidia.spark.rapids.NoopMetric,
      GpuMetric.NUM_INPUT_BATCHES -> com.nvidia.spark.rapids.NoopMetric,
      GpuMetric.NUM_INPUT_ROWS -> com.nvidia.spark.rapids.NoopMetric,
      GpuMetric.NUM_OUTPUT_BATCHES -> com.nvidia.spark.rapids.NoopMetric,
      GpuMetric.NUM_OUTPUT_ROWS -> com.nvidia.spark.rapids.NoopMetric,
      GpuMetric.OP_TIME_LEGACY -> com.nvidia.spark.rapids.NoopMetric,
      GpuMetric.READ_THROTTLING_TIME -> com.nvidia.spark.rapids.NoopMetric,
      GpuMetric.SYNC_READ_TIME -> com.nvidia.spark.rapids.NoopMetric
    )
  }

  private def createReadOption(): CoalesceReadOption = {
    CoalesceReadOption(
      kudoEnabled = true,
      kudoMode = RapidsConf.ShuffleKudoMode.GPU,
      kudoDebugMode = DumpOption.Never,
      kudoDebugDumpPrefix = None,
      useAsync = false
    )
  }

  private def verifyAndCompareResults(
      deserializedBatches: Array[ColumnarBatch],
      originalBatch: ColumnarBatch,
      dataTypes: Array[org.apache.spark.sql.types.DataType],
      minBatchesExpected: Int = 1): Unit = {
    val totalDeserializedRows = deserializedBatches.map(_.numRows).sum
    assertResult(originalBatch.numRows)(totalDeserializedRows)
    assert(deserializedBatches.length >= minBatchesExpected,
      s"Should have at least $minBatchesExpected deserialized batch(es)")
    val concatenatedBatch = ConcatAndConsumeAll.buildNonEmptyBatchFromTypes(
      deserializedBatches, dataTypes)
    withResource(concatenatedBatch) { _ =>
      compareBatches(originalBatch, concatenatedBatch)
    }
  }

  private def runKudoShuffleTest(
      targetBatchSize: Long,
      minSplitSize: Option[Long] = None,
      forceSplitRetry: Boolean = false,
      wideStrings: Boolean = false): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = createSparkConf()
    TestUtils.withGpuSparkSession(conf) { _ =>
      GpuShuffleEnv.init(new RapidsConf(conf))
      val partitionIndices = Array(0, 2, 2)
      val gp = createGpuPartitioning(partitionIndices)
      withResource(if (wideStrings) buildWideStringBatch() else buildBatch()) { originalBatch =>
        // incRefCounts here so we can hold onto the original batch and use it
        // below to compare the values to the coalesced results
        GpuColumnVector.incRefCounts(originalBatch)
        val columns = GpuColumnVector.extractColumns(originalBatch)
        val numRows = originalBatch.numRows
        val dataTypes = GpuColumnVector.extractTypes(originalBatch)

        // Get serialized batches from the partitioning operation
        withResource(gp.sliceInternalGpuOrCpuAndClose(
          numRows, partitionIndices, columns).map(_._1)) { serializedPartitions =>

          assert(serializedPartitions.forall(
            _.column(0).isInstanceOf[SlicedSerializedColumnVector]))

          // Simulate the full shuffle round-trip: serialize to stream, then deserialize back
          val serializedData = serializeBatchesToStream(serializedPartitions, dataTypes)
          val kudoBatchesIter = deserializeStreamToIterator(serializedData, dataTypes)

          // Set up metrics and read options
          val metricsMap = createMetricsMap()
          val readOption = createReadOption()

          // Get the coalescing iterator
          val coalesceIter = minSplitSize match {
            case Some(minSize) =>
              GpuShuffleCoalesceUtils.getGpuShuffleCoalesceIterator(
                kudoBatchesIter, targetBatchSize, dataTypes, readOption, metricsMap,
                prefetchFirstBatch = false, minSplitSize = minSize)
            case None =>
              GpuShuffleCoalesceUtils.getGpuShuffleCoalesceIterator(
                kudoBatchesIter, targetBatchSize, dataTypes, readOption, metricsMap)
          }

          // Force split retry if requested
          if (forceSplitRetry) {
            RmmSpark.currentThreadIsDedicatedToTask(1)
            RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
              RmmSpark.OomInjectionType.GPU.ordinal, 0)
          }

          // Collect all deserialized batches
          val deserializedBatches = coalesceIter.toArray

          // Verify results
          verifyAndCompareResults(deserializedBatches, originalBatch, dataTypes)

          if (forceSplitRetry) {
            val retryCount = RmmSpark.getAndResetNumSplitRetryThrow(1)
            assert(retryCount > 0,
              s"Expected at least one split retry, but saw $retryCount retries")
          }
        }
      }
    }
  }

  test("project size estimation counts the actual wide STRING offsets") {
    withResource(buildWideStringBatch()) { batch =>
      val reference = GpuBoundReference(0, StringType, true)(ExprId(0), "string")
      val estimated = PreProjectSplitIterator.calcMinOutputSize(batch,
        GpuTieredProject(Seq(Seq(reference))))
      assertResult(GpuColumnVector.getTotalDeviceMemoryUsed(batch))(estimated)
    }
  }

  test("serialized buffer slices preserve byte positions above Int.MaxValue") {
    val start = Int.MaxValue.toLong + 1
    withResource(HostMemoryBuffer.allocate(start + 8, false)) { buffer =>
      buffer.setLong(start, 0x123456789abcdefL)
      withResource(new SlicedSerializedColumnVector(buffer, start, start + 8)) { column =>
        assertResult(8L)(column.getLength)
        assertResult(0x123456789abcdefL)(column.getWrap.getLong(0))
        assertResult(2L)(buffer.getRefCount)
      }
      assertResult(1L)(buffer.getRefCount)
      intercept[IllegalArgumentException] {
        new SlicedSerializedColumnVector(buffer, start, start + 9)
      }
      assertResult(1L)(buffer.getRefCount)
    }
  }

  test("GPU Kudo mode serializes and merges wide STRING offsets") {
    runKudoShuffleTest(targetBatchSize = 1000, wideStrings = true)
  }

  Seq(false, true).foreach { wide =>
    test(s"single GPU partition keeps a packed table with Kudo GPU mode enabled, wide=$wide") {
      TrampolineUtil.cleanupAnyExistingSession()
      val conf = createSparkConf()
        .set(RapidsConf.SHUFFLE_MANAGER_MODE.key, "CACHE_ONLY")
        .set("spark.shuffle.manager", ShimLoader.getRapidsShuffleManagerClass)
      TestUtils.withGpuSparkSession(conf) { _ =>
        GpuShuffleEnv.init(new RapidsConf(conf))
        assert(GpuSinglePartitioning.usesGPUShuffle)
        assert(!GpuSinglePartitioning.usesKudoGPUSlicing)
        withResource(if (wide) buildWideStringBatch() else buildBatch()) { original =>
          GpuColumnVector.incRefCounts(original)
          val output = GpuSinglePartitioning.columnarEvalAny(original)
            .asInstanceOf[Array[(ColumnarBatch, Int)]]
          withResource(output.map(_._1)) { partitions =>
            assertResult(1)(partitions.length)
            assert(partitions.head.column(0).isInstanceOf[GpuPackedTableColumn])
            val table = partitions.head.column(0).asInstanceOf[GpuPackedTableColumn]
              .getContiguousTable.getTable
            if (wide) {
              assertResult(DType.INT64)(table.getColumn(0).getOffsetsType)
            }
            withResource(GpuColumnVector.from(table,
                GpuColumnVector.extractTypes(original))) { restored =>
              compareBatches(original, restored)
            }
          }
        }
      }
    }

    test(s"GPU shuffle keeps packed tables with Kudo GPU mode enabled, wide=$wide") {
      TrampolineUtil.cleanupAnyExistingSession()
      val conf = createSparkConf()
        .set(RapidsConf.SHUFFLE_MANAGER_MODE.key, "CACHE_ONLY")
        .set("spark.shuffle.manager", ShimLoader.getRapidsShuffleManagerClass)
      TestUtils.withGpuSparkSession(conf) { _ =>
        GpuShuffleEnv.init(new RapidsConf(conf))
        val indices = Array(0, 2, 2)
        val partitioning = createGpuPartitioning(indices)
        assert(partitioning.usesGPUShuffle)
        assert(!partitioning.usesKudoGPUSlicing)
        withResource(if (wide) buildWideStringBatch() else buildBatch()) { original =>
          GpuColumnVector.incRefCounts(original)
          withResource(partitioning.sliceInternalGpuOrCpuAndClose(original.numRows(), indices,
              GpuColumnVector.extractColumns(original)).map(_._1)) { partitions =>
            assert(partitions.forall(_.column(0).isInstanceOf[GpuPackedTableColumn]))
            val tables = partitions.map(_.column(0).asInstanceOf[GpuPackedTableColumn]
              .getContiguousTable.getTable)
            if (wide) {
              tables.foreach { table =>
                val expectedWidth = if (table.getRowCount == 0) DType.INT32 else DType.INT64
                assertResult(expectedWidth)(table.getColumn(0).getOffsetsType)
              }
            }
            withResource(Table.concatenate(tables: _*)) { merged =>
              withResource(GpuColumnVector.from(merged,
                  GpuColumnVector.extractTypes(original))) { restored =>
                compareBatches(original, restored)
              }
            }
          }
        }
      }
    }
  }

  test("wide STRING GPU merge retries after GPU split OOM") {
    runKudoShuffleTest(targetBatchSize = 1000, minSplitSize = Some(1),
      forceSplitRetry = true, wideStrings = true)
  }

  test("GPU kudo partitioning with deserialization") {
    // Use a small target size to prevent coalescing from combining partitions
    runKudoShuffleTest(targetBatchSize = 1000)
  }

  test("GPU kudo partitioning with deserialization and split retry") {
    // minSplitSize=1 so the split is not blocked by the minSize floor for small test data.
    // In production minSplitSize defaults to 10MB; the split throws if dataSize/2 < minSize.
    runKudoShuffleTest(
      targetBatchSize = 100000,
      minSplitSize = Some(1L),
      forceSplitRetry = true)
  }

}
