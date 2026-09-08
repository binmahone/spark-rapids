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

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.format.{CodecType, TableMeta}
import com.nvidia.spark.rapids.shuffle.RapidsShuffleTestHelper
import com.nvidia.spark.rapids.spill.SpillFramework
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.storage.ShuffleBlockId

class ShuffleBufferCatalogSuite
  extends AnyFunSuite with MockitoSugar with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    super.beforeEach()
    SpillFramework.initialize(new RapidsConf(new SparkConf))
  }

  override def afterEach(): Unit = {
    super.afterEach()
    SpillFramework.shutdown()
  }

  test("registered shuffles should be active") {
    val shuffleCatalog = new ShuffleBufferCatalog()
    assertResult(false)(shuffleCatalog.hasActiveShuffle(123))
    shuffleCatalog.registerShuffle(123)
    assertResult(true)(shuffleCatalog.hasActiveShuffle(123))
    shuffleCatalog.unregisterShuffle(123)
    assertResult(false)(shuffleCatalog.hasActiveShuffle(123))
  }

  test("adding a degenerate batch") {
    val shuffleCatalog = new ShuffleBufferCatalog()
    val tableMeta = mock[TableMeta]
    // need to register the shuffle id first
    assertThrows[IllegalStateException] {
      shuffleCatalog.addDegenerateRapidsBuffer(ShuffleBlockId(1, 1L, 1), tableMeta)
    }
    shuffleCatalog.registerShuffle(1)
    shuffleCatalog.addDegenerateRapidsBuffer(ShuffleBlockId(1,1L,1), tableMeta)
    val storedMetas = shuffleCatalog.blockIdToMetas(ShuffleBlockId(1, 1L, 1))
    assertResult(1)(storedMetas.size)
    assertResult(tableMeta)(storedMetas.head)
  }

  test("adding a contiguous batch adds it to the spill store") {
    val shuffleCatalog = new ShuffleBufferCatalog()
    val ct = RapidsShuffleTestHelper.buildContiguousTable(1000)
    shuffleCatalog.registerShuffle(1)
    assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
    shuffleCatalog.addContiguousTable(ShuffleBlockId(1, 1L, 1), ct, -1)
    assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
    val storedMetas = shuffleCatalog.blockIdToMetas(ShuffleBlockId(1, 1L, 1))
    assertResult(1)(storedMetas.size)
    shuffleCatalog.unregisterShuffle(1)
  }

  test("adding a compressed batch does not consume the caller's reference") {
    val shuffleCatalog = new ShuffleBufferCatalog()
    val blockId = ShuffleBlockId(1, 1L, 1)
    val tableMeta = RapidsShuffleTestHelper.mockTableMeta(1000)
    val compressedBatch = withResource(DeviceMemoryBuffer.allocate(1024)) { deviceBuffer =>
      GpuCompressedColumnVector.from(deviceBuffer, tableMeta)
    }

    shuffleCatalog.registerShuffle(blockId.shuffleId)
    withResource(compressedBatch) { batch =>
      shuffleCatalog.addCompressedBatch(blockId, batch, -1)
    }

    val tableId = tableMeta.bufferMeta().id()
    withResource(shuffleCatalog.getShuffleBufferHandle(tableId)) { sendHandle =>
      withResource(sendHandle.spillable.materialize()) { materialized =>
        assertResult(1024)(materialized.getLength)
      }
    }
    shuffleCatalog.unregisterShuffle(blockId.shuffleId)
  }

  test("received compressed buffer remains compressed until coalesce") {
    val receivedCatalog = new ShuffleReceivedBufferCatalog()
    RapidsShuffleTestHelper.withMockContiguousTable(1000) { contiguousTable =>
      val compressedSize = contiguousTable.getBuffer.getLength
      val tableMeta = MetaUtils.buildTableMeta(
        Some(1), contiguousTable, CodecType.NVCOMP_LZ4, compressedSize)
      closeOnExcept(DeviceMemoryBuffer.allocate(compressedSize)) { receivedBuffer =>
        receivedBuffer.copyFromDeviceBufferAsync(
          0, contiguousTable.getBuffer, 0, compressedSize, Cuda.DEFAULT_STREAM)
        Cuda.DEFAULT_STREAM.sync()
        val handle = receivedCatalog.addBuffer(receivedBuffer, tableMeta, -1)

        val (batch, memoryUsedBytes) =
          receivedCatalog.getColumnarBatchAndRemove(handle, Array[DataType](IntegerType))
        withResource(batch) { compressedBatch =>
          assert(GpuCompressedColumnVector.isBatchCompressed(compressedBatch))
          assertResult(1000)(compressedBatch.numRows())
          assertResult(compressedSize)(memoryUsedBytes)
          val compressedColumn =
            compressedBatch.column(0).asInstanceOf[GpuCompressedColumnVector]
          assertResult(CodecType.NVCOMP_LZ4)(
            compressedColumn.getTableMeta.bufferMeta().codecBufferDescrs(0).codec())
          assertResult(compressedSize)(compressedColumn.getTableBuffer.getLength)
        }
      }
    }
  }

  test("failed map cleanup only removes buffers from that shuffle map") {
    val shuffleCatalog = new ShuffleBufferCatalog()
    val failedBlock = ShuffleBlockId(1, 10L, 0)
    val otherMapBlock = ShuffleBlockId(1, 11L, 0)
    val otherShuffleBlock = ShuffleBlockId(2, 10L, 0)
    val failedMeta = mock[TableMeta]
    val otherMapMeta = mock[TableMeta]
    val otherShuffleMeta = mock[TableMeta]

    shuffleCatalog.registerShuffle(1)
    shuffleCatalog.registerShuffle(2)
    shuffleCatalog.addDegenerateRapidsBuffer(failedBlock, failedMeta)
    shuffleCatalog.addDegenerateRapidsBuffer(otherMapBlock, otherMapMeta)
    shuffleCatalog.addDegenerateRapidsBuffer(otherShuffleBlock, otherShuffleMeta)

    shuffleCatalog.removeCachedHandles(failedBlock.shuffleId, failedBlock.mapId)

    assertThrows[NoSuchElementException] {
      shuffleCatalog.blockIdToMetas(failedBlock)
    }
    assertResult(Seq(otherMapMeta))(shuffleCatalog.blockIdToMetas(otherMapBlock))
    assertResult(Seq(otherShuffleMeta))(shuffleCatalog.blockIdToMetas(otherShuffleBlock))

    // Full shuffle cleanup must remain safe after a failed map removed its own entries.
    shuffleCatalog.unregisterShuffle(1)
    shuffleCatalog.unregisterShuffle(2)
  }

  test("shuffle send lease keeps a buffer readable across unregister") {
    val shuffleCatalog = new ShuffleBufferCatalog()
    val blockId = ShuffleBlockId(1, 10L, 0)
    val ct = RapidsShuffleTestHelper.buildContiguousTable(1000)

    shuffleCatalog.registerShuffle(blockId.shuffleId)
    shuffleCatalog.addContiguousTable(blockId, ct, -1)
    val meta = shuffleCatalog.blockIdToMetas(blockId).head
    val sendHandle = shuffleCatalog.getShuffleBufferHandle(meta.bufferMeta().id())

    shuffleCatalog.unregisterShuffle(blockId.shuffleId)
    withResource(sendHandle.spillable.materialize()) { buffer =>
      assert(buffer.getLength > 0)
    }

    sendHandle.close()
    assertThrows[IllegalStateException] {
      sendHandle.spillable.materialize()
    }
  }

  test("get a columnar batch iterator from catalog") {
    val shuffleCatalog = new ShuffleBufferCatalog()
    shuffleCatalog.registerShuffle(1)
    // add metadata only table
    val tableMeta = RapidsShuffleTestHelper.mockTableMeta(0)
    shuffleCatalog.addDegenerateRapidsBuffer(ShuffleBlockId(1, 1L, 1), tableMeta)
    val ct = RapidsShuffleTestHelper.buildContiguousTable(1000)
    shuffleCatalog.addContiguousTable(ShuffleBlockId(1, 1L, 1), ct, -1)
    val iter =
      shuffleCatalog.getColumnarBatchIterator(
        ShuffleBlockId(1, 1L, 1), Array[DataType](IntegerType))
    withResource(iter.toArray) { cbs =>
      assertResult(2)(cbs.length)
      assertResult(0)(cbs.head.numRows())
      assertResult(1)(cbs.head.numCols())
      assertResult(1000)(cbs.last.numRows())
      assertResult(1)(cbs.last.numCols())
      shuffleCatalog.unregisterShuffle(1)
    }
  }
}
