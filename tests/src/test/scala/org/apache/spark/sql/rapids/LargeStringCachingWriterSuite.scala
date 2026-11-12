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

package org.apache.spark.sql.rapids

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer, HostMemoryBuffer}
import com.nvidia.spark.rapids.{GpuCompressedColumnVector, RapidsConf, RmmSparkRetrySuiteBase}
import com.nvidia.spark.rapids.{ShuffleBufferCatalog, TableCompressionCodec}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.format.CodecType
import com.nvidia.spark.rapids.shuffle.RapidsShuffleTestHelper
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase.METRIC_DATA_SIZE
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{BlockManager, BlockManagerId, ShuffleBlockId}

class LargeStringCachingWriterSuite extends RmmSparkRetrySuiteBase with MockitoSugar {
  private def compressedBatch(rows: Int): ColumnarBatch = {
    val codec = TableCompressionCodec.getCodec(CodecType.COPY,
      TableCompressionCodec.makeCodecConfig(new RapidsConf(new SparkConf)))
    withResource(codec.createBatchCompressor(0, Cuda.DEFAULT_STREAM)) { compressor =>
      compressor.addTableToCompress(RapidsShuffleTestHelper.buildContiguousTable(rows))
      withResource(compressor.finish()) { tables =>
        GpuCompressedColumnVector.from(tables.head)
      }
    }
  }

  private def copyBytes(buffer: DeviceMemoryBuffer): Array[Byte] = {
    withResource(HostMemoryBuffer.allocate(buffer.getLength)) { host =>
      host.copyFromDeviceBuffer(buffer)
      val bytes = new Array[Byte](buffer.getLength.toInt)
      host.getBytes(bytes, 0, 0L, bytes.length)
      bytes
    }
  }

  test("cached compressed output survives the input iterator releasing its batch") {
    val catalog = new ShuffleBufferCatalog()
    val shuffleId = 7901
    val dependency = mock[GpuShuffleDependency[Int, ColumnarBatch, ColumnarBatch]]
    when(dependency.partitioner).thenReturn(new HashPartitioner(4))
    val handle = new GpuShuffleHandle[Int, ColumnarBatch](
      new BaseShuffleHandle(shuffleId, dependency), dependency)
    catalog.registerShuffle(shuffleId)
    val blockManager = mock[BlockManager]
    when(blockManager.shuffleServerId).thenReturn(BlockManagerId("executor", "localhost", 12345))
    val writer = new RapidsCachingWriter[Int, ColumnarBatch](blockManager,
      handle, 0L, new ShuffleWriteMetrics, catalog, None,
      Map(METRIC_DATA_SIZE -> new SQLMetric("size")))
    try {
      var expected: Array[Byte] = null
      val buffer = withResource(compressedBatch(16)) { batch =>
        val original = batch.column(0).asInstanceOf[GpuCompressedColumnVector].getTableBuffer
        expected = copyBytes(original)
        writer.write(Iterator(0 -> batch))
        assertResult(2)(original.getRefCount)
        original
      }
      assertResult(1)(buffer.getRefCount)
      assert(writer.stop(true).isDefined)
      val tableId = catalog.blockIdToMetas(ShuffleBlockId(shuffleId, 0L, 0))
        .head.bufferMeta().id()
      withResource(catalog.getShuffleBufferHandle(tableId)) { handle =>
        withResource(handle.spillable.materialize()) { stored =>
          assert(copyBytes(stored).sameElements(expected))
        }
      }
    } finally {
      catalog.unregisterShuffle(shuffleId)
    }
  }
}
