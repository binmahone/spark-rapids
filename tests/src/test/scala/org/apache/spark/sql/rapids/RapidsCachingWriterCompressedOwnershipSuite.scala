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
import com.nvidia.spark.rapids.{CopyCompressionCodec, GpuCompressedColumnVector, RapidsConf, ShuffleBufferCatalog}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shuffle.RapidsShuffleTestHelper
import com.nvidia.spark.rapids.spill.SpillFramework
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase.METRIC_DATA_SIZE
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.ShuffleBlockId

class RapidsCachingWriterCompressedOwnershipSuite
    extends AnyFunSuite with MockitoSugar with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    super.beforeEach()
    SpillFramework.initialize(new RapidsConf(new SparkConf))
  }

  override def afterEach(): Unit = {
    try {
      SpillFramework.shutdown()
    } finally {
      super.afterEach()
    }
  }

  private def bytes(buffer: DeviceMemoryBuffer): Array[Byte] = {
    withResource(HostMemoryBuffer.allocate(buffer.getLength)) { host =>
      host.copyFromDeviceBuffer(buffer)
      val result = new Array[Byte](buffer.getLength.toInt)
      host.getBytes(result, 0, 0, result.length)
      result
    }
  }

  test("compressed writer preserves caller and catalog buffer ownership") {
    val catalog = new ShuffleBufferCatalog
    val blockId = ShuffleBlockId(1, 1L, 0)
    catalog.registerShuffle(blockId.shuffleId)
    val handle = mock[GpuShuffleHandle[Int, ColumnarBatch]]
    val dependency = mock[GpuShuffleDependency[Int, ColumnarBatch, ColumnarBatch]]
    when(handle.shuffleId).thenReturn(blockId.shuffleId)
    when(handle.dependency).thenReturn(dependency)
    when(dependency.partitioner).thenReturn(new HashPartitioner(1))
    val writeMetrics = new ShuffleWriteMetrics
    val writer = new RapidsCachingWriter[Int, ColumnarBatch](null, handle, blockId.mapId,
      writeMetrics, catalog, None, Map(METRIC_DATA_SIZE -> new SQLMetric("size", 0)))
    val codec = new CopyCompressionCodec
    var expected: Array[Byte] = null

    withResource(new AutoCloseable {
      override def close(): Unit = catalog.unregisterShuffle(blockId.shuffleId)
    }) { _ =>
      withResource(codec.createBatchCompressor(0, Cuda.DEFAULT_STREAM)) { compressor =>
        compressor.addTableToCompress(RapidsShuffleTestHelper.buildContiguousTable(1000))
        withResource(compressor.finish()) { compressed =>
          expected = bytes(compressed.head.buffer)
          withResource(GpuCompressedColumnVector.from(compressed.head)) { batch =>
            val buffer = batch.column(0).asInstanceOf[GpuCompressedColumnVector].getTableBuffer
            val callerReferences = buffer.getRefCount
            writer.write(Iterator(0 -> batch))
            assert(buffer.getRefCount == callerReferences + 1)
          }
        }
      }
      val meta = catalog.blockIdToMetas(blockId).head
      withResource(catalog.getShuffleBufferHandle(meta.bufferMeta().id())) { lease =>
        withResource(lease.spillable.materialize()) { buffer =>
          assert(bytes(buffer).sameElements(expected))
        }
      }
    }
  }
}
