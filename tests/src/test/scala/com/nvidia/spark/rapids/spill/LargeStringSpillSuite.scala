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

package com.nvidia.spark.rapids.spill

import java.util.{Collections, Optional}

import ai.rapids.cudf.{Cuda, DType, HostColumnVector, HostMemoryBuffer, Table}
import com.nvidia.spark.rapids.{GpuCompressedColumnVector, MetaUtils, RapidsConf, RmmSparkRetrySuiteBase, TableCompressionCodec}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.format.CodecType

import org.apache.spark.SparkConf

class LargeStringSpillSuite extends RmmSparkRetrySuiteBase {
  private def wideTable(): Table = {
    withResource(HostColumnVector.fromStrings("abc", null, "def", "")) { narrow =>
      val offsets = HostMemoryBuffer.allocate(40, false)
      Seq(0L, 3L, 3L, 6L, 6L).zipWithIndex.foreach { case (end, i) =>
        offsets.setLong(i * 8L, end)
      }
      narrow.getData.incRefCount()
      narrow.getValidity.incRefCount()
      withResource(new HostColumnVector(DType.STRING, 4, Optional.of[java.lang.Long](1L),
          narrow.getData, narrow.getValidity, offsets, Collections.emptyList(), DType.INT64)) {
        wide => withResource(wide.copyToDevice())(column => new Table(column))
      }
    }
  }

  private def verify(table: Table): Unit = {
    assertResult(DType.INT64)(table.getColumn(0).getOffsetsType)
    withResource(table.getColumn(0).copyToHost()) { host =>
      assertResult("abc")(host.getJavaString(0))
      assert(host.isNull(1))
      assertResult("def")(host.getJavaString(2))
      assertResult("")(host.getJavaString(3))
    }
  }

  Seq(false, true).foreach { disk =>
    test(s"wide STRING table recovers its width and values after spill, disk=$disk") {
      withResource(SpillableTableHandle(wideTable())) { handle =>
        assert(handle.spillable)
        assert(SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes) > 0)
        assert(handle.dev.isEmpty)
        assert(handle.host.isDefined)
        if (disk) {
          assert(SpillFramework.stores.hostStore.spill(handle.host.get.sizeInBytes) > 0)
          assert(handle.host.get.host.isEmpty)
          assert(handle.host.get.disk.isDefined)
        }
        withResource(handle.materialize())(verify)
      }
    }

    Seq(CodecType.COPY, CodecType.NVCOMP_LZ4).foreach { codecId =>
      test(s"compressed wide STRING recovers after spill, codec=$codecId disk=$disk") {
        val config = TableCompressionCodec.makeCodecConfig(new RapidsConf(new SparkConf))
        val codec = TableCompressionCodec.getCodec(codecId, config)
        val compressed = withResource(wideTable()) { table =>
          withResource(codec.createBatchCompressor(0, Cuda.DEFAULT_STREAM)) { compressor =>
            compressor.addTableToCompress(table.contiguousSplit()(0))
            withResource(compressor.finish()) { tables =>
              GpuCompressedColumnVector.from(tables.head)
            }
          }
        }
        withResource(SpillableCompressedColumnarBatchHandle(compressed)) { handle =>
          assert(SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes) > 0)
          assert(handle.dev.isEmpty)
          assert(handle.host.isDefined)
          if (disk) {
            assert(SpillFramework.stores.hostStore.spill(handle.host.get.sizeInBytes) > 0)
            assert(handle.host.get.host.isEmpty)
            assert(handle.host.get.disk.isDefined)
          }
          withResource(handle.materialize()) { batch =>
            val vector = batch.column(0).asInstanceOf[GpuCompressedColumnVector]
            withResource(codec.createBatchDecompressor(1024 * 1024, Cuda.DEFAULT_STREAM)) {
              decompressor =>
                vector.getTableBuffer.incRefCount()
                decompressor.addBufferToDecompress(vector.getTableBuffer,
                  vector.getTableMeta.bufferMeta())
                withResource(decompressor.finishAsync()) { buffers =>
                  withResource(MetaUtils.getTableFromMeta(
                    buffers.head, vector.getTableMeta))(verify)
                }
            }
          }
        }
      }
    }
  }
}
