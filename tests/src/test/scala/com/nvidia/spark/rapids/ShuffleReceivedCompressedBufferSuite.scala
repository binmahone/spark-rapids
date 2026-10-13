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

import ai.rapids.cudf.Cuda
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shuffle.RapidsShuffleTestHelper
import com.nvidia.spark.rapids.spill.SpillFramework
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.IntegerType

class ShuffleReceivedCompressedBufferSuite extends AnyFunSuite with BeforeAndAfterEach {
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

  test("received compressed payload stays opaque until its codec decodes it") {
    val codec = new CopyCompressionCodec
    val catalog = new ShuffleReceivedBufferCatalog
    withResource(codec.createBatchCompressor(0, Cuda.DEFAULT_STREAM)) { compressor =>
      compressor.addTableToCompress(RapidsShuffleTestHelper.buildContiguousTable(1000))
      withResource(compressor.finish()) { compressed =>
        val source = compressed.head
        val copied = source.buffer.sliceWithCopy(0, source.buffer.getLength)
        val handle = catalog.addBuffer(copied, source.meta, -1)
        val (received, bytesUsed) = catalog.getColumnarBatchAndRemove(handle, Array(IntegerType))
        withResource(received) { batch =>
          assert(GpuCompressedColumnVector.isBatchCompressed(batch))
          assert(batch.numRows() == 1000)
          assert(bytesUsed == source.buffer.getLength)
          val column = batch.column(0).asInstanceOf[GpuCompressedColumnVector]
          withResource(codec.createBatchDecompressor(1024 * 1024, Cuda.DEFAULT_STREAM)) {
              decompressor =>
            column.getTableBuffer.incRefCount()
            decompressor.addBufferToDecompress(column.getTableBuffer, source.meta.bufferMeta())
            withResource(decompressor.finishAsync()) { output =>
              withResource(MetaUtils.getTableFromMeta(output.head, source.meta)) { table =>
                withResource(table.getColumn(0).copyToHost()) { actual =>
                  assert(actual.getRowCount == 1000)
                  assert((0 until 1000).forall(index => actual.getInt(index) == index))
                }
              }
            }
          }
        }
      }
    }
  }
}
