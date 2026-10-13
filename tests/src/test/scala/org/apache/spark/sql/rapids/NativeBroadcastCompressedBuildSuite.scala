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

import ai.rapids.cudf.Cuda
import com.nvidia.spark.rapids.{CopyCompressionCodec, GpuColumnVector, GpuCompressedColumnVector, GpuMetric, NoopMetric, RapidsConf}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shuffle.RapidsShuffleTestHelper
import com.nvidia.spark.rapids.spill.SpillFramework
import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.GpuShuffleBroadcastHelper
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.storage.BlockManager

class NativeBroadcastCompressedBuildSuite extends AnyFunSuite with MockitoSugar {
  test("native broadcast build decodes compressed GPU batches before concatenating") {
    val previous = SparkEnv.get
    val sparkConf = new SparkConf(false)
      .set("spark.shuffle.manager", GpuShuffleEnv.RAPIDS_SHUFFLE_CLASS)
      .set("spark.rapids.shuffle.enabled", "true")
      .set("spark.rapids.shuffle.mode", "UCX")
      .set("spark.rapids.sql.enabled", "true")
    val sparkEnv = mock[SparkEnv]
    when(sparkEnv.conf).thenReturn(sparkConf)
    when(sparkEnv.blockManager).thenReturn(mock[BlockManager])
    when(sparkEnv.shuffleManager).thenReturn(mock[ShuffleManager])
    val sqlConf = new SQLConf
    sparkConf.getAll.foreach { case (key, value) => sqlConf.setConfString(key, value) }
    SparkEnv.set(sparkEnv)
    SpillFramework.initialize(new RapidsConf(sparkConf))
    try {
      SQLConf.withExistingConf(sqlConf) {
        GpuShuffleEnv.init(new RapidsConf(sqlConf))
        assert(GpuShuffleEnv.useGPUShuffle(new RapidsConf(sqlConf)))
        val codec = new CopyCompressionCodec
        val batches = (0 until 2).map { _ =>
          withResource(codec.createBatchCompressor(0, Cuda.DEFAULT_STREAM)) { compressor =>
            compressor.addTableToCompress(RapidsShuffleTestHelper.buildContiguousTable(500))
            withResource(compressor.finish()) { compressed =>
              GpuCompressedColumnVector.from(compressed.head)
            }
          }
        }
        withResource(batches) { inputs =>
          val schema = StructType(Seq(StructField("key", IntegerType)))
          val attributes = Seq(AttributeReference("key", IntegerType)())
          val metrics = Map.empty[String, GpuMetric].withDefaultValue(NoopMetric)
          val retained = inputs.iterator.map(GpuCompressedColumnVector.incRefCounts)
          withResource(GpuShuffleBroadcastHelper.getShuffleBroadcastBatchFromIter(
              retained, schema, attributes, metrics, 1024 * 1024, 1024 * 1024)) { batch =>
            assert(batch.numRows() == 1000)
            assert(!GpuCompressedColumnVector.isBatchCompressed(batch))
            withResource(GpuColumnVector.extractBases(batch).head.copyToHost()) { actual =>
              assert((0 until 1000).forall(index => actual.getInt(index) == index % 500))
            }
          }
        }
      }
    } finally {
      GpuShuffleEnv.shutdown()
      SparkEnv.set(previous)
      SpillFramework.shutdown()
    }
  }
}
