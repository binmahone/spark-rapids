/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

import java.io.ByteArrayInputStream

import ai.rapids.cudf.{ColumnVector, DType, Schema}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.kudo.{KudoSerializer, KudoTable}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuRoundRobinKudoSuite extends AnyFunSuite {
  for (count <- Seq(2, 4, 20)) {
    test(s"Kudo GPU round-robin preserves rows across $count partitions") {
      TrampolineUtil.cleanupAnyExistingSession()
      val conf = new SparkConf()
        .set(RapidsConf.SHUFFLE_COMPRESSION_CODEC.key, "none")
        .set(RapidsConf.SHUFFLE_MANAGER_MODE.key, "MULTITHREADED")
        .set(RapidsConf.SHUFFLE_KUDO_SERIALIZER_ENABLED.key, "true")
        .set(RapidsConf.SHUFFLE_KUDO_WRITE_MODE.key, "GPU")
      TestUtils.withGpuSparkSession(conf) { _ =>
        GpuShuffleEnv.init(new RapidsConf(conf))
        val values = Array("zero", null, "", "three", "four", "five", "six", "seven")
        withResource(ColumnVector.fromStrings(values: _*)) { input =>
          assertResult(DType.INT32)(input.getOffsetsType)
          val partitioning = GpuRoundRobinPartitioning(count)
          assert(partitioning.usesKudoGPUSlicing)
          val batch = new ColumnarBatch(
            Array(GpuColumnVector.from(input.incRefCount(), StringType)), values.length)
          val output = partitioning.columnarEvalAny(batch)
            .asInstanceOf[Array[(ColumnarBatch, Int)]]
          withResource(output.map(_._1)) { _ =>
            assert(output.forall { case (_, index) => index >= 0 && index < count })
            assertResult(values.length)(output.map(_._1.numRows()).sum)
            val serializer = new KudoSerializer(Schema.builder().column(DType.STRING, "s").build())
            val actual = output.flatMap { case (serialized, _) =>
              val wrap = serialized.column(0).asInstanceOf[SlicedSerializedColumnVector].getWrap
              val bytes = new Array[Byte](Math.toIntExact(wrap.getLength))
              wrap.getBytes(bytes, 0, 0, bytes.length)
              withResource(KudoTable.from(new ByteArrayInputStream(bytes)).get()) { table =>
                withResource(serializer.mergeToTable(Array(table))) { restored =>
                  withResource(restored.getColumn(0).copyToHost()) { host =>
                    (0 until restored.getRowCount.toInt).map { row =>
                      if (host.isNull(row)) "<null>" else host.getJavaString(row)
                    }
                  }
                }
              }
            }
            assertResult(values.map(v => Option(v).getOrElse("<null>")).sorted)(actual.sorted)
          }
        }
      }
    }
  }
}
