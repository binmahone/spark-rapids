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

package org.apache.spark.sql.rapids.execution

import ai.rapids.cudf.{Rmm, RmmAllocationMode, Table}
import com.nvidia.spark.rapids.{GpuColumnVector, NoopMetric, RapidsConf}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.spill.SpillFramework
import org.apache.commons.lang3.SerializationUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType}

class IndependentBroadcastRetirementGpuSuite extends AnyFunSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    super.beforeAll()
    Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 512 * 1024 * 1024)
    SpillFramework.initialize(new RapidsConf(new SparkConf))
  }

  override def afterAll(): Unit = {
    SpillFramework.shutdown()
    Rmm.shutdown()
    super.afterAll()
  }

  test("retirement releases a real materialized broadcast and prevents reuse") {
    val values = (1 to 4).map(Int.box)
    val broadcast = withResource(new Table.TestBuilder().column(values: _*).build()) { table =>
      withResource(GpuColumnVector.from(table, Array[DataType](IntegerType))) { batch =>
        val attrs = GpuColumnVector.extractTypes(batch).map(t => AttributeReference("", t)())
        val host = withResource(new SerializeBatchDeserializeHostBuffer(batch)) { value =>
          SerializationUtils.clone(value)
        }
        GpuBroadcastExchangeExecBase.makeBroadcastBatch(Array(host), attrs,
          NoopMetric, NoopMetric, NoopMetric, SQLConf.get)
      }
    }
    try {
      val bytes = broadcast.batch.sizeInBytes
      assert(bytes > 0)
      val before = Rmm.getTotalBytesAllocated
      assertResult(bytes)(broadcast.retireAfterIndependentQuery())
      assert(broadcast.isRetiredAfterIndependentQuery)
      assert(Rmm.getTotalBytesAllocated < before)
      assertResult(0L)(broadcast.retireAfterIndependentQuery())
      intercept[IllegalArgumentException](broadcast.batch)
    } finally {
      broadcast.closeInternal()
    }
  }
}
