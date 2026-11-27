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
package org.apache.spark.sql.rapids.aggregate

import java.math.BigInteger

import ai.rapids.cudf.{ColumnVector, Rmm}
import com.nvidia.spark.rapids.{GpuBoundReference, GpuColumnVector, RmmSparkRetrySuiteBase}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.types.{DataType, DecimalType, LongType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class ExtractChunkNullReplacementSuite extends RmmSparkRetrySuiteBase {
  private def check(values: Seq[BigInteger], decimal: Boolean): Unit = {
    val allocated = Rmm.getTotalBytesAllocated
    val dataType: DataType = if (decimal) DecimalType(38, 2) else LongType
    val input = if (decimal) {
      ColumnVector.decimalFromBigInt(-2, values: _*)
    } else {
      ColumnVector.fromBoxedLongs(values.map(v =>
        if (v == null) null else Long.box(v.longValueExact())): _*)
    }
    withResource(input) { column =>
      withResource(new ColumnarBatch(Array(
          GpuColumnVector.from(column.incRefCount(), dataType)), values.size)) { batch =>
        val bound = GpuBoundReference(0, dataType, nullable = true)(ExprId(0), "value")
        for (replace <- Seq(false, true); chunk <- 0 until (if (decimal) 4 else 2)) {
          withResource(GpuExtractChunk32(bound, chunk, replace).columnarEval(batch)) { result =>
            withResource(result.getBase.copyToHost()) { host =>
              assert(host.getRowCount == values.size)
              for ((value, row) <- values.zipWithIndex) {
                if (value == null && !replace) {
                  assert(host.isNull(row))
                } else {
                  assert(!host.isNull(row))
                  val expected = if (value == null) 0 else value.shiftRight(32 * chunk).intValue()
                  assert(host.getInt(row) == expected)
                }
              }
            }
          }
        }
        assert(column.getNullCount == values.count(_ == null))
      }
      assert(column.getRefCount == 1)
    }
    assert(Rmm.getTotalBytesAllocated == allocated)
  }

  for (decimal <- Seq(false, true)) {
    val kind = if (decimal) "decimal128" else "int64"
    test(s"$kind chunks preserve positive and negative values without nulls") {
      val values = Seq(BigInteger.ZERO, BigInteger.ONE, BigInteger.valueOf(-1),
        BigInteger.valueOf(Long.MinValue), BigInteger.valueOf(Long.MaxValue))
      val wide = if (decimal) Seq(BigInteger.TEN.pow(37), BigInteger.TEN.pow(37).negate())
        else Seq.empty
      check(values ++ wide, decimal)
    }
    test(s"$kind chunks replace only null rows and preserve nullable input") {
      check(Seq(null, BigInteger.valueOf(-4294967297L), BigInteger.valueOf(4294967297L)),
        decimal)
    }
    test(s"$kind chunks preserve empty and all-null batches") {
      check(Seq.empty, decimal)
      check(Seq(null, null), decimal)
    }
  }
}
