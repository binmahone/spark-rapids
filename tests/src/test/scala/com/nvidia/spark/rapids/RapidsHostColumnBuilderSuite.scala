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

import java.util.Arrays

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.Arm.withResource
import ai.rapids.cudf.HostColumnVector.{BasicType, ListType, StructType}
import org.scalatest.funsuite.AnyFunSuite

class RapidsHostColumnBuilderSuite extends AnyFunSuite {
  test("growing buffer preserves correctness") {
    val b1 = new RapidsHostColumnBuilder(new BasicType(false, DType.INT32), 0) // grows
    val b2 = new RapidsHostColumnBuilder(new BasicType(false, DType.INT32), 8) // does not grow
    for (i <- 0 to 7) {
      b1.append(i)
      b2.append(i)
    }
    val v1 = b1.build()
    val v2 = b2.build()
    for (i <- 0 to 7) {
      assertResult(v1.getInt(i))(v2.getInt(i))
    }
    v1.close()
    v2.close()
    b1.close()
    b2.close()
  }

  test("appendLists walks appendChildOrNull for typed and null list elements") {
    // appendLists -> append(List) -> appendChildOrNull: one arm per element type, plus the null arm
    def buildList(childType: DType, elems: AnyRef*): Unit = {
      val lt = new ListType(true, new BasicType(true, childType))
      val b = new RapidsHostColumnBuilder(lt, 1)
      try {
        b.appendLists(java.util.Arrays.asList(elems: _*))
        val v = b.build()
        try {
          assertResult(1L)(v.getRowCount)
        } finally {
          v.close()
        }
      } finally {
        b.close()
      }
    }
    buildList(DType.INT32, Integer.valueOf(1), null, Integer.valueOf(3))
    buildList(DType.INT64, java.lang.Long.valueOf(1L), null)
    buildList(DType.FLOAT64, java.lang.Double.valueOf(1.0d), null)
    buildList(DType.FLOAT32, java.lang.Float.valueOf(1.0f), null)
    buildList(DType.BOOL8, java.lang.Boolean.TRUE, null)
    buildList(DType.STRING, "a", null)
  }

  test("captureState then restoreState rolls back appended struct rows including children") {
    val st = new StructType(true,
      new BasicType(true, DType.INT32),
      new BasicType(true, DType.INT32))
    val b = new RapidsHostColumnBuilder(st, 4)
    try {
      b.getChild(0).append(1)
      b.getChild(1).append(10)
      b.endStruct()
      val snapshot = b.captureState()
      b.getChild(0).append(2)
      b.getChild(1).append(20)
      b.endStruct()
      b.restoreState(snapshot)
      val v = b.build()
      try {
        assertResult(1L)(v.getRowCount)
      } finally {
        v.close()
      }
    } finally {
      b.close()
    }
  }
  test("wide STRING offsets preserve append and snapshot rollback beyond Int32") {
    val chunk = 1 << 20
    val value = new Array[Byte](chunk)
    Arrays.fill(value, 'x'.toByte)
    withResource(new RapidsHostColumnBuilder(new BasicType(true, DType.STRING), 1)) { builder =>
      (0 until 2047).foreach(_ => builder.appendUTF8String(value))
      builder.appendUTF8String(value, 0, chunk - 1)
      withResource(builder.build()) { host =>
        assertResult(DType.INT32)(host.getOffsetsType)
        assertResult(Int.MaxValue.toLong)(host.getEndListOffset(2047))
      }
      builder.appendUTF8String(value, 0, 1)
      builder.appendNull()
      val snapshot = builder.captureState()
      builder.appendNull()
      builder.appendUTF8String(value)
      builder.restoreState(snapshot)
      builder.append("ab")
      withResource(builder.build()) { host =>
        assertResult(DType.INT64)(host.getOffsetsType)
        assertResult(2051L)(host.getRowCount)
        assertResult(1L)(host.getNullCount)
        assert(host.isNull(2049))
        assert(!host.isNull(2050))
        assertResult(1L << 31)(host.getStartListOffset(2050))
        assertResult("ab")(host.getJavaString(2050))
      }
    }
  }

}
