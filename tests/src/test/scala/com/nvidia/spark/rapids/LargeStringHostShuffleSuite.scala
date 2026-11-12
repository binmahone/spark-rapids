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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.{Arrays, Collections, Optional}

import ai.rapids.cudf.{DType, HostColumnVector, HostMemoryBuffer}
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.vectorized.ColumnarBatch

class LargeStringHostShuffleSuite extends AnyFunSuite {
  private def strings(offsetsType: DType, ends: Seq[Long]): HostColumnVector = {
    val data = HostMemoryBuffer.allocate(math.max(ends.last, 1L), false)
    val offsets = HostMemoryBuffer.allocate(ends.length.toLong * offsetsType.getSizeInBytes, false)
    ends.zipWithIndex.foreach { case (end, i) =>
      if (offsetsType == DType.INT64) offsets.setLong(i * 8L, end)
      else offsets.setInt(i * 4L, Math.toIntExact(end))
    }
    new HostColumnVector(DType.STRING, ends.length - 1, Optional.of[java.lang.Long](0L), data,
      null, offsets, Collections.emptyList(), offsetsType)
  }

  private def sliceSize(offsetsType: DType, ends: Seq[Long], start: Int, end: Int): Long = {
    withResource(new RapidsHostColumnVector(StringType, strings(offsetsType, ends))) { host =>
      withResource(new ColumnarBatch(Array(new SlicedGpuColumnVector(host, start, end)),
          end - start)) { batch =>
        SlicedGpuColumnVector.getTotalHostMemoryUsed(batch)
      }
    }
  }

  test("ordinary STRING slices count four-byte offsets") {
    assertResult(12L + 5)(sliceSize(DType.INT32, Seq(0L, 3L, 5L), 0, 2))
  }

  test("wide STRING slices count eight-byte offsets") {
    assertResult(24L + 5)(sliceSize(DType.INT64, Seq(0L, 3L, 5L), 0, 2))
  }

  test("STRING byte length above Int.MaxValue remains positive") {
    val bytes = Int.MaxValue.toLong + 10
    assertResult(24L + bytes)(sliceSize(DType.INT64, Seq(0L, 10L, bytes), 0, 2))
  }

  test("STRING slice beginning above Int.MaxValue uses long byte positions") {
    val start = Int.MaxValue.toLong + 10
    assertResult(16L + 7)(sliceSize(DType.INT64, Seq(0L, start, start + 7), 1, 2))
  }

  test("text line buffering promotes offsets when data passes two GiB") {
    val line = new Array[Byte](1 << 20)
    Arrays.fill(line, 't'.toByte)
    withResource(new HostStringColBufferer(line.length, Array('\n'.toByte))) { buffer =>
      (0 until 2049).foreach(_ => buffer.add(line, 0, line.length))
      assertResult(2049L * line.length)(buffer.getLength)
      withResource(buffer.getColumnAndRelease) { column =>
        assertResult(DType.INT64)(column.getOffsetsType)
        withResource(column.copyToHost()) { host =>
          assertResult(2048L * line.length)(host.getStartListOffset(2048))
          assertResult(2049L * line.length)(host.getEndListOffset(2048))
          assert(Arrays.equals(line, host.getUTF8(2048)))
        }
      }
    }
  }

  test("Kudo iterator peeks a payload length above Int.MaxValue without allocation") {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    val payloadLength = Int.MaxValue.toLong + 100
    out.writeInt(0x4B554431)
    out.writeInt(0)
    out.writeInt(1)
    out.writeLong(0)
    out.writeLong(16)
    out.writeLong(payloadLength)
    out.writeInt(1)
    out.writeByte(0)
    out.writeByte(1)
    withResource(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray))) { in =>
      val iterator = new KudoSerializedBatchIterator(in, NoopMetric)
      assertResult(Some(payloadLength))(iterator.peekNextBatchSize())
      assertResult(Some(payloadLength))(iterator.peekNextBatchSize())
    }
  }
}
