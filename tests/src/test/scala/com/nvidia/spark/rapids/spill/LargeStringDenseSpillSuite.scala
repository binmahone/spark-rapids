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

import java.nio.charset.StandardCharsets
import java.util.Arrays

import ai.rapids.cudf.{DType, HostColumnVector, Table}
import com.nvidia.spark.rapids.{RapidsConf, RmmSparkRetrySuiteBase}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.SparkConf

class LargeStringDenseSpillSuite extends RmmSparkRetrySuiteBase {
  test("STRING payload above two GiB preserves every byte after host and disk spill") {
    checkRoundTrip("4GB", throughHost = true)
  }

  test("STRING payload above two GiB preserves every byte after direct disk spill") {
    checkRoundTrip("1MB", throughHost = false)
  }

  private def checkRoundTrip(hostCapacity: String, throughHost: Boolean): Unit = {
    SpillFramework.shutdown()
    SpillFramework.initialize(new RapidsConf(new SparkConf()
      .set(RapidsConf.OFF_HEAP_LIMIT_ENABLED.key, "false")
      .set(RapidsConf.HOST_SPILL_STORAGE_SIZE.key, hostCapacity)))
    val rows = 2049
    val value = "x" * (1024 * 1024)
    val expected = value.getBytes(StandardCharsets.UTF_8)
    val table = withResource(HostColumnVector.builder(DType.STRING, rows)) { builder =>
      (0 until rows).foreach(_ => builder.append(value))
      withResource(builder.build()) { host =>
        assertResult(DType.INT64)(host.getOffsetsType)
        withResource(host.copyToDevice())(column => new Table(column))
      }
    }
    withResource(SpillableTableHandle(table)) { handle =>
      assert(handle.approxSizeInBytes > Int.MaxValue)
      assert(SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes) > 0)
      assert(handle.dev.isEmpty)
      assert(handle.host.isDefined)
      assertResult(throughHost)(handle.host.get.host.isDefined)
      val hostBytes = handle.host.get.sizeInBytes
      assert(hostBytes > Int.MaxValue)
      if (throughHost) {
        assert(SpillFramework.stores.hostStore.spill(hostBytes) > 0)
      }
      assert(handle.host.get.host.isEmpty)
      assert(handle.host.get.disk.isDefined)
      withResource(handle.materialize()) { restored =>
        assertResult(DType.INT64)(restored.getColumn(0).getOffsetsType)
        withResource(restored.getColumn(0).copyToHost()) { host =>
          assertResult(rows.toLong)(host.getRowCount)
          assertResult(rows.toLong * expected.length)(host.getOffsets.getLong(rows * 8L))
          (0 until rows).foreach { row =>
            assert(Arrays.equals(expected, host.getUTF8(row)), s"Row $row changed")
          }
        }
      }
      info(s"DENSE_SPILL_RESULT rows=$rows hostBytes=$hostBytes " +
        s"throughHost=$throughHost everyByte=PASS")
    }
  }
}
