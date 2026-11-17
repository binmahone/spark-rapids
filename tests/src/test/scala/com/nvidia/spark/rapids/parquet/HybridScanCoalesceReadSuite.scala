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

package com.nvidia.spark.rapids.parquet

import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile.CopyRange
import org.scalatest.funsuite.AnyFunSuite

class HybridScanCoalesceReadSuite extends AnyFunSuite {
  private def intervals(ranges: Seq[CopyRange]): Seq[(Long, Long, Long)] =
    ranges.map(r => (r.getInputOffset, r.getLength, r.getOutputOffset))

  test("a read starting at file offset zero is retained") {
    val result = GpuParquetScan.coalesceReads(Seq(new CopyRange(0L, 100L, 7L)))
    assert(intervals(result) == Seq((0L, 100L, 7L)))
  }

  test("adjacent reads from offset zero merge without losing the prefix") {
    val result = GpuParquetScan.coalesceReads(Seq(
      new CopyRange(0L, 100L, 0L),
      new CopyRange(100L, 50L, 100L),
      new CopyRange(9000L, 500L, 150L)))
    assert(intervals(result) == Seq((0L, 150L, 0L), (9000L, 500L, 150L)))
  }

  test("empty input and nonzero adjacent reads preserve their intervals") {
    assert(GpuParquetScan.coalesceReads(Seq.empty).isEmpty)
    val result = GpuParquetScan.coalesceReads(Seq(
      new CopyRange(4L, 20L, 0L), new CopyRange(24L, 12L, 20L)))
    assert(intervals(result) == Seq((4L, 32L, 0L)))
  }
}
