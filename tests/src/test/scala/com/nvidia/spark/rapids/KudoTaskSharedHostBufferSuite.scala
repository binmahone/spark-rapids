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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.HostMemoryBuffer
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase._

class KudoTaskSharedHostBufferSuite extends AnyFunSuite {
  private def metricMap: Map[String, GpuMetric] = Map(
    METRIC_SHUFFLE_KUDO_TASK_SHARED_SAMPLE_COUNT -> new LocalGpuMetric,
    METRIC_SHUFFLE_KUDO_TASK_SHARED_THRESHOLD_REJECT_COUNT -> new LocalGpuMetric,
    METRIC_SHUFFLE_KUDO_TASK_SHARED_BUFFER_CREATE_COUNT -> new LocalGpuMetric,
    METRIC_SHUFFLE_KUDO_TASK_SHARED_BUFFER_SLICE_COUNT -> new LocalGpuMetric,
    METRIC_SHUFFLE_KUDO_TASK_DEDICATED_BUFFER_COUNT -> new LocalGpuMetric,
    METRIC_SHUFFLE_KUDO_TASK_HOST_ALLOCATION_COUNT -> new LocalGpuMetric,
    METRIC_SHUFFLE_KUDO_TASK_HOST_ALLOCATION_BYTES -> new LocalGpuMetric,
    METRIC_SHUFFLE_KUDO_TASK_HOST_ALLOCATION_TIME -> new LocalGpuMetric,
    METRIC_SHUFFLE_KUDO_TASK_SHARED_LOCK_WAIT_TIME -> new LocalGpuMetric)

  private def closeAll(buffers: scala.collection.Seq[HostMemoryBuffer]): Unit = {
    buffers.foreach(_.close())
  }

  private def mockHostAllocator(
      allocations: ArrayBuffer[HostMemoryBuffer],
      slices: ArrayBuffer[HostMemoryBuffer]): Int => HostMemoryBuffer = { _ =>
    val root = mock[HostMemoryBuffer]
    when(root.slice(anyLong(), anyLong())).thenAnswer { _ =>
      val slice = mock[HostMemoryBuffer]
      slices += slice
      slice
    }
    allocations += root
    root
  }

  test("shares host allocations across single-table block iterators") {
    val metrics = metricMap
    val allocations = new ArrayBuffer[HostMemoryBuffer]
    val slices = new ArrayBuffer[HostMemoryBuffer]
    val allocator = new KudoTaskSharedHostBuffer(
      metrics, triggerSize = 4 << 20, sampleCount = 5,
      minBufferSize = 1 << 20, maxBufferSize = 20 << 20,
      testHostAllocator = Some(mockHostAllocator(allocations, slices)))
    val buffers = new ArrayBuffer[HostMemoryBuffer]
    try {
      (0 until 9).foreach { _ =>
        buffers += allocator.acquire(2 << 20)
      }

      assertResult(5)(metrics(METRIC_SHUFFLE_KUDO_TASK_SHARED_SAMPLE_COUNT).value)
      assertResult(5)(metrics(METRIC_SHUFFLE_KUDO_TASK_DEDICATED_BUFFER_COUNT).value)
      assertResult(1)(metrics(METRIC_SHUFFLE_KUDO_TASK_SHARED_BUFFER_CREATE_COUNT).value)
      assertResult(4)(metrics(METRIC_SHUFFLE_KUDO_TASK_SHARED_BUFFER_SLICE_COUNT).value)
      assertResult(6)(metrics(METRIC_SHUFFLE_KUDO_TASK_HOST_ALLOCATION_COUNT).value)
    } finally {
      allocator.close()
      closeAll(buffers)
    }
  }

  test("rotating and closing roots preserves live slices") {
    val metrics = metricMap
    val allocations = new ArrayBuffer[HostMemoryBuffer]
    val slices = new ArrayBuffer[HostMemoryBuffer]
    val allocator = new KudoTaskSharedHostBuffer(
      metrics, triggerSize = 4 << 20, sampleCount = 1,
      minBufferSize = 4 << 20, maxBufferSize = 4 << 20,
      testHostAllocator = Some(mockHostAllocator(allocations, slices)))
    val buffers = new ArrayBuffer[HostMemoryBuffer]
    try {
      buffers += allocator.acquire(2 << 20)
      val firstSlice = allocator.acquire(1 << 20)
      buffers += firstSlice
      buffers += allocator.acquire(1 << 20)
      buffers += allocator.acquire(1 << 20)
      buffers += allocator.acquire(1 << 20)
      buffers += allocator.acquire(1 << 20)

      allocator.close()
      verify(allocations(1), times(1)).close()
      verify(firstSlice, never()).close()
      assert(metrics(METRIC_SHUFFLE_KUDO_TASK_SHARED_BUFFER_CREATE_COUNT).value >= 2)
    } finally {
      allocator.close()
      closeAll(buffers)
    }
  }

  test("rejects sharing when sampled tables reach the trigger") {
    val metrics = metricMap
    val allocations = new ArrayBuffer[HostMemoryBuffer]
    val slices = new ArrayBuffer[HostMemoryBuffer]
    val allocator = new KudoTaskSharedHostBuffer(
      metrics, triggerSize = 4 << 20, sampleCount = 2,
      minBufferSize = 1 << 20, maxBufferSize = 20 << 20,
      testHostAllocator = Some(mockHostAllocator(allocations, slices)))
    val buffers = new ArrayBuffer[HostMemoryBuffer]
    try {
      buffers += allocator.acquire(4 << 20)
      buffers += allocator.acquire(4 << 20)
      buffers += allocator.acquire(2 << 20)

      assertResult(1)(metrics(METRIC_SHUFFLE_KUDO_TASK_SHARED_THRESHOLD_REJECT_COUNT).value)
      assertResult(0)(metrics(METRIC_SHUFFLE_KUDO_TASK_SHARED_BUFFER_SLICE_COUNT).value)
      assertResult(3)(metrics(METRIC_SHUFFLE_KUDO_TASK_DEDICATED_BUFFER_COUNT).value)
    } finally {
      allocator.close()
      closeAll(buffers)
    }
  }

  test("maps reader threads deterministically across configured stripes") {
    val allocator = new KudoTaskSharedHostBufferSet(
      metricMap, triggerSize = 4 << 20, targetTableCount = 32,
      maxBufferSize = 64 << 20, stripeCount = 4)
    try {
      assertResult(Seq(0, 1, 2, 3, 0, 1, 2, 3)) {
        (0L until 8L).map(allocator.stripeIndex)
      }
    } finally {
      allocator.close()
    }
  }
}
