/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import java.util.concurrent.{Callable, Future}

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuShuffleAsyncCoalesceIterator._

import org.apache.spark.TaskContext
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Similar as GpuShuffleCoalesceIterator, but pulling in host batches asynchronously, to
 * overlap the host batch reading and the downstream GPU operations.
 */
class GpuShuffleAsyncCoalesceIterator(iter: Iterator[CoalescedHostResult],
    dataTypes: Array[DataType],
    metricsMap: Map[String, GpuMetric]) extends Iterator[ColumnarBatch] {
  private[this] val opTimeMetric = metricsMap(GpuMetric.OP_TIME)
  private[this] val outputBatchesMetric = metricsMap(GpuMetric.NUM_OUTPUT_BATCHES)
  private[this] val outputRowsMetric = metricsMap(GpuMetric.NUM_OUTPUT_ROWS)
  private[this] val asyncWaitTimeMetric =
    metricsMap.getOrElse(SHUFFLE_ASYNC_WAIT_TIME, NoopMetric)

  private lazy val readExecutor =
    TrampolineUtil.newDaemonSingleThreadExecutor("async shuffle read")

  private lazy val readCallable = new Callable[CoalescedHostResult]() {
    // Get the task context of the task thread.
    private val tc = TaskContext.get()
    // The actual async read, including the host batches read and concatenation in
    // "HostCoalesceIteratorBase.next()".
    override def call(): CoalescedHostResult = {
      // Initialize the task context for the work thread in case the upstreams require it.
      if (TaskContext.get() == null) {
        TrampolineUtil.setTaskContext(tc)
      }
      iter.next()
    }
  }

  private var readFutureOpt: Option[Future[CoalescedHostResult]] = None

  override def hasNext(): Boolean = {
    readFutureOpt.isDefined || {
      // No async read is running when it comes here, so no need synchronization
      // when accessing the input iterator. "iter.hasNext" should be lightweight
      // enough, since it just read in a header which is very small.
      opTimeMetric.ns(iter.hasNext)
    }
  }

  override def next(): ColumnarBatch = {
    if (!hasNext()) {
      throw new NoSuchElementException("No more batches")
    }
    withResource(new NvtxRange("Concat+Load Batch", NvtxColor.RED)) { _ =>
      val hostConcatedRet = withResource(new MetricRange(opTimeMetric)) { _ =>
        readFutureOpt.map { readFuture =>
          // An async read is running, waiting for the result
          asyncWaitTimeMetric.ns(readFuture.get())
        }.getOrElse {
          // The first batch, just read it directly
          iter.next()
        }
      }
      val gpuCB = withResource(hostConcatedRet) { _ =>
        // We acquire the GPU regardless of whether the concatenated batch is an empty batch
        // or not, because the downstream tasks expect the `GpuShuffleCoalesceIterator`
        // to acquire the semaphore and may generate GPU data from batches that are empty.
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        withResource(new MetricRange(opTimeMetric))(_ => hostConcatedRet.toGpuBatch(dataTypes))
      }
      closeOnExcept(gpuCB) { _ =>
        opTimeMetric.ns {
          // No need synchronization here since the async read is already done.
          if (iter.hasNext) {
            // Prefetch and concatenate the next one asynchronously.
            readFutureOpt = Some(readExecutor.submit(readCallable))
          } else {
            readFutureOpt = None
          }
          outputBatchesMetric += 1
          outputRowsMetric += gpuCB.numRows()
          gpuCB
        }
      }
    }
  }

}

object GpuShuffleAsyncCoalesceIterator {
  // For the metric on async wait time
  val SHUFFLE_ASYNC_WAIT_TIME = "shuffleAsyncReadWaitTime"
  val DESCRIPTION_SHUFFLE_ASYNC_WAIT_TIME = "async wait time"
}
