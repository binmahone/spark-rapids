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

package com.nvidia.spark.rapids.shuffle

import ai.rapids.cudf.DeviceMemoryBuffer
import com.nvidia.spark.rapids.{AcquireFailed, GpuDeviceManager, GpuSemaphore, MetaUtils, RapidsConf, RapidsShuffleHandle, ScalableTaskCompletion, SemaphoreAcquired, ShuffleReceivedBufferCatalog, TaskRegistryTracker, TestUtils}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.spill.SpillableDeviceBufferHandle
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

import org.apache.spark.shuffle.rapids.{RapidsShuffleFetchFailedException, RapidsShuffleTimeoutException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.vectorized.ColumnarBatch

class RapidsShuffleWaitSuite extends RapidsShuffleTestHelper {
  override def beforeEach(): Unit = {
    super.beforeEach()
    ScalableTaskCompletion.reset()
    GpuSemaphore.shutdown()
    SparkSession.builder.master("local[1]").appName("shuffleWaitTests")
      .config("spark.rapids.sql.concurrentGpuTasks", "1")
      .config(RapidsConf.DYNAMIC_CONCURRENT_GPU_TASKS.key, "false")
      .getOrCreate()
    GpuDeviceManager.setRmmTaskInitEnabled(false)
  }

  override def afterEach(): Unit = {
    try {
      TaskRegistryTracker.clearRegistry()
      ScalableTaskCompletion.reset()
    } finally {
      super.afterEach()
    }
  }

  Seq("ready", "delayed", "timeout", "transfer-error", "interrupted", "acquire-error")
    .foreach { outcome =>
      test(s"shuffle queue ownership and GPU permit: $outcome") {
        val context = spy(new MockTaskContext(taskAttemptId = 1, partitionId = 0))
        val competitor = new MockTaskContext(taskAttemptId = 2, partitionId = 0)
        def permitAvailable: Boolean = GpuSemaphore.tryAcquire(competitor) match {
          case SemaphoreAcquired =>
            GpuSemaphore.releaseIfNecessary(competitor)
            true
          case AcquireFailed(_) => false
        }
        // Initialize the stage estimator outside the mock task's SQLConf.
        GpuSemaphore.acquireIfNecessary(context)
        assert(!permitAvailable)
        var owned: RapidsShuffleHandle = null
        var device: DeviceMemoryBuffer = null
        try {
          TestUtils.withTaskContext(context) {
            val catalog = spy(new ShuffleReceivedBufferCatalog)
            val iterator = RapidsShuffleTestHelper.makeIterator(
              mockConf, mockTransport, testMetricsUpdater, 1, catalog)
            val capture = ArgumentCaptor.forClass(classOf[RapidsShuffleFetchHandler])
            when(mockTransport.makeClient(any())).thenReturn(client)
            doNothing().when(client).doFetch(any(), capture.capture())
            iterator.start()
            // Prevent a second registration when next() starts the iterator.
            doNothing().when(iterator).start()
            val handler = capture.getValue
            handler.start(1)
            if (outcome == "acquire-error") {
              device = DeviceMemoryBuffer.allocate(128)
              owned = RapidsShuffleHandle(SpillableDeviceBufferHandle(device), null)
            } else {
              withResource(new ColumnarBatch(Array.empty, 7)) { batch =>
                owned = catalog.addDegenerateBatch(MetaUtils.buildDegenerateTableMeta(batch))
              }
            }
            doAnswer { invocation =>
              assert(!permitAvailable, "materialization must own the GPU permit")
              invocation.callRealMethod()
            }.when(catalog).getColumnarBatchAndRemove(any(), any())
            var waited = false
            if (outcome == "ready") {
              assert(handler.batchReceived(owned))
            } else {
              doAnswer { invocation =>
                waited = true
                assert(permitAvailable, "a blocked fetch must let another task use the GPU")
                outcome match {
                  case "timeout" => None
                  case "interrupted" => throw new InterruptedException("poll interrupted")
                  case "transfer-error" =>
                    handler.transferError("test fetch error", null)
                    invocation.callRealMethod()
                  case _ =>
                    assert(handler.batchReceived(owned))
                    if (outcome == "acquire-error") {
                      doThrow(new IllegalStateException("acquire failed"))
                        .when(context).stageId()
                    }
                    invocation.callRealMethod()
                }
              }.when(iterator).pollForResult(anyLong())
            }
            outcome match {
              case "timeout" => intercept[RapidsShuffleTimeoutException](iterator.next())
              case "transfer-error" =>
                intercept[RapidsShuffleFetchFailedException](iterator.next())
              case "interrupted" => intercept[InterruptedException](iterator.next())
              case "acquire-error" =>
                assert(intercept[IllegalStateException](iterator.next()).getMessage ==
                  "acquire failed")
                assert(device.getRefCount == 0,
                  "a dequeued handle must close if reacquisition fails")
                verify(catalog, never()).getColumnarBatchAndRemove(any(), any())
              case _ =>
                withResource(iterator.next()) { batch =>
                  assert(batch.numRows() == 7)
                  assert(!permitAvailable, "the returned batch must own the GPU permit")
                }
            }
            assert(waited == (outcome != "ready"))
            if (outcome != "ready" && outcome != "delayed") {
              assert(permitAvailable, "a failed fetch must leave the GPU available")
            }
          }
        } finally {
          // The degenerate handle has no device resource; the acquire failure owns a real one.
          if (device != null && device.getRefCount > 0) {
            owned.close()
          }
          GpuSemaphore.releaseIfNecessary(context)
          GpuSemaphore.releaseIfNecessary(competitor)
        }
      }
    }
}
