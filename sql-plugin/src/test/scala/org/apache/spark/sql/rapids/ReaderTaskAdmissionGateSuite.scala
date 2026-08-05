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

package org.apache.spark.sql.rapids

import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import com.nvidia.spark.rapids.{GpuConcurrencySnapshot, ScalableTaskCompletion}
import org.mockito.Mockito.{mock, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.TaskContext

class ReaderTaskAdmissionGateSuite extends AnyFunSuite with BeforeAndAfterEach {

  private val emptyObservation = ReaderTaskObservation(0L, 0L, 0L, 0L)

  private def fixedConfig(permits: Int): ReaderTaskAdmissionConfig = {
    ReaderTaskAdmissionConfig(
      permits, adaptiveEnabled = false, permits, permits, 2.0,
      decisionWindowTasks = 4, detailedLoggingEnabled = false)
  }

  private def taskContext(taskAttemptId: Long): TaskContext = {
    val context = mock(classOf[TaskContext])
    when(context.taskAttemptId()).thenReturn(taskAttemptId)
    when(context.stageId()).thenReturn(1)
    context
  }

  private def gpuSnapshot(capacity: Int): GpuConcurrencySnapshot = {
    GpuConcurrencySnapshot(
      capacity, activeTasks = 0L, waitingTasks = 0, occupiedPermits = 0L,
      maxPermits = 100L, taskPermitsEstimate = 10L, hardTaskLimit = 0)
  }

  override def afterEach(): Unit = {
    ScalableTaskCompletion.reset()
  }

  test("reader admission is task-reentrant and blocks excess tasks") {
    val releaseCount = new AtomicInteger()
    val releasedGpu = new CountDownLatch(2)
    val gate = new ReaderTaskAdmissionGate(fixedConfig(1), _ => {
      releaseCount.incrementAndGet()
      releasedGpu.countDown()
    })
    val firstContext = taskContext(1L)
    val secondContext = taskContext(2L)
    val first = gate.acquire(firstContext)
    assert(first.acquired)
    assert(gate.availablePermits === 0)
    assert(!gate.acquire(firstContext).acquired)
    assert(releaseCount.get() === 1)

    val executor = Executors.newSingleThreadExecutor()
    val started = new CountDownLatch(1)
    try {
      val second = executor.submit(new Callable[ReaderTaskAdmissionResult] {
        override def call(): ReaderTaskAdmissionResult = {
          started.countDown()
          gate.acquire(secondContext)
        }
      })
      assert(started.await(5, TimeUnit.SECONDS))
      assert(releasedGpu.await(5, TimeUnit.SECONDS))
      assert(releaseCount.get() === 2)
      intercept[TimeoutException] {
        second.get(100, TimeUnit.MILLISECONDS)
      }

      gate.releaseReference(firstContext, emptyObservation)
      assert(gate.availablePermits === 0)
      gate.releaseReference(firstContext, emptyObservation)
      assert(second.get(5, TimeUnit.SECONDS).acquired)
      assert(gate.availablePermits === 0)
      gate.releaseReference(secondContext, emptyObservation)
      assert(gate.availablePermits === 1)
    } finally {
      executor.shutdownNow()
    }
  }

  test("interrupted admission does not leak task identity or permits") {
    val gate = new ReaderTaskAdmissionGate(fixedConfig(1), _ => ())
    val firstContext = taskContext(1L)
    val secondContext = taskContext(2L)
    assert(gate.acquire(firstContext).acquired)

    val executor = Executors.newSingleThreadExecutor()
    val started = new CountDownLatch(1)
    try {
      val blocked = executor.submit(new Callable[ReaderTaskAdmissionResult] {
        override def call(): ReaderTaskAdmissionResult = {
          started.countDown()
          gate.acquire(secondContext)
        }
      })
      assert(started.await(5, TimeUnit.SECONDS))
      assert(blocked.cancel(true))
      executor.submit(new Runnable {
        override def run(): Unit = {}
      }).get(5, TimeUnit.SECONDS)
      gate.releaseReference(firstContext, emptyObservation)

      val retry = gate.acquire(secondContext)
      assert(retry.acquired)
      gate.releaseReference(secondContext, emptyObservation)
      assert(gate.availablePermits === 1)
    } finally {
      executor.shutdownNow()
    }
  }

  test("adaptive admission changes direction from measured reader pressure") {
    val snapshot = new AtomicReference[GpuConcurrencySnapshot](gpuSnapshot(4))
    val config = ReaderTaskAdmissionConfig(
      initialConcurrentTasks = 4,
      adaptiveEnabled = true,
      minConcurrentTasks = 2,
      maxConcurrentTasks = 8,
      gpuConcurrencyMultiplier = 1.5,
      decisionWindowTasks = 2,
      detailedLoggingEnabled = false)
    val gate = new ReaderTaskAdmissionGate(config, _ => (), _ => snapshot.get())

    def complete(taskId: Long, observation: ReaderTaskObservation) = {
      val context = taskContext(taskId)
      assert(gate.acquire(context).acquired)
      gate.releaseReference(context, observation)
    }

    assert(complete(1L, ReaderTaskObservation(1L, 100L, 10L, 0L)).isEmpty)
    val increase = complete(2L, ReaderTaskObservation(1L, 100L, 10L, 0L)).get
    assert(increase.reason === "low-pressure-increase")
    assert(increase.gpuCeiling === 6)
    assert(increase.oldPermits === 4)
    assert(increase.newPermits === 5)
    assert(gate.currentDesiredPermits === 5)

    assert(complete(3L, ReaderTaskObservation(50L, 100L, 10L, 4L)).isEmpty)
    val hysteresis = complete(4L, ReaderTaskObservation(50L, 100L, 10L, 4L)).get
    assert(hysteresis.reason === "reader-pressure-hysteresis-hold")
    assert(hysteresis.oldPermits === 5)
    assert(hysteresis.newPermits === 5)

    assert(complete(5L, ReaderTaskObservation(50L, 100L, 10L, 4L)).isEmpty)
    val decrease = complete(6L, ReaderTaskObservation(50L, 100L, 10L, 4L)).get
    assert(decrease.reason === "reader-pressure-decrease")
    assert(decrease.oldPermits === 5)
    assert(decrease.newPermits === 4)
    assert(gate.currentDesiredPermits === 4)

    snapshot.set(gpuSnapshot(1))
    assert(complete(7L, ReaderTaskObservation(1L, 100L, 10L, 0L)).isEmpty)
    val clamp = complete(8L, ReaderTaskObservation(1L, 100L, 10L, 0L)).get
    assert(clamp.reason === "gpu-ceiling-clamp")
    assert(clamp.newPermits === 2)
    assert(gate.currentDesiredPermits === 2)
  }

  test("adaptive admission does not decrease from queue delay alone") {
    val config = ReaderTaskAdmissionConfig(
      initialConcurrentTasks = 4,
      adaptiveEnabled = true,
      minConcurrentTasks = 2,
      maxConcurrentTasks = 8,
      gpuConcurrencyMultiplier = 2.0,
      decisionWindowTasks = 2,
      detailedLoggingEnabled = false)
    val gate = new ReaderTaskAdmissionGate(config, _ => (), _ => gpuSnapshot(4))

    def complete(taskId: Long) = {
      val context = taskContext(taskId)
      assert(gate.acquire(context).acquired)
      gate.releaseReference(context, ReaderTaskObservation(
        workerQueueDelayNs = 50L,
        workerActiveNs = 100L,
        limiterAcquires = 10L,
        limiterFailures = 0L))
    }

    assert(complete(1L).isEmpty)
    val first = complete(2L).get
    assert(first.reason === "hold")
    assert(first.newPermits === 4)
    assert(complete(3L).isEmpty)
    val second = complete(4L).get
    assert(second.reason === "hold")
    assert(second.newPermits === 4)
  }
}
