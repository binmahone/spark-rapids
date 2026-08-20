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
      decisionWindowTasks = 4, stableTargetWindows = 2, maxAdjustmentStep = 1,
      detailedLoggingEnabled = false)
  }

  private def taskContext(taskAttemptId: Long, stageId: Int = 1): TaskContext = {
    val context = mock(classOf[TaskContext])
    when(context.taskAttemptId()).thenReturn(taskAttemptId)
    when(context.stageId()).thenReturn(stageId)
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

  test("adaptive admission slowly tracks a stable GPU-derived target") {
    val snapshot = new AtomicReference[GpuConcurrencySnapshot](gpuSnapshot(4))
    val config = ReaderTaskAdmissionConfig(
      initialConcurrentTasks = 4,
      adaptiveEnabled = true,
      minConcurrentTasks = 2,
      maxConcurrentTasks = 8,
      gpuConcurrencyMultiplier = 1.5,
      decisionWindowTasks = 2,
      stableTargetWindows = 2,
      maxAdjustmentStep = 1,
      detailedLoggingEnabled = false)
    val gate = new ReaderTaskAdmissionGate(config, _ => (), _ => snapshot.get())

    def complete(taskId: Long, observation: ReaderTaskObservation) = {
      val context = taskContext(taskId)
      assert(gate.acquire(context).acquired)
      gate.releaseReference(context, observation)
    }

    assert(complete(1L, ReaderTaskObservation(1L, 100L, 10L, 0L)).isEmpty)
    val stabilizing = complete(2L, ReaderTaskObservation(1L, 100L, 10L, 0L)).get
    assert(stabilizing.reason === "gpu-target-stabilizing")
    assert(stabilizing.gpuTarget === 6)
    assert(stabilizing.oldPermits === 4)
    assert(stabilizing.newPermits === 4)

    assert(complete(3L, ReaderTaskObservation(50L, 100L, 10L, 4L)).isEmpty)
    val firstIncrease = complete(4L, ReaderTaskObservation(50L, 100L, 10L, 4L)).get
    assert(firstIncrease.reason === "gpu-target-increase")
    assert(firstIncrease.oldPermits === 4)
    assert(firstIncrease.newPermits === 5)

    assert(complete(5L, ReaderTaskObservation(50L, 100L, 10L, 4L)).isEmpty)
    val secondIncrease = complete(6L, ReaderTaskObservation(50L, 100L, 10L, 4L)).get
    assert(secondIncrease.reason === "gpu-target-increase")
    assert(secondIncrease.oldPermits === 5)
    assert(secondIncrease.newPermits === 6)
    assert(gate.currentDesiredPermits === 6)

    snapshot.set(gpuSnapshot(1))
    assert(complete(7L, ReaderTaskObservation(1L, 100L, 10L, 0L)).isEmpty)
    val newTarget = complete(8L, ReaderTaskObservation(1L, 100L, 10L, 0L)).get
    assert(newTarget.reason === "gpu-target-stabilizing")
    assert(newTarget.newPermits === 6)
    assert(complete(9L, emptyObservation).isEmpty)
    val firstDecrease = complete(10L, emptyObservation).get
    assert(firstDecrease.reason === "gpu-target-decrease")
    assert(firstDecrease.newPermits === 5)
  }

  test("adaptive admission converges with a bounded multi-permit adjustment") {
    val snapshot = new AtomicReference[GpuConcurrencySnapshot](gpuSnapshot(4))
    val config = ReaderTaskAdmissionConfig(
      initialConcurrentTasks = 4,
      adaptiveEnabled = true,
      minConcurrentTasks = 2,
      maxConcurrentTasks = 8,
      gpuConcurrencyMultiplier = 2.0,
      decisionWindowTasks = 2,
      stableTargetWindows = 2,
      maxAdjustmentStep = 2,
      detailedLoggingEnabled = false)
    val gate = new ReaderTaskAdmissionGate(config, _ => (), _ => snapshot.get())

    def complete(taskId: Long) = {
      val context = taskContext(taskId)
      assert(gate.acquire(context).acquired)
      gate.releaseReference(context, emptyObservation)
    }

    assert(complete(1L).isEmpty)
    val stabilizing = complete(2L).get
    assert(stabilizing.reason === "gpu-target-stabilizing")
    assert(stabilizing.oldPermits === 4)
    assert(stabilizing.newPermits === 4)

    assert(complete(3L).isEmpty)
    val firstIncrease = complete(4L).get
    assert(firstIncrease.reason === "gpu-target-increase")
    assert(firstIncrease.oldPermits === 4)
    assert(firstIncrease.newPermits === 6)

    assert(complete(5L).isEmpty)
    val secondIncrease = complete(6L).get
    assert(secondIncrease.oldPermits === 6)
    assert(secondIncrease.newPermits === 8)

    snapshot.set(gpuSnapshot(1))
    assert(complete(7L).isEmpty)
    val newTarget = complete(8L).get
    assert(newTarget.reason === "gpu-target-stabilizing")
    assert(newTarget.newPermits === 8)
    assert(complete(9L).isEmpty)
    val firstDecrease = complete(10L).get
    assert(firstDecrease.reason === "gpu-target-decrease")
    assert(firstDecrease.oldPermits === 8)
    assert(firstDecrease.newPermits === 6)
  }

  test("bounded adjustment reaches FINRA-sized stage targets") {
    val snapshot = new AtomicReference[GpuConcurrencySnapshot](gpuSnapshot(4))
    val config = ReaderTaskAdmissionConfig(
      initialConcurrentTasks = 4,
      adaptiveEnabled = true,
      minConcurrentTasks = 2,
      maxConcurrentTasks = 16,
      gpuConcurrencyMultiplier = 2.0,
      decisionWindowTasks = 4,
      stableTargetWindows = 2,
      maxAdjustmentStep = 2,
      detailedLoggingEnabled = false)
    val gate = new ReaderTaskAdmissionGate(config, _ => (), _ => snapshot.get())
    var taskId = 0L

    def completeStage(stageId: Int, gpuCapacity: Int): Seq[ReaderTaskAdmissionDecision] = {
      snapshot.set(gpuSnapshot(gpuCapacity))
      (1 to 16).flatMap { _ =>
        taskId += 1
        val context = taskContext(taskId, stageId)
        assert(gate.acquire(context).acquired)
        gate.releaseReference(context, emptyObservation)
      }
    }

    val stageFour = completeStage(stageId = 4, gpuCapacity = 4)
    assert(stageFour.map(_.newPermits) === Seq(4, 6, 8, 8))

    val stageEight = completeStage(stageId = 8, gpuCapacity = 6)
    assert(stageEight.map(_.newPermits) === Seq(8, 10, 12, 12))

    val stageSixteen = completeStage(stageId = 16, gpuCapacity = 4)
    assert(stageSixteen.map(_.newPermits) === Seq(12, 10, 8, 8))

    val stageTwenty = completeStage(stageId = 20, gpuCapacity = 6)
    assert(stageTwenty.map(_.newPermits) === Seq(8, 10, 12, 12))
  }

  test("adaptive admission discards mixed-stage decision windows") {
    val config = ReaderTaskAdmissionConfig(
      initialConcurrentTasks = 4,
      adaptiveEnabled = true,
      minConcurrentTasks = 2,
      maxConcurrentTasks = 8,
      gpuConcurrencyMultiplier = 2.0,
      decisionWindowTasks = 2,
      stableTargetWindows = 1,
      maxAdjustmentStep = 1,
      detailedLoggingEnabled = false)
    val gate = new ReaderTaskAdmissionGate(config, _ => (), _ => gpuSnapshot(3))

    def complete(taskId: Long, stageId: Int) = {
      val context = taskContext(taskId, stageId)
      assert(gate.acquire(context).acquired)
      gate.releaseReference(context, emptyObservation)
    }

    assert(complete(1L, stageId = 1).isEmpty)
    assert(complete(2L, stageId = 2).isEmpty)
    val decision = complete(3L, stageId = 2).get
    assert(decision.reason === "gpu-target-increase")
    assert(decision.oldPermits === 4)
    assert(decision.newPermits === 5)
  }

  test("adaptive admission restabilizes the target at a completed stage boundary") {
    val config = ReaderTaskAdmissionConfig(
      initialConcurrentTasks = 4,
      adaptiveEnabled = true,
      minConcurrentTasks = 2,
      maxConcurrentTasks = 8,
      gpuConcurrencyMultiplier = 2.0,
      decisionWindowTasks = 2,
      stableTargetWindows = 2,
      maxAdjustmentStep = 1,
      detailedLoggingEnabled = false)
    val gate = new ReaderTaskAdmissionGate(config, _ => (), _ => gpuSnapshot(3))

    def complete(taskId: Long, stageId: Int) = {
      val context = taskContext(taskId, stageId)
      assert(gate.acquire(context).acquired)
      gate.releaseReference(context, emptyObservation)
    }

    assert(complete(1L, stageId = 1).isEmpty)
    assert(complete(2L, stageId = 1).get.reason === "gpu-target-stabilizing")
    assert(complete(3L, stageId = 1).isEmpty)
    assert(complete(4L, stageId = 1).get.reason === "gpu-target-increase")

    assert(complete(5L, stageId = 2).isEmpty)
    val firstStageTwoDecision = complete(6L, stageId = 2).get
    assert(firstStageTwoDecision.reason === "gpu-target-stabilizing")
    assert(firstStageTwoDecision.oldPermits === 5)
    assert(firstStageTwoDecision.newPermits === 5)
  }
}
