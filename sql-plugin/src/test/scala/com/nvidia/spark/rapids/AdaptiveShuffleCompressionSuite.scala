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

import org.scalatest.funsuite.AnyFunSuite

class AdaptiveShuffleCompressionSuite extends AnyFunSuite {

  private class TestGpuReservation extends GpuCompressionReservation {
    private var held = false
    var acquireCount = 0
    var releaseCount = 0

    override def tryAcquire(): Boolean = synchronized {
      acquireCount += 1
      if (held) {
        false
      } else {
        held = true
        true
      }
    }

    override def release(): Unit = synchronized {
      assert(held)
      held = false
      releaseCount += 1
    }
  }

  test("CPU and GPU plans use one compression owner and the same wire encoding") {
    val cpu = TaskCompressionPlan(ShuffleCompressionBackend.SparkCpuZstd)
    val gpu = TaskCompressionPlan(ShuffleCompressionBackend.NvcompGpuZstd)

    assert(cpu.useSparkCompressionWrapper)
    assert(!cpu.useGpuCompressor)
    assert(!gpu.useSparkCompressionWrapper)
    assert(gpu.useGpuCompressor)
    assertResult(cpu.encoding)(gpu.encoding)
  }

  test("first backend decision remains sticky for a task") {
    val reservation = new TestGpuReservation
    val state = new TaskCompressionPlanState(reservation)
    var proposalCount = 0

    val first = state.getOrFreeze(
      adaptiveGpuCompressionEnabled = true, {
        proposalCount += 1
        ShuffleCompressionBackend.NvcompGpuZstd
      })
    val afterPressureChanged = state.getOrFreeze(
      adaptiveGpuCompressionEnabled = true, {
        proposalCount += 1
        ShuffleCompressionBackend.SparkCpuZstd
      })

    assertResult(first)(afterPressureChanged)
    assertResult(Some(first))(state.get)
    assertResult(1)(proposalCount)
    assertResult(1)(reservation.acquireCount)
    assert(state.gpuReservationHeldTimeNs > 0L)

    state.close()
    assertResult(1)(reservation.releaseCount)
  }

  test("disabled global switch preserves the CPU compression path") {
    val state = new TaskCompressionPlanState()
    var evaluatedProposal = false

    val plan = state.getOrFreeze(
      adaptiveGpuCompressionEnabled = false, {
        evaluatedProposal = true
        ShuffleCompressionBackend.NvcompGpuZstd
      })

    assertResult(ShuffleCompressionBackend.SparkCpuZstd)(plan.backend)
    assert(!evaluatedProposal)
  }

  test("logging and event reporting each claim a task decision once") {
    val state = new TaskCompressionPlanState()

    assert(state.markDecisionForLogging())
    assert(!state.markDecisionForLogging())
    assert(state.markDecisionForReporting())
    assert(!state.markDecisionForReporting())
  }

  test("adaptive GPU compression is disabled by default and can be enabled") {
    val key = RapidsConf.MULTITHREADED_SHUFFLE_ADAPTIVE_GPU_COMPRESSION.key
    val maxTasksKey =
      RapidsConf.MULTITHREADED_SHUFFLE_ADAPTIVE_GPU_COMPRESSION_MAX_CONCURRENT_TASKS.key
    val maxWaitersKey =
      RapidsConf.MULTITHREADED_SHUFFLE_ADAPTIVE_GPU_COMPRESSION_MAX_GPU_SEMAPHORE_WAITERS.key
    val releaseAfterGpuPhaseKey =
      RapidsConf.MULTITHREADED_SHUFFLE_ADAPTIVE_GPU_COMPRESSION_RELEASE_AFTER_GPU_PHASE.key

    assert(!new RapidsConf(Map.empty[String, String])
      .isMultithreadedShuffleAdaptiveGpuCompressionEnabled)
    assert(new RapidsConf(Map(key -> "true"))
      .isMultithreadedShuffleAdaptiveGpuCompressionEnabled)
    assertResult(1)(new RapidsConf(Map.empty[String, String])
      .multithreadedShuffleAdaptiveGpuCompressionMaxConcurrentTasks)
    assertResult(2)(new RapidsConf(Map(maxTasksKey -> "2"))
      .multithreadedShuffleAdaptiveGpuCompressionMaxConcurrentTasks)
    assertResult(0)(new RapidsConf(Map.empty[String, String])
      .multithreadedShuffleAdaptiveGpuCompressionMaxGpuSemaphoreWaiters)
    assertResult(16)(new RapidsConf(Map(maxWaitersKey -> "16"))
      .multithreadedShuffleAdaptiveGpuCompressionMaxGpuSemaphoreWaiters)
    assert(!new RapidsConf(Map.empty[String, String])
      .multithreadedShuffleAdaptiveGpuCompressionReleaseAfterGpuPhase)
    assert(new RapidsConf(Map(releaseAfterGpuPhaseKey -> "true"))
      .multithreadedShuffleAdaptiveGpuCompressionReleaseAfterGpuPhase)
  }

  test("experimental GPU-phase release is fail-closed for a second phase in one task") {
    val reservation = new TestGpuReservation
    val state = new TaskCompressionPlanState(reservation)

    val plan = state.getOrFreeze(
      adaptiveGpuCompressionEnabled = true,
      ShuffleCompressionBackend.NvcompGpuZstd)
    assert(plan.useGpuCompressor)

    state.releaseGpuReservationAfterCompression()
    assertResult(1)(reservation.releaseCount)
    assertThrows[IllegalArgumentException] {
      state.getOrFreeze(
        adaptiveGpuCompressionEnabled = true,
        ShuffleCompressionBackend.NvcompGpuZstd)
    }

    state.close()
    assertResult(1)(reservation.releaseCount)
  }

  test("GPU proposal requires CPU backlog and waiter count within the configured bound") {
    val cpuBackloggedGpuAvailable = AdaptiveCompressionPressure(
      writerPoolSize = 20,
      activeWriterThreads = 20,
      queuedWriterTasks = 3,
      gpuSemaphoreWaiters = 0)
    assert(cpuBackloggedGpuAvailable.proposesGpu(maxGpuSemaphoreWaiters = 0))

    assert(!cpuBackloggedGpuAvailable.copy(queuedWriterTasks = 0)
      .proposesGpu(maxGpuSemaphoreWaiters = 0))
    assert(!cpuBackloggedGpuAvailable.copy(activeWriterThreads = 19)
      .proposesGpu(maxGpuSemaphoreWaiters = 0))
    assert(!cpuBackloggedGpuAvailable.copy(gpuSemaphoreWaiters = 1)
      .proposesGpu(maxGpuSemaphoreWaiters = 0))
    assert(cpuBackloggedGpuAvailable.copy(gpuSemaphoreWaiters = 16)
      .proposesGpu(maxGpuSemaphoreWaiters = 16))
    assert(!cpuBackloggedGpuAvailable.copy(gpuSemaphoreWaiters = 17)
      .proposesGpu(maxGpuSemaphoreWaiters = 16))
  }

  test("executor controller ramps up, holds its learned route, and backs off") {
    val controller = new AdaptiveGpuCompressionController(
      maxConcurrentTasks = 8,
      maxGpuSemaphoreWaiters = 2)
    val healthy = AdaptiveCompressionPressure(
      writerPoolSize = 20,
      activeWriterThreads = 20,
      queuedWriterTasks = 3,
      gpuSemaphoreWaiters = 0)

    assertResult(1)(controller.observe(healthy).targetConcurrentTasks)
    assertResult(2)(controller.observe(healthy).targetConcurrentTasks)
    assertResult(2)(controller.observe(healthy).targetConcurrentTasks)
    assertResult(4)(controller.observe(healthy).targetConcurrentTasks)
    assertResult(4)(controller.observe(healthy).targetConcurrentTasks)
    assertResult(8)(controller.observe(healthy).targetConcurrentTasks)

    val drainedCpuQueue = healthy.copy(activeWriterThreads = 4, queuedWriterTasks = 0)
    val learned = controller.observe(drainedCpuQueue)
    assert(learned.proposeGpu)
    assertResult("learned-gpu-route")(learned.reason)
    assertResult(8)(learned.targetConcurrentTasks)

    val overloaded = drainedCpuQueue.copy(gpuSemaphoreWaiters = 3)
    assertResult(8)(controller.observe(overloaded).targetConcurrentTasks)
    val firstBackoff = controller.observe(overloaded)
    assert(!firstBackoff.proposeGpu)
    assertResult(4)(firstBackoff.targetConcurrentTasks)
    controller.observe(overloaded)
    assertResult(2)(controller.observe(overloaded).targetConcurrentTasks)
  }

  test("executor controller does not learn a GPU route without CPU backlog") {
    val controller = new AdaptiveGpuCompressionController(
      maxConcurrentTasks = 8,
      maxGpuSemaphoreWaiters = 2)
    val idle = AdaptiveCompressionPressure(
      writerPoolSize = 20,
      activeWriterThreads = 4,
      queuedWriterTasks = 0,
      gpuSemaphoreWaiters = 0)

    val decision = controller.observe(idle)
    assert(!decision.proposeGpu)
    assertResult(1)(decision.targetConcurrentTasks)
    assertResult("no-cpu-backlog")(decision.reason)
  }

  test("executor controller continues exploring after GPU offload drains the CPU queue") {
    val controller = new AdaptiveGpuCompressionController(
      maxConcurrentTasks = 8,
      maxGpuSemaphoreWaiters = 2)
    val healthy = AdaptiveCompressionPressure(
      writerPoolSize = 20,
      activeWriterThreads = 20,
      queuedWriterTasks = 3,
      gpuSemaphoreWaiters = 0)
    val drainedCpuQueue = healthy.copy(activeWriterThreads = 4, queuedWriterTasks = 0)

    controller.observe(healthy)
    assertResult(2)(controller.observe(healthy).targetConcurrentTasks)
    assertResult(2)(controller.observe(drainedCpuQueue).targetConcurrentTasks)
    val explored = controller.observe(drainedCpuQueue)

    assert(explored.proposeGpu)
    assertResult("learned-gpu-route")(explored.reason)
    assertResult(4)(explored.targetConcurrentTasks)
  }

  test("executor controller initializes from task settings before its first observation") {
    ExecutorAdaptiveGpuCompressionController.resetForTests()
    val healthy = AdaptiveCompressionPressure(
      writerPoolSize = 20,
      activeWriterThreads = 20,
      queuedWriterTasks = 3,
      gpuSemaphoreWaiters = 0)

    val first = ExecutorAdaptiveGpuCompressionController.observe(
      healthy,
      maxConcurrentTasks = 8,
      maxGpuSemaphoreWaiters = 2)
    val second = ExecutorAdaptiveGpuCompressionController.observe(
      healthy,
      maxConcurrentTasks = 8,
      maxGpuSemaphoreWaiters = 2)

    assert(first.proposeGpu)
    assertResult(1)(first.targetConcurrentTasks)
    assertResult(2)(second.targetConcurrentTasks)
    assertResult(2)(ExecutorGpuCompressionReservation.targetCount)
    ExecutorAdaptiveGpuCompressionController.resetForTests()
  }

  test("only one task reserves GPU compression and other tasks stay on CPU") {
    val shuffleId = 3
    AdaptiveShuffleCompressionMetrics.clearShuffle(shuffleId)
    val before = AdaptiveShuffleCompressionMetrics.executorSnapshot
    val reservation = new TestGpuReservation
    val firstTask = new TaskCompressionPlanState(reservation)
    val secondTask = new TaskCompressionPlanState(reservation)

    val firstPlan = firstTask.getOrFreeze(
      adaptiveGpuCompressionEnabled = true,
      ShuffleCompressionBackend.NvcompGpuZstd)
    val secondPlan = secondTask.getOrFreeze(
      adaptiveGpuCompressionEnabled = true,
      ShuffleCompressionBackend.NvcompGpuZstd)
    AdaptiveShuffleCompressionMetrics.record(shuffleId, firstPlan)
    AdaptiveShuffleCompressionMetrics.record(shuffleId, secondPlan)

    assertResult(ShuffleCompressionBackend.NvcompGpuZstd)(firstPlan.backend)
    assertResult(ShuffleCompressionBackend.SparkCpuZstd)(secondPlan.backend)
    assert(secondPlan.gpuReservationDenied)

    val after = AdaptiveShuffleCompressionMetrics.executorSnapshot
    assertResult(before.gpuProposedTaskAttempts + 2)(after.gpuProposedTaskAttempts)
    assertResult(before.gpuSelectedTaskAttempts + 1)(after.gpuSelectedTaskAttempts)
    assertResult(before.gpuReservationDeniedTaskAttempts + 1)(
      after.gpuReservationDeniedTaskAttempts)
    assertResult(before.cpuSelectedTaskAttempts + 1)(after.cpuSelectedTaskAttempts)

    val shuffleSnapshot = AdaptiveShuffleCompressionMetrics.takeShuffleSnapshot(shuffleId)
    assertResult(2)(shuffleSnapshot.gpuProposedTaskAttempts)
    assertResult(1)(shuffleSnapshot.gpuSelectedTaskAttempts)
    assertResult(1)(shuffleSnapshot.gpuReservationDeniedTaskAttempts)
    assertResult(1)(shuffleSnapshot.cpuSelectedTaskAttempts)
    assert(!AdaptiveShuffleCompressionMetrics.takeShuffleSnapshot(shuffleId).nonEmpty)

    firstTask.close()
    secondTask.close()
    assertResult(1)(reservation.releaseCount)
  }

  test("periodic metric drains preserve later task-attempt deltas") {
    val shuffleId = 4
    AdaptiveShuffleCompressionMetrics.clearShuffle(shuffleId)
    val reservation = new TestGpuReservation

    val firstTask = new TaskCompressionPlanState(reservation)
    val firstPlan = firstTask.getOrFreeze(
      adaptiveGpuCompressionEnabled = true,
      ShuffleCompressionBackend.SparkCpuZstd)
    AdaptiveShuffleCompressionMetrics.record(shuffleId, firstPlan)
    AdaptiveShuffleCompressionMetrics.recordWork(
      shuffleId,
      ShuffleCompressionBackend.SparkCpuZstd,
      rawBytes = 100,
      compressedBytes = 40,
      compressionTimeNs = 10,
      reservationTimeNs = 0)

    val firstDrain =
      AdaptiveShuffleCompressionMetrics.drainShuffleSnapshots.toMap.apply(shuffleId)
    assertResult(1)(firstDrain.cpuSelectedTaskAttempts)
    assertResult(100)(firstDrain.cpuRawBytes)
    assertResult(40)(firstDrain.cpuCompressedBytes)
    assertResult(10)(firstDrain.cpuCompressionTimeNs)
    assert(!AdaptiveShuffleCompressionMetrics.drainShuffleSnapshots.toMap.contains(shuffleId))

    val secondTask = new TaskCompressionPlanState(reservation)
    val secondPlan = secondTask.getOrFreeze(
      adaptiveGpuCompressionEnabled = true,
      ShuffleCompressionBackend.SparkCpuZstd)
    AdaptiveShuffleCompressionMetrics.record(shuffleId, secondPlan)
    AdaptiveShuffleCompressionMetrics.recordWork(
      shuffleId,
      ShuffleCompressionBackend.SparkCpuZstd,
      rawBytes = 200,
      compressedBytes = 70,
      compressionTimeNs = 20,
      reservationTimeNs = 0)

    val secondDrain =
      AdaptiveShuffleCompressionMetrics.drainShuffleSnapshots.toMap.apply(shuffleId)
    assertResult(1)(secondDrain.cpuSelectedTaskAttempts)
    assertResult(200)(secondDrain.cpuRawBytes)
    assertResult(70)(secondDrain.cpuCompressedBytes)
    assertResult(20)(secondDrain.cpuCompressionTimeNs)
    assert(!AdaptiveShuffleCompressionMetrics.drainShuffleSnapshots.toMap.contains(shuffleId))

    firstTask.close()
    secondTask.close()
    AdaptiveShuffleCompressionMetrics.clearShuffle(shuffleId)
  }

  test("executor GPU compression reservation enforces its configured limit") {
    ExecutorGpuCompressionReservation.configure(2)
    try {
      ExecutorGpuCompressionReservation.updateTarget(2)
      assert(ExecutorGpuCompressionReservation.tryAcquire())
      assert(ExecutorGpuCompressionReservation.tryAcquire())
      assert(!ExecutorGpuCompressionReservation.tryAcquire())
      assertResult(2)(ExecutorGpuCompressionReservation.activeCount)
      ExecutorGpuCompressionReservation.release()
      assert(ExecutorGpuCompressionReservation.tryAcquire())
      ExecutorGpuCompressionReservation.release()
      ExecutorGpuCompressionReservation.release()
      assertResult(0)(ExecutorGpuCompressionReservation.activeCount)
    } finally {
      ExecutorGpuCompressionReservation.configure(1)
      ExecutorGpuCompressionReservation.updateTarget(1)
    }
  }

  test("executor GPU compression reservation follows a dynamic target below its ceiling") {
    ExecutorGpuCompressionReservation.configure(4)
    try {
      ExecutorGpuCompressionReservation.updateTarget(2)
      assert(ExecutorGpuCompressionReservation.tryAcquire())
      assert(ExecutorGpuCompressionReservation.tryAcquire())
      assert(!ExecutorGpuCompressionReservation.tryAcquire())
      assertResult(2)(ExecutorGpuCompressionReservation.activeCount)
      assertResult(2)(ExecutorGpuCompressionReservation.targetCount)

      ExecutorGpuCompressionReservation.updateTarget(1)
      assert(!ExecutorGpuCompressionReservation.tryAcquire())
      ExecutorGpuCompressionReservation.release()
      assert(!ExecutorGpuCompressionReservation.tryAcquire())
      ExecutorGpuCompressionReservation.release()
      assert(ExecutorGpuCompressionReservation.tryAcquire())
      ExecutorGpuCompressionReservation.release()
    } finally {
      ExecutorGpuCompressionReservation.configure(1)
      ExecutorGpuCompressionReservation.updateTarget(1)
    }
  }
}
