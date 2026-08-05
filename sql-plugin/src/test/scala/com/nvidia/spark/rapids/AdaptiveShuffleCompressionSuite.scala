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
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.TaskContext

class AdaptiveShuffleCompressionSuite extends AnyFunSuite with MockitoSugar {

  private def taskContext: TaskContext = mock[TaskContext]

  private class TestGpuReservation extends GpuCompressionReservation {
    private var held = false
    var acquireCount = 0
    var releaseCount = 0

    override def tryAcquire(
        taskContext: TaskContext,
        memoryBytes: Long): Option[GpuMemoryReservation] = synchronized {
      acquireCount += 1
      if (held) {
        None
      } else {
        held = true
        Some(new GpuMemoryReservation(memoryBytes, 0L, 0L, 0L, () => {
          TestGpuReservation.this.synchronized {
            assert(held)
            held = false
            releaseCount += 1
          }
        }))
      }
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
      taskContext, 64L,
      adaptiveGpuCompressionEnabled = true, {
        proposalCount += 1
        ShuffleCompressionBackend.NvcompGpuZstd
      })
    val afterPressureChanged = state.getOrFreeze(
      taskContext, 64L,
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
      taskContext, 64L,
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
    val maxWaitersKey =
      RapidsConf.MULTITHREADED_SHUFFLE_ADAPTIVE_GPU_COMPRESSION_MAX_GPU_SEMAPHORE_WAITERS.key

    assert(!new RapidsConf(Map.empty[String, String])
      .isMultithreadedShuffleAdaptiveGpuCompressionEnabled)
    assert(new RapidsConf(Map(key -> "true"))
      .isMultithreadedShuffleAdaptiveGpuCompressionEnabled)
    assertResult(0)(new RapidsConf(Map.empty[String, String])
      .multithreadedShuffleAdaptiveGpuCompressionMaxGpuSemaphoreWaiters)
    assertResult(16)(new RapidsConf(Map(maxWaitersKey -> "16"))
      .multithreadedShuffleAdaptiveGpuCompressionMaxGpuSemaphoreWaiters)
  }

  test("GPU-phase release reacquires a reservation for a second phase in one task") {
    val reservation = new TestGpuReservation
    val state = new TaskCompressionPlanState(reservation)

    val plan = state.getOrFreeze(
      taskContext, 64L,
      adaptiveGpuCompressionEnabled = true,
      ShuffleCompressionBackend.NvcompGpuZstd)
    assert(plan.useGpuCompressor)

    state.releaseGpuReservationAfterCompression()
    assertResult(1)(reservation.releaseCount)
    val secondPlan = state.getOrFreeze(
      taskContext, 96L,
      adaptiveGpuCompressionEnabled = true,
      ShuffleCompressionBackend.NvcompGpuZstd)
    assert(secondPlan.useGpuCompressor)
    assertResult(2)(reservation.acquireCount)

    state.releaseGpuReservationAfterCompression()
    assertResult(2)(reservation.releaseCount)

    state.close()
    assertResult(2)(reservation.releaseCount)
  }

  test("a later GPU phase falls back to CPU when reacquisition is denied") {
    val reservation = new TestGpuReservation
    val firstTask = new TaskCompressionPlanState(reservation)
    val blockingTask = new TaskCompressionPlanState(reservation)

    val firstPlan = firstTask.getOrFreeze(
      taskContext, 64L,
      adaptiveGpuCompressionEnabled = true,
      ShuffleCompressionBackend.NvcompGpuZstd)
    assert(firstPlan.useGpuCompressor)
    firstTask.releaseGpuReservationAfterCompression()

    val blockingPlan = blockingTask.getOrFreeze(
      taskContext, 64L,
      adaptiveGpuCompressionEnabled = true,
      ShuffleCompressionBackend.NvcompGpuZstd)
    assert(blockingPlan.useGpuCompressor)

    val fallbackPlan = firstTask.getOrFreeze(
      taskContext, 96L,
      adaptiveGpuCompressionEnabled = true,
      ShuffleCompressionBackend.NvcompGpuZstd)
    assert(fallbackPlan.useSparkCompressionWrapper)
    assert(fallbackPlan.gpuReservationDenied)

    blockingTask.close()
    val resumedPlan = firstTask.getOrFreeze(
      taskContext, 128L,
      adaptiveGpuCompressionEnabled = true,
      ShuffleCompressionBackend.NvcompGpuZstd)
    assert(resumedPlan.useGpuCompressor)
    firstTask.releaseGpuReservationAfterCompression()

    firstTask.close()
    assertResult(4)(reservation.acquireCount)
    assertResult(3)(reservation.releaseCount)
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

  test("executor policy holds its learned route and backs off") {
    val policy = new AdaptiveGpuCompressionPolicy(maxGpuSemaphoreWaiters = 2)
    val healthy = AdaptiveCompressionPressure(
      writerPoolSize = 20,
      activeWriterThreads = 20,
      queuedWriterTasks = 3,
      gpuSemaphoreWaiters = 0)

    assert(policy.observe(healthy).proposeGpu)

    val drainedCpuQueue = healthy.copy(activeWriterThreads = 4, queuedWriterTasks = 0)
    val learned = policy.observe(drainedCpuQueue)
    assert(learned.proposeGpu)
    assertResult("learned-gpu-route")(learned.reason)

    val overloaded = drainedCpuQueue.copy(gpuSemaphoreWaiters = 3)
    val transientOverload = policy.observe(overloaded)
    assert(transientOverload.proposeGpu)
    assertResult("learned-gpu-route-transient-overload")(transientOverload.reason)
    val firstBackoff = policy.observe(overloaded)
    assert(!firstBackoff.proposeGpu)
  }

  test("executor policy does not learn a GPU route without CPU backlog") {
    val policy = new AdaptiveGpuCompressionPolicy(maxGpuSemaphoreWaiters = 2)
    val idle = AdaptiveCompressionPressure(
      writerPoolSize = 20,
      activeWriterThreads = 4,
      queuedWriterTasks = 0,
      gpuSemaphoreWaiters = 0)

    val decision = policy.observe(idle)
    assert(!decision.proposeGpu)
    assertResult("no-cpu-backlog")(decision.reason)
  }

  test("executor policy continues exploring after GPU offload drains the CPU queue") {
    val policy = new AdaptiveGpuCompressionPolicy(maxGpuSemaphoreWaiters = 2)
    val healthy = AdaptiveCompressionPressure(
      writerPoolSize = 20,
      activeWriterThreads = 20,
      queuedWriterTasks = 3,
      gpuSemaphoreWaiters = 0)
    val drainedCpuQueue = healthy.copy(activeWriterThreads = 4, queuedWriterTasks = 0)

    policy.observe(healthy)
    policy.observe(drainedCpuQueue)
    val explored = policy.observe(drainedCpuQueue)

    assert(explored.proposeGpu)
    assertResult("learned-gpu-route")(explored.reason)
  }

  test("CPU backlog takes precedence over GPU waiter pressure") {
    val policy = new AdaptiveGpuCompressionPolicy(maxGpuSemaphoreWaiters = 2)
    val cpuAndGpuBacklogged = AdaptiveCompressionPressure(
      writerPoolSize = 20,
      activeWriterThreads = 20,
      queuedWriterTasks = 3,
      gpuSemaphoreWaiters = 3)

    val first = policy.observe(cpuAndGpuBacklogged)
    val second = policy.observe(cpuAndGpuBacklogged)

    assert(first.proposeGpu)
    assert(second.proposeGpu)
    assertResult("cpu-backlogged")(first.reason)
    assertResult("cpu-backlogged")(second.reason)
  }

  test("GPU waiter pressure backs off a learned route after CPU backlog drains") {
    val policy = new AdaptiveGpuCompressionPolicy(maxGpuSemaphoreWaiters = 2)
    val cpuBacklogged = AdaptiveCompressionPressure(
      writerPoolSize = 20,
      activeWriterThreads = 20,
      queuedWriterTasks = 3,
      gpuSemaphoreWaiters = 0)
    val gpuBacklogged = cpuBacklogged.copy(
      activeWriterThreads = 4,
      queuedWriterTasks = 0,
      gpuSemaphoreWaiters = 3)

    policy.observe(cpuBacklogged)
    val firstBackoff = policy.observe(gpuBacklogged)
    val secondBackoff = policy.observe(gpuBacklogged)

    assert(firstBackoff.proposeGpu)
    assert(!secondBackoff.proposeGpu)
    assertResult("learned-gpu-route-transient-overload")(firstBackoff.reason)
    assertResult("gpu-overloaded")(secondBackoff.reason)
  }

  test("executor policy initializes from task settings before its first observation") {
    ExecutorAdaptiveGpuCompressionPolicy.resetForTests()
    val healthy = AdaptiveCompressionPressure(
      writerPoolSize = 20,
      activeWriterThreads = 20,
      queuedWriterTasks = 3,
      gpuSemaphoreWaiters = 0)

    val first = ExecutorAdaptiveGpuCompressionPolicy.observe(
      healthy,
      maxGpuSemaphoreWaiters = 2)
    val second = ExecutorAdaptiveGpuCompressionPolicy.observe(
      healthy,
      maxGpuSemaphoreWaiters = 2)

    assert(first.proposeGpu)
    assert(second.proposeGpu)
    ExecutorAdaptiveGpuCompressionPolicy.resetForTests()
  }

  test("only one task reserves GPU compression and other tasks stay on CPU") {
    val shuffleId = 3
    AdaptiveShuffleCompressionMetrics.clearShuffle(shuffleId)
    val before = AdaptiveShuffleCompressionMetrics.executorSnapshot
    val reservation = new TestGpuReservation
    val firstTask = new TaskCompressionPlanState(reservation)
    val secondTask = new TaskCompressionPlanState(reservation)

    val firstPlan = firstTask.getOrFreeze(
      taskContext, 64L,
      adaptiveGpuCompressionEnabled = true,
      ShuffleCompressionBackend.NvcompGpuZstd)
    val secondPlan = secondTask.getOrFreeze(
      taskContext, 64L,
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
      taskContext, 64L,
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
      taskContext, 64L,
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

}
