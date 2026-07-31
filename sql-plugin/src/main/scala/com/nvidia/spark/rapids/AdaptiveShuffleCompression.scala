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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import org.apache.spark.TaskContext

sealed trait ShuffleCompressionBackend

object ShuffleCompressionBackend {
  case object SparkCpuZstd extends ShuffleCompressionBackend
  case object NvcompGpuZstd extends ShuffleCompressionBackend
}

sealed trait ShuffleCompressionEncoding

object ShuffleCompressionEncoding {
  case object StandardZstdFrames extends ShuffleCompressionEncoding
}

case class AdaptiveCompressionPressure(
    writerPoolSize: Int,
    activeWriterThreads: Int,
    queuedWriterTasks: Int,
    gpuSemaphoreWaiters: Int) {

  def cpuBacklogged: Boolean =
    writerPoolSize > 0 &&
      activeWriterThreads >= writerPoolSize &&
      queuedWriterTasks > 0

  def gpuWithinBound(maxGpuSemaphoreWaiters: Int): Boolean =
    gpuSemaphoreWaiters <= maxGpuSemaphoreWaiters

  /** Returns the point-in-time pressure gate used to bootstrap the executor controller. */
  def proposesGpu(maxGpuSemaphoreWaiters: Int): Boolean =
    cpuBacklogged && gpuWithinBound(maxGpuSemaphoreWaiters)
}

case class AdaptiveGpuCompressionDecision(
    proposeGpu: Boolean,
    targetConcurrentTasks: Int,
    reason: String)

/**
 * Learns an executor-local GPU compression concurrency target from observed CPU and GPU pressure.
 *
 * CPU backlog provides the initial evidence that GPU compression is useful. Two consecutive
 * healthy observations double the target until the configured ceiling is reached. Once learned,
 * the controller keeps routing work to the GPU while semaphore pressure remains acceptable,
 * avoiding oscillation when successful offload temporarily drains the CPU queue. Two consecutive
 * overloaded observations halve the target.
 */
class AdaptiveGpuCompressionController(
    val maxConcurrentTasks: Int,
    val maxGpuSemaphoreWaiters: Int) {
  require(maxConcurrentTasks > 0, "Maximum concurrent GPU compression tasks must be positive")
  require(maxGpuSemaphoreWaiters >= 0, "Maximum GPU semaphore waiters must be non-negative")

  private var targetConcurrentTasks = 1
  private var consecutiveHealthyObservations = 0
  private var consecutiveOverloadedObservations = 0
  private var learnedGpuRoute = false

  def observe(pressure: AdaptiveCompressionPressure): AdaptiveGpuCompressionDecision =
    synchronized {
      val gpuWithinBound = pressure.gpuWithinBound(maxGpuSemaphoreWaiters)
      val gpuOverloaded = !gpuWithinBound

      if (gpuOverloaded) {
        consecutiveHealthyObservations = 0
        consecutiveOverloadedObservations += 1
        if (consecutiveOverloadedObservations >= 2 && targetConcurrentTasks > 1) {
          targetConcurrentTasks = math.max(1, (targetConcurrentTasks + 1) / 2)
          if (targetConcurrentTasks == 1) {
            learnedGpuRoute = false
          }
          consecutiveOverloadedObservations = 0
        }
      } else if (pressure.cpuBacklogged || learnedGpuRoute) {
        consecutiveOverloadedObservations = 0
        consecutiveHealthyObservations += 1
        if (pressure.cpuBacklogged) {
          learnedGpuRoute = true
        }
        if (consecutiveHealthyObservations >= 2 &&
            targetConcurrentTasks < maxConcurrentTasks) {
          targetConcurrentTasks = math.min(maxConcurrentTasks, targetConcurrentTasks * 2)
          consecutiveHealthyObservations = 0
        }
      } else {
        consecutiveHealthyObservations = 0
        consecutiveOverloadedObservations = 0
      }

      val proposeGpu =
        pressure.writerPoolSize > 0 &&
          gpuWithinBound &&
          (pressure.cpuBacklogged || learnedGpuRoute)
      val reason =
        if (gpuOverloaded) {
          "gpu-overloaded"
        } else if (pressure.cpuBacklogged) {
          "cpu-backlogged"
        } else if (learnedGpuRoute) {
          "learned-gpu-route"
        } else {
          "no-cpu-backlog"
        }
      AdaptiveGpuCompressionDecision(proposeGpu, targetConcurrentTasks, reason)
    }

  private[rapids] def target: Int = synchronized {
    targetConcurrentTasks
  }
}

trait GpuCompressionReservation {
  def tryAcquire(): Boolean
  def release(): Unit
}

object ExecutorGpuCompressionReservation extends GpuCompressionReservation {
  private val activeReservations = new AtomicInteger(0)
  @volatile private var maxConcurrentTasks = 1
  @volatile private var targetConcurrentTasks = 1

  def configure(maxTasks: Int): Unit = synchronized {
    require(maxTasks > 0, "GPU compression reservation limit must be positive")
    require(activeReservations.get() == 0 || maxConcurrentTasks == maxTasks,
      "GPU compression reservation limit cannot change while reservations are active")
    maxConcurrentTasks = maxTasks
    targetConcurrentTasks = math.min(targetConcurrentTasks, maxConcurrentTasks)
  }

  def updateTarget(targetTasks: Int): Unit = synchronized {
    require(targetTasks > 0 && targetTasks <= maxConcurrentTasks,
      s"GPU compression target $targetTasks must be between 1 and $maxConcurrentTasks")
    targetConcurrentTasks = targetTasks
  }

  override def tryAcquire(): Boolean = synchronized {
    if (activeReservations.get() < targetConcurrentTasks) {
      activeReservations.incrementAndGet()
      true
    } else {
      false
    }
  }

  override def release(): Unit = synchronized {
    require(activeReservations.get() > 0,
      "GPU compression reservation was released without being held")
    activeReservations.decrementAndGet()
  }

  private[rapids] def activeCount: Int = activeReservations.get()
  private[rapids] def targetCount: Int = targetConcurrentTasks
}

object ExecutorAdaptiveGpuCompressionController {
  private var controller: AdaptiveGpuCompressionController = _

  def configure(maxConcurrentTasks: Int, maxGpuSemaphoreWaiters: Int): Unit = synchronized {
    if (controller == null ||
        controller.maxConcurrentTasks != maxConcurrentTasks ||
        controller.maxGpuSemaphoreWaiters != maxGpuSemaphoreWaiters) {
      require(ExecutorGpuCompressionReservation.activeCount == 0,
        "Adaptive GPU compression controller cannot be reconfigured while reservations are active")
      controller =
        new AdaptiveGpuCompressionController(maxConcurrentTasks, maxGpuSemaphoreWaiters)
      ExecutorGpuCompressionReservation.configure(maxConcurrentTasks)
      ExecutorGpuCompressionReservation.updateTarget(1)
    }
  }

  def observe(pressure: AdaptiveCompressionPressure): AdaptiveGpuCompressionDecision =
    synchronized {
      require(controller != null, "Adaptive GPU compression controller is not configured")
      val decision = controller.observe(pressure)
      ExecutorGpuCompressionReservation.updateTarget(decision.targetConcurrentTasks)
      decision
    }

  /**
   * Configures the executor-local controller from serialized task settings before observing
   * pressure. Driver-side singleton state is not available in executor JVMs.
   */
  def observe(
      pressure: AdaptiveCompressionPressure,
      maxConcurrentTasks: Int,
      maxGpuSemaphoreWaiters: Int): AdaptiveGpuCompressionDecision = synchronized {
    configure(maxConcurrentTasks, maxGpuSemaphoreWaiters)
    observe(pressure)
  }

  private[rapids] def resetForTests(): Unit = synchronized {
    require(ExecutorGpuCompressionReservation.activeCount == 0,
      "Adaptive GPU compression controller cannot be reset while reservations are active")
    controller = null
  }
}

case class AdaptiveShuffleCompressionMetricsSnapshot(
    gpuProposedTaskAttempts: Long,
    gpuSelectedTaskAttempts: Long,
    gpuReservationDeniedTaskAttempts: Long,
    cpuSelectedTaskAttempts: Long,
    gpuRawBytes: Long,
    gpuCompressedBytes: Long,
    gpuCompressionTimeNs: Long,
    gpuReservationTimeNs: Long,
    cpuRawBytes: Long,
    cpuCompressedBytes: Long,
    cpuCompressionTimeNs: Long) {
  def nonEmpty: Boolean =
    gpuProposedTaskAttempts != 0L ||
      gpuSelectedTaskAttempts != 0L ||
      gpuReservationDeniedTaskAttempts != 0L ||
      cpuSelectedTaskAttempts != 0L ||
      gpuRawBytes != 0L ||
      gpuCompressedBytes != 0L ||
      gpuCompressionTimeNs != 0L ||
      gpuReservationTimeNs != 0L ||
      cpuRawBytes != 0L ||
      cpuCompressedBytes != 0L ||
      cpuCompressionTimeNs != 0L
}

object AdaptiveShuffleCompressionMetrics {
  private class Counters {
    val gpuProposedTaskAttempts = new AtomicLong(0L)
    val gpuSelectedTaskAttempts = new AtomicLong(0L)
    val gpuReservationDeniedTaskAttempts = new AtomicLong(0L)
    val cpuSelectedTaskAttempts = new AtomicLong(0L)
    val gpuRawBytes = new AtomicLong(0L)
    val gpuCompressedBytes = new AtomicLong(0L)
    val gpuCompressionTimeNs = new AtomicLong(0L)
    val gpuReservationTimeNs = new AtomicLong(0L)
    val cpuRawBytes = new AtomicLong(0L)
    val cpuCompressedBytes = new AtomicLong(0L)
    val cpuCompressionTimeNs = new AtomicLong(0L)

    def record(plan: TaskCompressionPlan): Unit = {
      if (plan.proposedBackend == ShuffleCompressionBackend.NvcompGpuZstd) {
        gpuProposedTaskAttempts.incrementAndGet()
      }
      if (plan.backend == ShuffleCompressionBackend.NvcompGpuZstd) {
        gpuSelectedTaskAttempts.incrementAndGet()
      } else {
        cpuSelectedTaskAttempts.incrementAndGet()
      }
      if (plan.gpuReservationDenied) {
        gpuReservationDeniedTaskAttempts.incrementAndGet()
      }
    }

    def recordWork(
        backend: ShuffleCompressionBackend,
        rawBytes: Long,
        compressedBytes: Long,
        compressionTimeNs: Long,
        reservationTimeNs: Long): Unit = backend match {
      case ShuffleCompressionBackend.NvcompGpuZstd =>
        gpuRawBytes.addAndGet(rawBytes)
        gpuCompressedBytes.addAndGet(compressedBytes)
        gpuCompressionTimeNs.addAndGet(compressionTimeNs)
        gpuReservationTimeNs.addAndGet(reservationTimeNs)
      case ShuffleCompressionBackend.SparkCpuZstd =>
        cpuRawBytes.addAndGet(rawBytes)
        cpuCompressedBytes.addAndGet(compressedBytes)
        cpuCompressionTimeNs.addAndGet(compressionTimeNs)
    }

    def snapshot: AdaptiveShuffleCompressionMetricsSnapshot =
      AdaptiveShuffleCompressionMetricsSnapshot(
        gpuProposedTaskAttempts.get(),
        gpuSelectedTaskAttempts.get(),
        gpuReservationDeniedTaskAttempts.get(),
        cpuSelectedTaskAttempts.get(),
        gpuRawBytes.get(),
        gpuCompressedBytes.get(),
        gpuCompressionTimeNs.get(),
        gpuReservationTimeNs.get(),
        cpuRawBytes.get(),
        cpuCompressedBytes.get(),
        cpuCompressionTimeNs.get())

    def drain: AdaptiveShuffleCompressionMetricsSnapshot =
      AdaptiveShuffleCompressionMetricsSnapshot(
        gpuProposedTaskAttempts.getAndSet(0L),
        gpuSelectedTaskAttempts.getAndSet(0L),
        gpuReservationDeniedTaskAttempts.getAndSet(0L),
        cpuSelectedTaskAttempts.getAndSet(0L),
        gpuRawBytes.getAndSet(0L),
        gpuCompressedBytes.getAndSet(0L),
        gpuCompressionTimeNs.getAndSet(0L),
        gpuReservationTimeNs.getAndSet(0L),
        cpuRawBytes.getAndSet(0L),
        cpuCompressedBytes.getAndSet(0L),
        cpuCompressionTimeNs.getAndSet(0L))
  }

  private val executorCumulative = new Counters
  private val byShuffle = new ConcurrentHashMap[Int, Counters]()

  def record(shuffleId: Int, plan: TaskCompressionPlan): Unit = {
    executorCumulative.record(plan)
    byShuffle.computeIfAbsent(shuffleId, _ => new Counters).record(plan)
  }

  def recordWork(
      shuffleId: Int,
      backend: ShuffleCompressionBackend,
      rawBytes: Long,
      compressedBytes: Long,
      compressionTimeNs: Long,
      reservationTimeNs: Long): Unit = {
    executorCumulative.recordWork(
      backend, rawBytes, compressedBytes, compressionTimeNs, reservationTimeNs)
    byShuffle.computeIfAbsent(shuffleId, _ => new Counters)
      .recordWork(backend, rawBytes, compressedBytes, compressionTimeNs, reservationTimeNs)
  }

  def executorSnapshot: AdaptiveShuffleCompressionMetricsSnapshot =
    executorCumulative.snapshot

  def takeShuffleSnapshot(shuffleId: Int): AdaptiveShuffleCompressionMetricsSnapshot = {
    val counters = byShuffle.remove(shuffleId)
    if (counters == null) {
      AdaptiveShuffleCompressionMetricsSnapshot(
        0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L)
    } else {
      counters.snapshot
    }
  }

  /**
   * Drains executor-local deltas for every shuffle without waiting for shuffle cleanup.
   *
   * Shuffle cleanup can happen after the driver listener bus has stopped. Periodic draining lets
   * the executor heartbeat deliver evidence while the application is still running. Counters stay
   * registered so task attempts recorded after a drain are included in a later delta.
   */
  def drainShuffleSnapshots: Seq[(Int, AdaptiveShuffleCompressionMetricsSnapshot)] = {
    import scala.collection.JavaConverters._

    byShuffle.entrySet().asScala.flatMap { entry =>
      val snapshot = entry.getValue.drain
      if (snapshot.nonEmpty) {
        Some(entry.getKey -> snapshot)
      } else {
        None
      }
    }.toSeq
  }

  private[rapids] def clearShuffle(shuffleId: Int): Unit = {
    byShuffle.remove(shuffleId)
  }
}

/**
 * A task-scoped compression plan.
 *
 * CPU and GPU backends intentionally share the same wire encoding so reducers can use Spark's
 * existing CPU Zstd decoder. Exactly one compression owner is active for a plan.
 */
case class TaskCompressionPlan(
    backend: ShuffleCompressionBackend,
    proposedBackend: ShuffleCompressionBackend) {
  val encoding: ShuffleCompressionEncoding = ShuffleCompressionEncoding.StandardZstdFrames

  def useSparkCompressionWrapper: Boolean =
    backend == ShuffleCompressionBackend.SparkCpuZstd

  def useGpuCompressor: Boolean =
    backend == ShuffleCompressionBackend.NvcompGpuZstd

  require(useSparkCompressionWrapper ^ useGpuCompressor,
    s"exactly one compression owner is required for backend $backend")

  def gpuReservationDenied: Boolean =
    proposedBackend == ShuffleCompressionBackend.NvcompGpuZstd &&
      backend == ShuffleCompressionBackend.SparkCpuZstd
}

object TaskCompressionPlan {
  def apply(backend: ShuffleCompressionBackend): TaskCompressionPlan =
    new TaskCompressionPlan(backend, backend)
}

/**
 * Freezes the compression backend on first use and keeps it for the lifetime of a task.
 *
 * Executor pressure may change while a task is running. Such changes affect later tasks, not
 * records already being produced by this task.
 */
class TaskCompressionPlanState(
    gpuReservation: GpuCompressionReservation = ExecutorGpuCompressionReservation)
    extends AutoCloseable {
  private val frozenPlan = new AtomicReference[TaskCompressionPlan]()
  private val ownsGpuReservation = new AtomicBoolean(false)
  private val gpuPhaseCompleted = new AtomicBoolean(false)
  private val gpuReservationAcquiredAtNs = new AtomicLong(0L)
  private val decisionLogged = new AtomicBoolean(false)
  private val decisionReported = new AtomicBoolean(false)

  /**
   * Freezes the effective backend at the compression boundary.
   *
   * The caller should invoke this only after serializing the first record, immediately before
   * compression. Disabling the feature always preserves the existing Spark CPU path.
   */
  def getOrFreeze(
      adaptiveGpuCompressionEnabled: Boolean,
      proposedBackend: => ShuffleCompressionBackend): TaskCompressionPlan = {
    val existing = frozenPlan.get()
    if (existing != null) {
      require(!existing.useGpuCompressor || !gpuPhaseCompleted.get(),
        "experimental GPU-phase reservation mode does not support multiple GPU compression " +
          "phases in one task")
      return existing
    }

    synchronized {
      val frozenInsideLock = frozenPlan.get()
      if (frozenInsideLock != null) {
        frozenInsideLock
      } else {
        val effectiveBackend = if (adaptiveGpuCompressionEnabled) {
          proposedBackend
        } else {
          ShuffleCompressionBackend.SparkCpuZstd
        }
        val reservedBackend = effectiveBackend match {
          case ShuffleCompressionBackend.NvcompGpuZstd if gpuReservation.tryAcquire() =>
            ownsGpuReservation.set(true)
            gpuReservationAcquiredAtNs.set(System.nanoTime())
            ShuffleCompressionBackend.NvcompGpuZstd
          case ShuffleCompressionBackend.NvcompGpuZstd =>
            ShuffleCompressionBackend.SparkCpuZstd
          case cpu =>
            cpu
        }
        val plan = TaskCompressionPlan(reservedBackend, effectiveBackend)
        frozenPlan.set(plan)
        plan
      }
    }
  }

  def get: Option[TaskCompressionPlan] = Option(frozenPlan.get())

  def markDecisionForLogging(): Boolean =
    decisionLogged.compareAndSet(false, true)

  def markDecisionForReporting(): Boolean =
    decisionReported.compareAndSet(false, true)

  def gpuReservationHeldTimeNs: Long = {
    val acquiredAtNs = gpuReservationAcquiredAtNs.get()
    if (acquiredAtNs == 0L) 0L else System.nanoTime() - acquiredAtNs
  }

  def releaseGpuReservationAfterCompression(): Unit = {
    require(frozenPlan.get() != null && frozenPlan.get().useGpuCompressor,
      "only a GPU compression plan may release its reservation after compression")
    require(gpuPhaseCompleted.compareAndSet(false, true),
      "a task reached the GPU compression phase more than once")
    if (ownsGpuReservation.compareAndSet(true, false)) {
      gpuReservation.release()
    } else {
      throw new IllegalStateException(
        "GPU compression completed without an active compression reservation")
    }
  }

  override def close(): Unit = {
    if (ownsGpuReservation.compareAndSet(true, false)) {
      gpuReservation.release()
    }
  }
}

object AdaptiveTaskCompressionPlans {
  private val plans = new ConcurrentHashMap[Long, TaskCompressionPlanState]()

  def getOrCreate(taskContext: TaskContext): TaskCompressionPlanState = {
    require(taskContext != null, "adaptive GPU compression requires a task context")
    val taskAttemptId = taskContext.taskAttemptId()
    plans.computeIfAbsent(taskAttemptId, _ => {
      val state = new TaskCompressionPlanState()
      taskContext.addTaskCompletionListener[Unit] { _ =>
        val removed = plans.remove(taskAttemptId)
        if (removed != null) {
          removed.close()
        }
      }
      state
    })
  }

  private[rapids] def clear(): Unit = {
    import scala.collection.JavaConverters._

    plans.values().asScala.foreach(_.close())
    plans.clear()
  }
}
