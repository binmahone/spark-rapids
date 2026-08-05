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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

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

  /** Returns the point-in-time pressure gate used to bootstrap the executor policy. */
  def proposesGpu(maxGpuSemaphoreWaiters: Int): Boolean =
    cpuBacklogged && gpuWithinBound(maxGpuSemaphoreWaiters)
}

case class AdaptiveGpuCompressionDecision(
    proposeGpu: Boolean,
    reason: String)

/**
 * Learns whether GPU compression is useful from observed CPU and GPU pressure.
 *
 * CPU backlog provides the initial evidence that GPU compression is useful. Once learned, the
 * policy keeps proposing GPU work while semaphore pressure remains acceptable, avoiding
 * oscillation when successful offload temporarily drains the CPU queue. A single overloaded
 * observation preserves the learned route; two consecutive overloaded observations back off.
 * The shared GPU semaphore, not this policy, is the memory-admission authority.
 */
class AdaptiveGpuCompressionPolicy(
    val maxGpuSemaphoreWaiters: Int) {
  require(maxGpuSemaphoreWaiters >= 0, "Maximum GPU semaphore waiters must be non-negative")

  private var consecutiveOverloadedObservations = 0
  private var learnedGpuRoute = false
  private var gpuPressureBackoffActive = false

  def observe(pressure: AdaptiveCompressionPressure): AdaptiveGpuCompressionDecision =
    synchronized {
      val gpuWithinBound = pressure.gpuWithinBound(maxGpuSemaphoreWaiters)
      val gpuOverloaded = !gpuWithinBound

      if (gpuOverloaded && !pressure.cpuBacklogged) {
        consecutiveOverloadedObservations += 1
        if (consecutiveOverloadedObservations >= 2) {
          gpuPressureBackoffActive = true
          learnedGpuRoute = false
          consecutiveOverloadedObservations = 0
        }
      } else if (pressure.cpuBacklogged || learnedGpuRoute) {
        gpuPressureBackoffActive = false
        consecutiveOverloadedObservations = 0
        if (pressure.cpuBacklogged) {
          learnedGpuRoute = true
        }
      } else {
        gpuPressureBackoffActive = false
        consecutiveOverloadedObservations = 0
      }

      val proposeGpu =
        pressure.writerPoolSize > 0 &&
          (pressure.cpuBacklogged || (learnedGpuRoute && !gpuPressureBackoffActive))
      val reason =
        if (pressure.cpuBacklogged) {
          "cpu-backlogged"
        } else if (gpuOverloaded && learnedGpuRoute && !gpuPressureBackoffActive) {
          "learned-gpu-route-transient-overload"
        } else if (gpuOverloaded) {
          "gpu-overloaded"
        } else if (learnedGpuRoute) {
          "learned-gpu-route"
        } else {
          "no-cpu-backlog"
        }
      AdaptiveGpuCompressionDecision(proposeGpu, reason)
    }
}

trait GpuCompressionReservation {
  def tryAcquire(taskContext: TaskContext, memoryBytes: Long): Option[GpuMemoryReservation]
}

object SharedGpuCompressionReservation extends GpuCompressionReservation {
  override def tryAcquire(
      taskContext: TaskContext,
      memoryBytes: Long): Option[GpuMemoryReservation] = {
    GpuSemaphore.tryAcquireTemporaryPeak(taskContext, memoryBytes)
  }
}

object ExecutorAdaptiveGpuCompressionPolicy {
  private var policy: AdaptiveGpuCompressionPolicy = _

  def configure(maxGpuSemaphoreWaiters: Int): Unit = synchronized {
    if (policy == null ||
        policy.maxGpuSemaphoreWaiters != maxGpuSemaphoreWaiters) {
      policy = new AdaptiveGpuCompressionPolicy(maxGpuSemaphoreWaiters)
    }
  }

  def observe(pressure: AdaptiveCompressionPressure): AdaptiveGpuCompressionDecision =
    synchronized {
      require(policy != null, "Adaptive GPU compression policy is not configured")
      policy.observe(pressure)
    }

  /**
   * Configures the executor-local policy from serialized task settings before observing
   * pressure. Driver-side singleton state is not available in executor JVMs.
   */
  def observe(
      pressure: AdaptiveCompressionPressure,
      maxGpuSemaphoreWaiters: Int): AdaptiveGpuCompressionDecision = synchronized {
    configure(maxGpuSemaphoreWaiters)
    observe(pressure)
  }

  private[rapids] def resetForTests(): Unit = synchronized {
    policy = null
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
 * Freezes the preferred compression backend on first use and keeps it for the lifetime of a task.
 *
 * Executor pressure may change while a task is running. Such changes affect later tasks, not
 * records already being produced by this task. When GPU reservations are released after each
 * compression phase, a later phase requests the current phase's estimated incremental bytes from
 * the shared GPU semaphore or falls back to CPU compression. Both backends produce the same wire
 * encoding.
 */
class TaskCompressionPlanState(
    gpuReservation: GpuCompressionReservation = SharedGpuCompressionReservation)
    extends AutoCloseable {
  private val frozenPlan = new AtomicReference[TaskCompressionPlan]()
  private val activeGpuReservation = new AtomicReference[GpuMemoryReservation]()
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
      taskContext: TaskContext,
      memoryBytes: Long,
      adaptiveGpuCompressionEnabled: Boolean,
      proposedBackend: => ShuffleCompressionBackend): TaskCompressionPlan = {
    val existing = frozenPlan.get()
    if (existing != null) {
      return resumeGpuPlanOrFallback(taskContext, memoryBytes, existing)
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
          case ShuffleCompressionBackend.NvcompGpuZstd
              if acquireGpuReservation(taskContext, memoryBytes) =>
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

  private def resumeGpuPlanOrFallback(
      taskContext: TaskContext,
      memoryBytes: Long,
      existing: TaskCompressionPlan): TaskCompressionPlan = {
    if (!existing.useGpuCompressor || !gpuPhaseCompleted.get()) {
      existing
    } else synchronized {
      if (!gpuPhaseCompleted.get()) {
        existing
      } else if (acquireGpuReservation(taskContext, memoryBytes)) {
        gpuReservationAcquiredAtNs.set(System.nanoTime())
        gpuPhaseCompleted.set(false)
        existing
      } else {
        TaskCompressionPlan(
          ShuffleCompressionBackend.SparkCpuZstd,
          ShuffleCompressionBackend.NvcompGpuZstd)
      }
    }
  }

  private def acquireGpuReservation(taskContext: TaskContext, memoryBytes: Long): Boolean = {
    require(taskContext != null, "GPU compression reservation requires a task context")
    require(memoryBytes > 0, "GPU compression reservation requires a positive byte estimate")
    gpuReservation.tryAcquire(taskContext, memoryBytes).exists { reservation =>
      if (activeGpuReservation.compareAndSet(null, reservation)) {
        true
      } else {
        reservation.close()
        throw new IllegalStateException("GPU compression reservation is already active")
      }
    }
  }

  def get: Option[TaskCompressionPlan] = Option(frozenPlan.get())

  def activeReservation: Option[GpuMemoryReservation] =
    Option(activeGpuReservation.get())

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
      "a GPU compression phase completed without an active phase")
    val reservation = activeGpuReservation.getAndSet(null)
    if (reservation != null) {
      reservation.close()
    } else {
      throw new IllegalStateException(
        "GPU compression completed without an active compression reservation")
    }
  }

  override def close(): Unit = {
    val reservation = activeGpuReservation.getAndSet(null)
    if (reservation != null) {
      reservation.close()
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
