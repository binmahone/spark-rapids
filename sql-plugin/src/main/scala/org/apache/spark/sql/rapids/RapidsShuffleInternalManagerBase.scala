/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

import java.io.{IOException, OutputStream}
import java.lang.management.ManagementFactory
import java.util.concurrent.{Callable, CancellationException, CompletableFuture, ConcurrentHashMap,
  ConcurrentLinkedQueue, ExecutionException, Executors, ExecutorService, Future, FutureTask,
  LinkedBlockingQueue, ScheduledFuture, ThreadFactory, ThreadPoolExecutor, TimeoutException,
  TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}
import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.NvtxRegistry
import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.format.TableMeta
import com.nvidia.spark.rapids.jni.kudo.OpenByteArrayOutputStream
import com.nvidia.spark.rapids.metrics.GpuBubbleTimerManager
import com.nvidia.spark.rapids.shuffle.{RapidsShuffleRequestHandler, RapidsShuffleServer, RapidsShuffleTransport}
import com.nvidia.spark.rapids.spill.SpillablePartialFileHandle

import org.apache.spark.{InterruptibleIterator, MapOutputTracker, ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{ShuffleWriter, _}
import org.apache.spark.shuffle.api._
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.sort.io.{RapidsLocalDiskShuffleDataIO, RapidsLocalDiskShuffleMapOutputWriter}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase.{METRIC_DATA_READ_SIZE, METRIC_DATA_SIZE, METRIC_SHUFFLE_DESERIALIZATION_TIME, METRIC_SHUFFLE_READ_TIME, METRIC_THREADED_READER_ADMISSION_ACQUIRE_COUNT, METRIC_THREADED_READER_ADMISSION_WAIT_TIME, METRIC_THREADED_READER_DESER_WAIT_TIME, METRIC_THREADED_READER_FUTURE_WAIT_TIME, METRIC_THREADED_READER_IO_WAIT_TIME, METRIC_THREADED_READER_LIMITER_ACQUIRE_COUNT, METRIC_THREADED_READER_LIMITER_ACQUIRE_FAIL_COUNT, METRIC_THREADED_READER_LIMITER_PENDING_BLOCK_COUNT, METRIC_THREADED_READER_RESULT_QUEUE_WAIT_TIME, METRIC_THREADED_READER_WORKER_ACTIVE_TIME, METRIC_THREADED_READER_WORKER_CPU_TIME, METRIC_THREADED_READER_WORKER_QUEUE_DELAY, METRIC_THREADED_READER_WORKER_TASK_COUNT, METRIC_THREADED_WRITER_COMPRESSION_QUEUE_WAIT_TIME, METRIC_THREADED_WRITER_COMPRESSION_TASK_COUNT, METRIC_THREADED_WRITER_COMPRESSION_TIME, METRIC_THREADED_WRITER_INPUT_FETCH_TIME, METRIC_THREADED_WRITER_LIMITER_WAIT_TIME, METRIC_THREADED_WRITER_MERGER_WRITE_TIME, METRIC_THREADED_WRITER_PARTIAL_FILE_MERGE_TIME, METRIC_THREADED_WRITER_SERIALIZATION_WAIT_TIME}
import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase.{METRIC_THREADED_READER_ADMISSION_DECISION_COUNT, METRIC_THREADED_READER_ADMISSION_DECREASE_COUNT, METRIC_THREADED_READER_ADMISSION_DESIRED_PERMITS_SUM, METRIC_THREADED_READER_ADMISSION_GPU_TARGET_SUM, METRIC_THREADED_READER_ADMISSION_HOLD_COUNT, METRIC_THREADED_READER_ADMISSION_INCREASE_COUNT}
import org.apache.spark.sql.rapids.shims.{GpuShuffleBlockResolver, RapidsShuffleThreadedReader, RapidsShuffleThreadedWriter}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{RapidsShuffleBlockFetcherIterator, _}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.{ExternalSorter, OpenHashSet}

class GpuShuffleHandle[K, V](
    val wrapped: ShuffleHandle,
    override val dependency: GpuShuffleDependency[K, V, V])
  extends BaseShuffleHandle(wrapped.shuffleId, dependency) {

  override def toString: String = s"GPU SHUFFLE HANDLE $shuffleId"
}

class ShuffleHandleWithMetrics[K, V, C](
    shuffleId: Int,
    val metrics: Map[String, SQLMetric],
    override val dependency: GpuShuffleDependency[K, V, C])
  extends BaseShuffleHandle(shuffleId, dependency) {
}

abstract class GpuShuffleBlockResolverBase(
    val wrapped: IndexShuffleBlockResolver,
    catalog: ShuffleBufferCatalog)
  extends ShuffleBlockResolver with Logging {
  override def getBlockData(blockId: BlockId, dirs: Option[Array[String]]): ManagedBuffer = {
    // Get MultithreadedShuffleBufferCatalog dynamically since it may not be
    // initialized when the resolver is created
    val mtCatalogOpt = GpuShuffleEnv.getMultithreadedCatalog

    blockId match {
      case sbid: ShuffleBlockId =>
        // Check MultithreadedShuffleBufferCatalog for single partition blocks
        mtCatalogOpt match {
          case Some(mtc) if mtc.hasData(sbid) =>
            return mtc.getMergedBuffer(sbid)
          case _ =>
        }

        // Check UCX/CACHE_ONLY catalog
        if (catalog != null && catalog.hasActiveShuffle(sbid.shuffleId)) {
          throw new IllegalStateException(s"The block $blockId is being managed by the catalog")
        }

        // Fall back to disk-based resolver
        wrapped.getBlockData(blockId, dirs)

      case sbbid: ShuffleBlockBatchId =>
        // ShuffleBlockBatchId contains multiple reduce partitions for batch fetch
        mtCatalogOpt match {
          case Some(mtc) if mtc.hasActiveShuffle(sbbid.shuffleId) =>
            return mtc.getMergedBatchBuffer(sbbid)
          case _ =>
        }

        // Check UCX/CACHE_ONLY catalog
        if (catalog != null && catalog.hasActiveShuffle(sbbid.shuffleId)) {
          throw new IllegalStateException(s"The block $blockId is being managed by the catalog")
        }
        wrapped.getBlockData(blockId, dirs)

      case _ =>
        throw new IllegalArgumentException(s"${blockId.getClass} $blockId "
          + "is not currently supported")
    }
  }

  override def stop(): Unit = wrapped.stop()
}

/**
 * The `ShuffleWriteMetricsReporter` is based on accumulators, which are not thread safe.
 * This class is a thin wrapper that adds synchronization, since these metrics will be written
 * by multiple threads.
 * @param wrapped
 */
class ThreadSafeShuffleWriteMetricsReporter(val wrapped: ShuffleWriteMetricsReporter)
  extends ShuffleWriteMetrics {

  def getWriteTime: Long = synchronized {
    TaskContext.get.taskMetrics().shuffleWriteMetrics.writeTime
  }

  override private[spark] def incBytesWritten(v: Long): Unit = synchronized {
    wrapped.incBytesWritten(v)
  }
  override private[spark] def incRecordsWritten(v: Long): Unit = synchronized {
    wrapped.incRecordsWritten(v)
  }
  override private[spark] def incWriteTime(v: Long): Unit = synchronized {
    wrapped.incWriteTime(v)
  }
  override private[spark] def decBytesWritten(v: Long): Unit = synchronized {
    wrapped.decBytesWritten(v)
  }
  override private[spark] def decRecordsWritten(v: Long): Unit = synchronized {
    wrapped.decRecordsWritten(v)
  }
}

private[rapids] case class ReaderTaskAdmissionResult(acquired: Boolean, waitTimeNs: Long)

private object ReaderThreadCpuTime {
  private val bean = ManagementFactory.getThreadMXBean

  def now(): Long = {
    if (bean.isCurrentThreadCpuTimeSupported) {
      math.max(0L, bean.getCurrentThreadCpuTime)
    } else {
      0L
    }
  }
}

private[rapids] case class ReaderTaskObservation(
    workerQueueDelayNs: Long,
    workerActiveNs: Long,
    limiterAcquires: Long,
    limiterFailures: Long)

private[rapids] case class ReaderTaskAdmissionConfig(
    initialConcurrentTasks: Int,
    adaptiveEnabled: Boolean,
    minConcurrentTasks: Int,
    maxConcurrentTasks: Int,
    gpuConcurrencyMultiplier: Double,
    decisionWindowTasks: Int,
    stableTargetWindows: Int,
    maxAdjustmentStep: Int,
    detailedLoggingEnabled: Boolean,
    immediateDecreaseEnabled: Boolean = false,
    stageBoundaryDecreaseEnabled: Boolean = false) {
  require(initialConcurrentTasks > 0)
  require(minConcurrentTasks > 0 && minConcurrentTasks <= initialConcurrentTasks)
  require(maxConcurrentTasks >= initialConcurrentTasks)
  require(gpuConcurrencyMultiplier > 0.0)
  require(decisionWindowTasks > 0)
  require(stableTargetWindows > 0)
  require(maxAdjustmentStep > 0)
}

private[rapids] case class ReaderTaskAdmissionDecision(
    oldPermits: Int,
    newPermits: Int,
    reason: String,
    gpuSnapshot: GpuConcurrencySnapshot,
    gpuTarget: Int,
    stableTargetWindows: Int,
    queueDelayRatio: Double,
    limiterFailureRatio: Double)

private[rapids] class ReaderTaskAdmissionGate(
    val config: ReaderTaskAdmissionConfig,
    releaseGpu: TaskContext => Unit =
      (context: TaskContext) => GpuSemaphore.releaseIfNecessary(context),
    gpuSnapshot: TaskContext => GpuConcurrencySnapshot =
      (context: TaskContext) => GpuSemaphore.concurrencySnapshot(context)) extends Logging {

  private class TaskAdmission {
    val references = new AtomicInteger(1)
    val permitReleased = new AtomicBoolean(false)
    val workerQueueDelayNs = new AtomicLong()
    val workerActiveNs = new AtomicLong()
    val limiterAcquires = new AtomicLong()
    val limiterFailures = new AtomicLong()
  }

  private val lock = new ReentrantLock(true)
  private val permitsChanged = lock.newCondition()
  private var activeTasks = 0
  private var waitingTasks = 0
  private var desiredPermits = config.initialConcurrentTasks
  private var completedInWindow = 0
  private var windowWorkerQueueDelayNs = 0L
  private var windowWorkerActiveNs = 0L
  private var windowLimiterAcquires = 0L
  private var windowLimiterFailures = 0L
  private var windowStageId = -1
  private var lastDecisionStageId = -1
  private var lastGpuTarget = -1
  private var consecutiveStableTargetWindows = 0
  private var admissionStageId = -1
  private val admittedTasks = new ConcurrentHashMap[Long, TaskAdmission]()

  def acquire(context: TaskContext): ReaderTaskAdmissionResult = {
    val taskAttemptId = context.taskAttemptId()
    val newAdmission = new TaskAdmission
    val existingAdmission = admittedTasks.putIfAbsent(taskAttemptId, newAdmission)
    if (existingAdmission != null) {
      existingAdmission.references.incrementAndGet()
      ReaderTaskAdmissionResult(acquired = false, waitTimeNs = 0L)
    } else {
      // Reader admission must never park a task while it owns GPU execution capacity.
      releaseGpu(context)
      val start = System.nanoTime()
      var lockAcquired = false
      try {
        lock.lockInterruptibly()
        lockAcquired = true
        maybeDecreaseAtStageBoundary(context)
        waitingTasks += 1
        try {
          while (activeTasks >= desiredPermits) {
            permitsChanged.await()
            maybeDecreaseAtStageBoundary(context)
          }
        } finally {
          waitingTasks -= 1
        }
        activeTasks += 1
        val waitTimeNs = System.nanoTime() - start
        onTaskCompletion(context) {
          releaseAll(taskAttemptId, newAdmission, context, adapt = false)
        }
        ReaderTaskAdmissionResult(acquired = true, waitTimeNs)
      } catch {
        case e: InterruptedException =>
          admittedTasks.remove(taskAttemptId, newAdmission)
          Thread.currentThread().interrupt()
          throw e
      } finally {
        if (lockAcquired) {
          lock.unlock()
        }
      }
    }
  }

  private def maybeDecreaseAtStageBoundary(context: TaskContext): Unit = {
    if (activeTasks == 0 && admissionStageId != context.stageId()) {
      val oldStageId = admissionStageId
      admissionStageId = context.stageId()
      resetObservationWindow()
      lastDecisionStageId = context.stageId()
      lastGpuTarget = -1
      consecutiveStableTargetWindows = 0
      if (config.adaptiveEnabled && config.stageBoundaryDecreaseEnabled) {
        val snapshot = gpuSnapshot(context)
        val gpuTarget = gpuTargetFor(snapshot)
        if (desiredPermits > gpuTarget) {
          val oldPermits = desiredPermits
          desiredPermits = gpuTarget
          if (config.detailedLoggingEnabled) {
            logWarning(s"ReaderTaskAdmissionStageBoundaryDecision " +
              s"oldStageId=$oldStageId stageId=${context.stageId()} " +
              s"oldPermits=$oldPermits newPermits=$desiredPermits " +
              s"reason=gpu-target-stage-boundary-decrease gpuTarget=$gpuTarget " +
              s"gpuEstimatedCapacity=${snapshot.estimatedCapacity} " +
              s"gpuActiveTasks=${snapshot.activeTasks} " +
              s"gpuWaitingTasks=${snapshot.waitingTasks}")
          }
          permitsChanged.signalAll()
        }
      }
    }
  }

  def releaseReference(
      context: TaskContext,
      observation: ReaderTaskObservation): Option[ReaderTaskAdmissionDecision] = {
    val taskAttemptId = context.taskAttemptId()
    val admission = admittedTasks.get(taskAttemptId)
    if (admission == null) {
      None
    } else {
      admission.workerQueueDelayNs.addAndGet(observation.workerQueueDelayNs)
      admission.workerActiveNs.addAndGet(observation.workerActiveNs)
      admission.limiterAcquires.addAndGet(observation.limiterAcquires)
      admission.limiterFailures.addAndGet(observation.limiterFailures)
      if (admission.references.decrementAndGet() == 0) {
        releaseAll(taskAttemptId, admission, context, adapt = true)
      } else {
        None
      }
    }
  }

  private def releaseAll(
      taskAttemptId: Long,
      admission: TaskAdmission,
      context: TaskContext,
      adapt: Boolean): Option[ReaderTaskAdmissionDecision] = {
    if (admission.permitReleased.compareAndSet(false, true)) {
      admittedTasks.remove(taskAttemptId, admission)
      lock.lock()
      try {
        activeTasks -= 1
        val decision = if (adapt) observeAndDecide(admission, context) else None
        permitsChanged.signalAll()
        decision
      } finally {
        lock.unlock()
      }
    } else {
      None
    }
  }

  private def observeAndDecide(
      admission: TaskAdmission,
      context: TaskContext): Option[ReaderTaskAdmissionDecision] = {
    if (!config.adaptiveEnabled) {
      return None
    }
    if (completedInWindow > 0 && windowStageId != context.stageId()) {
      resetObservationWindow()
      lastGpuTarget = -1
      consecutiveStableTargetWindows = 0
    }
    windowStageId = context.stageId()
    completedInWindow += 1
    windowWorkerQueueDelayNs += admission.workerQueueDelayNs.get()
    windowWorkerActiveNs += admission.workerActiveNs.get()
    windowLimiterAcquires += admission.limiterAcquires.get()
    windowLimiterFailures += admission.limiterFailures.get()
    if (completedInWindow < config.decisionWindowTasks) {
      return None
    }

    val snapshot = gpuSnapshot(context)
    val gpuTarget = gpuTargetFor(snapshot)
    val queueRatio = if (windowWorkerActiveNs == 0L) 0.0 else {
      windowWorkerQueueDelayNs.toDouble / windowWorkerActiveNs
    }
    val limiterRatio = if (windowLimiterAcquires == 0L) 0.0 else {
      windowLimiterFailures.toDouble / windowLimiterAcquires
    }
    if (lastDecisionStageId != context.stageId()) {
      lastDecisionStageId = context.stageId()
      lastGpuTarget = -1
      consecutiveStableTargetWindows = 0
    }
    if (gpuTarget == lastGpuTarget) {
      consecutiveStableTargetWindows += 1
    } else {
      lastGpuTarget = gpuTarget
      consecutiveStableTargetWindows = 1
    }
    val oldPermits = desiredPermits
    val (nextPermits, reason) = if (
        config.immediateDecreaseEnabled && desiredPermits > gpuTarget) {
      (gpuTarget, "gpu-target-immediate-decrease")
    } else if (
        consecutiveStableTargetWindows < config.stableTargetWindows) {
      (desiredPermits, "gpu-target-stabilizing")
    } else if (desiredPermits < gpuTarget) {
      (math.min(gpuTarget, desiredPermits + config.maxAdjustmentStep),
        "gpu-target-increase")
    } else if (desiredPermits > gpuTarget) {
      (math.max(gpuTarget, desiredPermits - config.maxAdjustmentStep),
        "gpu-target-decrease")
    } else {
      (desiredPermits, "gpu-target-hold")
    }
    desiredPermits = nextPermits
    resetObservationWindow()
    val decision = ReaderTaskAdmissionDecision(
      oldPermits, nextPermits, reason, snapshot, gpuTarget,
      consecutiveStableTargetWindows, queueRatio, limiterRatio)
    if (config.detailedLoggingEnabled) {
      logWarning(s"ReaderTaskAdmissionDecision stageId=${context.stageId()} " +
        s"oldPermits=$oldPermits newPermits=$nextPermits reason=$reason " +
        s"activeReaders=$activeTasks waitingReaders=$waitingTasks gpuTarget=$gpuTarget " +
        f"gpuConcurrencyMultiplier=${config.gpuConcurrencyMultiplier}%.3f " +
        s"gpuEstimatedCapacity=${snapshot.estimatedCapacity} " +
        s"gpuActiveTasks=${snapshot.activeTasks} gpuWaitingTasks=${snapshot.waitingTasks} " +
        f"queueDelayRatio=$queueRatio%.6f limiterFailureRatio=$limiterRatio%.6f " +
        s"maxAdjustmentStep=${config.maxAdjustmentStep} " +
        s"immediateDecreaseEnabled=${config.immediateDecreaseEnabled} " +
        s"stageBoundaryDecreaseEnabled=${config.stageBoundaryDecreaseEnabled} " +
        s"stableTargetWindows=$consecutiveStableTargetWindows/" +
        s"${config.stableTargetWindows}")
    }
    Some(decision)
  }

  private def gpuTargetFor(snapshot: GpuConcurrencySnapshot): Int = {
    math.max(config.minConcurrentTasks,
      math.min(config.maxConcurrentTasks,
        math.floor(snapshot.estimatedCapacity * config.gpuConcurrencyMultiplier).toInt))
  }

  private def resetObservationWindow(): Unit = {
    completedInWindow = 0
    windowWorkerQueueDelayNs = 0L
    windowWorkerActiveNs = 0L
    windowLimiterAcquires = 0L
    windowLimiterFailures = 0L
    windowStageId = -1
  }

  private[rapids] def availablePermits: Int = {
    lock.lock()
    try math.max(0, desiredPermits - activeTasks) finally lock.unlock()
  }

  private[rapids] def currentDesiredPermits: Int = {
    lock.lock()
    try desiredPermits finally lock.unlock()
  }

}

object RapidsShuffleInternalManagerBase extends Logging {
  private val poolUnavailable = "unavailable"

  def unwrapHandle(handle: ShuffleHandle): ShuffleHandle = handle match {
    case gh: GpuShuffleHandle[_, _] => gh.wrapped
    case other => other
  }

  // this is set by the executor on startup, when the MULTITHREADED
  // shuffle mode is utilized, as per these configs:
  //   spark.rapids.shuffle.multiThreaded.writer.threads
  //   spark.rapids.shuffle.multiThreaded.reader.threads
  private var writerPool: ThreadPoolExecutor = _
  private var readerPool: ExecutorService = _
  private var mergerPool: ExecutorService = _
  @volatile private var readerTaskAdmissionGate: ReaderTaskAdmissionGate = _

  private val hangDiagnosticScheduler = Executors.newSingleThreadScheduledExecutor(
    new ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(runnable, "rapids-shuffle-hang-diagnostic")
        thread.setDaemon(true)
        thread
      }
    })

  private var mtShuffleInitialized: Boolean = false

  def queueWriteTask[T](task: FutureTask[T]): Future[T] = {
    writerPool.execute(task)
    task
  }

  def adaptiveCompressionPressure: AdaptiveCompressionPressure = {
    val pool = writerPool
    if (pool == null) {
      AdaptiveCompressionPressure(0, 0, 0, GpuBubbleTimerManager.getInstance.waiterCount)
    } else {
      AdaptiveCompressionPressure(
        pool.getCorePoolSize,
        pool.getActiveCount,
        pool.getQueue.size(),
        GpuBubbleTimerManager.getInstance.waiterCount)
    }
  }

  /** Send a deserialization task to the shared reader pool. */
  def queueReadTask[T](task: Callable[T]): Future[T] = {
    readerPool.submit(task)
  }

  def acquireReaderTaskAdmission(
      context: TaskContext,
      config: Option[ReaderTaskAdmissionConfig]): ReaderTaskAdmissionResult = {
    config match {
      case None =>
        ReaderTaskAdmissionResult(acquired = false, waitTimeNs = 0L)
      case Some(admissionConfig) =>
        val gate = readerTaskAdmissionGate match {
          case existing if existing != null => existing
          case _ => synchronized {
            if (readerTaskAdmissionGate == null) {
              readerTaskAdmissionGate = new ReaderTaskAdmissionGate(admissionConfig)
              logInfo(s"Configured threaded shuffle reader task admission: $admissionConfig")
            }
            readerTaskAdmissionGate
          }
        }
        require(gate.config == admissionConfig,
          s"Threaded shuffle reader task admission was configured with ${gate.config} " +
            s"and cannot be changed to $admissionConfig")
        gate.acquire(context)
    }
  }

  def releaseReaderTaskAdmission(
      context: TaskContext,
      config: Option[ReaderTaskAdmissionConfig],
      observation: ReaderTaskObservation): Option[ReaderTaskAdmissionDecision] = {
    if (config.nonEmpty) {
      val gate = readerTaskAdmissionGate
      if (gate != null) {
        return gate.releaseReference(context, observation)
      }
    }
    None
  }

  def executeMergerTask(task: Runnable): Unit = mergerPool.execute(task)

  def scheduleHangDiagnostic(task: Runnable): ScheduledFuture[_] = {
    hangDiagnosticScheduler.scheduleAtFixedRate(task, 30L, 30L, TimeUnit.SECONDS)
  }

  private def poolDiagnosticSnapshot(pool: ExecutorService): String = pool match {
    case threadPool: ThreadPoolExecutor =>
      s"size=${threadPool.getPoolSize}," +
        s"active=${threadPool.getActiveCount}," +
        s"queued=${threadPool.getQueue.size()}," +
        s"completed=${threadPool.getCompletedTaskCount}"
    case null => poolUnavailable
    case other => s"type=${other.getClass.getSimpleName}"
  }

  private[rapids] def threadPoolDiagnosticSnapshot: String = {
    s"writer={${poolDiagnosticSnapshot(writerPool)}}," +
      s"merger={${poolDiagnosticSnapshot(mergerPool)}}," +
      s"reader={${poolDiagnosticSnapshot(readerPool)}}"
  }

  private def shutdownNow(pool: ExecutorService): Unit = {
    pool.shutdownNow().asScala.foreach {
      case future: Future[_] => future.cancel(false)
      case _ =>
    }
  }

  def startThreadPoolIfNeeded(
      numWriterThreads: Int,
      numReaderThreads: Int): Unit = synchronized {
    if (!mtShuffleInitialized) {
      mtShuffleInitialized = true
      if (numWriterThreads > 0) {
        writerPool = Executors.newFixedThreadPool(numWriterThreads, new ThreadFactoryBuilder()
          .setNameFormat("rapids-shuffle-writer-%d")
          .setDaemon(true)
          .build()).asInstanceOf[ThreadPoolExecutor]
        mergerPool = Executors.newFixedThreadPool(numWriterThreads, new ThreadFactoryBuilder()
          .setNameFormat("rapids-shuffle-merger-%d")
          .setDaemon(true)
          .build())
      }
      if (numReaderThreads > 0) {
        readerPool = Executors.newFixedThreadPool(numReaderThreads, new ThreadFactoryBuilder()
          .setNameFormat("rapids-shuffle-reader-%d")
          .setDaemon(true)
          .build())
      }
    }
  }

  def stopThreadPool(): Unit = synchronized {
    mtShuffleInitialized = false
    if (writerPool != null) {
      shutdownNow(writerPool)
      writerPool = null
    }

    if (readerPool != null) {
      shutdownNow(readerPool)
      readerPool = null
    }

    if (mergerPool != null) {
      shutdownNow(mergerPool)
      mergerPool = null
    }
    readerTaskAdmissionGate = null
  }
}

trait RapidsShuffleWriterShimHelper {
  def setChecksumIfNeeded(writer: DiskBlockObjectWriter, partition: Int): Unit = {
    // noop until Spark 3.2.0+
  }

  // Partition lengths, used for MapStatus, but also exposed in Spark 3.2.0+
  private var myPartitionLengths: Array[Long] = null

  // This is a Spark 3.2.0+ function, adding a default here for testing purposes
  def getPartitionLengths(): Array[Long] = myPartitionLengths

  def commitAllPartitions(writer: ShuffleMapOutputWriter, emptyChecksums: Boolean): Array[Long] = {
    myPartitionLengths = doCommitAllPartitions(writer, emptyChecksums)
    myPartitionLengths
  }

  def doCommitAllPartitions(writer: ShuffleMapOutputWriter, emptyChecksums: Boolean): Array[Long]
}

abstract class RapidsShuffleThreadedWriterBase[K, V](
    blockManager: BlockManager,
    handle: ShuffleHandleWithMetrics[K, V, V],
    mapId: Long,
    sparkConf: SparkConf,
    writeMetrics: ShuffleWriteMetricsReporter,
    maxBytesInFlight: Long,
    shuffleExecutorComponents: ShuffleExecutorComponents,
    numWriterThreads: Int)
  extends RapidsShuffleWriter[K, V]
    with RapidsShuffleWriterShimHelper {
  private val dep: ShuffleDependency[K, V, V] = handle.dependency
  private val shuffleId = dep.shuffleId
  private val partitioner = dep.partitioner
  private val numPartitions = partitioner.numPartitions
  private val serializer = dep.serializer.newInstance()
  private val fileBufferSize = sparkConf.get(config.SHUFFLE_FILE_BUFFER_SIZE).toInt * 1024
  private val limiter = new BytesInFlightLimiter(maxBytesInFlight)
  private val limiterWaitTimeMetric =
    handle.metrics.get(METRIC_THREADED_WRITER_LIMITER_WAIT_TIME)
  private val serializationWaitTimeMetric =
    handle.metrics.get(METRIC_THREADED_WRITER_SERIALIZATION_WAIT_TIME)
  private val inputFetchTimeMetric =
    handle.metrics.get(METRIC_THREADED_WRITER_INPUT_FETCH_TIME)
  private val compressionQueueWaitTimeMetric =
    handle.metrics.get(METRIC_THREADED_WRITER_COMPRESSION_QUEUE_WAIT_TIME)
  private val compressionTimeMetric =
    handle.metrics.get(METRIC_THREADED_WRITER_COMPRESSION_TIME)
  private val mergerWriteTimeMetric =
    handle.metrics.get(METRIC_THREADED_WRITER_MERGER_WRITE_TIME)
  private val partialFileMergeTimeMetric =
    handle.metrics.get(METRIC_THREADED_WRITER_PARTIAL_FILE_MERGE_TIME)
  private val compressionTaskCountMetric =
    handle.metrics.get(METRIC_THREADED_WRITER_COMPRESSION_TASK_COUNT)
  private val compressionTasksSubmitted = new AtomicLong(0L)
  private val compressionTasksStarted = new AtomicLong(0L)
  private val compressionTasksCompleted = new AtomicLong(0L)
  private val compressionTasksFailed = new AtomicLong(0L)
  private val mergerWriteTimeNs = new AtomicLong(0L)
  private val mergerWaitDiagnosticIntervalSeconds = 30L

  private var shuffleWriteRange: NvtxId = NvtxRegistry.THREADED_WRITER_WRITE.push()

  // Case class for tracking partial sorted files in multi-batch scenario
  private case class PartialFile(
      handle: SpillablePartialFileHandle,
      partitionLengths: Array[Long],
      mapOutputWriter: ShuffleMapOutputWriter)

  /**
   * Represents a single compressed record ready to be written to disk.
   * Each record has its own independent buffer, avoiding the 2GB limit issue
   * that occurs when multiple records share a single buffer.
   *
   * @param buffer The compressed data buffer (owned by this record, closed after writing)
   * @param compressedSize The actual size of compressed data in buffer
   * @param remainingQuota The quota to release after writing to disk
   */
  private case class CompressedRecord(
    buffer: OpenByteArrayOutputStream,
    compressedSize: Long,
    remainingQuota: Long)

  private def writePrecompressedFrames(
      compressedFrames: HostMemoryBuffer,
      destination: OpenByteArrayOutputStream): Unit = {
    require(compressedFrames.getLength <= Int.MaxValue,
      s"GPU-compressed shuffle record exceeds the JVM buffer limit: " +
        s"${compressedFrames.getLength}")

    if (blockManager.serializerManager.encryptionEnabled) {
      withResource(blockManager.serializerManager.wrapForEncryption(destination)) { encrypted =>
        val copyBuffer = new Array[Byte](math.min(
          fileBufferSize.toLong,
          compressedFrames.getLength).toInt)
        var offset = 0L
        while (offset < compressedFrames.getLength) {
          val copyLength = math.min(copyBuffer.length.toLong,
            compressedFrames.getLength - offset).toInt
          compressedFrames.getBytes(copyBuffer, 0, offset, copyLength)
          encrypted.write(copyBuffer, 0, copyLength)
          offset += copyLength
        }
      }
    } else {
      destination.write(compressedFrames, 0, compressedFrames.getLength.toInt)
    }
  }

  /**
   * Cooperatively writes one GPU batch without occupying a merger thread while waiting for work.
   * At most one step is scheduled for this merger. A step drains all currently ready records and
   * yields when it reaches an empty queue or an unfinished compression future.
   */
  private class BatchMerger(
      writer: ShuffleMapOutputWriter,
      partitionRecords: ConcurrentHashMap[Int,
        ConcurrentLinkedQueue[Future[CompressedRecord]]],
      maxPartitionIdQueued: AtomicInteger) {
    val completionFuture = new CompletableFuture[Void]()

    private val scheduled = new AtomicBoolean(false)
    private val stepFuture = new AtomicReference[FutureTask[Void]]()
    private var currentPartitionToWrite = 0
    private var outputStream: OutputStream = _
    private val mergerStepsStarted = new AtomicLong(0L)
    private val mergerStepsCompleted = new AtomicLong(0L)

    private sealed trait WorkState
    private case object Complete extends WorkState
    private case object NotReady extends WorkState
    private case object EmptyPartition extends WorkState
    private case class ReadyRecord(
        queue: ConcurrentLinkedQueue[Future[CompressedRecord]],
        future: Future[CompressedRecord]) extends WorkState
    private case object FinishedPartition extends WorkState

    def schedule(): Unit = {
      if (!completionFuture.isDone && scheduled.compareAndSet(false, true)) {
        val task = new FutureTask[Void](new Callable[Void] {
          override def call(): Void = {
            runStep()
            null
          }
        })
        stepFuture.set(task)
        try {
          RapidsShuffleInternalManagerBase.executeMergerTask(task)
        } catch {
          case t: Throwable =>
            stepFuture.compareAndSet(task, null)
            scheduled.set(false)
            fail(t)
        }
      }
    }

    def cancel(): Unit = {
      if (!completionFuture.isDone) {
        limiter.abort(new CancellationException("shuffle batch merger cancelled"))
      }
      completionFuture.cancel(true)
      Option(stepFuture.get()).foreach(_.cancel(true))
      synchronized {
        closeOutputStreamQuietly()
      }
    }

    private def runStep(): Unit = synchronized {
      mergerStepsStarted.incrementAndGet()
      try {
        var keepDraining = true
        while (keepDraining && !completionFuture.isDone) {
          currentWorkState match {
            case Complete =>
              completionFuture.complete(null)
              keepDraining = false
            case NotReady =>
              keepDraining = false
            case EmptyPartition =>
              // The producer has advanced beyond this partition without adding records.
              writer.getPartitionWriter(currentPartitionToWrite).openStream().close()
              currentPartitionToWrite += 1
            case ReadyRecord(recordQueue, future) =>
              if (outputStream == null) {
                outputStream = writer.getPartitionWriter(currentPartitionToWrite).openStream()
              }
              recordQueue.poll()
              writeRecord(future.get())
            case FinishedPartition =>
              closeOutputStream()
              partitionRecords.remove(currentPartitionToWrite)
              currentPartitionToWrite += 1
          }
        }

        if (currentPartitionToWrite >= numPartitions) {
          completionFuture.complete(null)
        }
      } catch {
        case ee: ExecutionException => fail(ee.getCause)
        case ie: InterruptedException =>
          Thread.currentThread().interrupt()
          fail(ie)
        case t: Throwable => fail(t)
      } finally {
        mergerStepsCompleted.incrementAndGet()
        stepFuture.set(null)
        scheduled.set(false)
        if (completionFuture.isDone) {
          closeOutputStreamQuietly()
        }

        // Recheck after clearing scheduled to avoid losing work queued during the transition.
        if (!completionFuture.isDone && hasReadyWork) {
          schedule()
        }
      }
    }

    def diagnosticSnapshot(batchId: Int): String = synchronized {
      var queueCount = 0L
      var recordCount = 0L
      var doneCount = 0L
      var cancelledCount = 0L
      partitionRecords.values().asScala.foreach { queue =>
        queueCount += 1
        queue.iterator().asScala.foreach { future =>
          recordCount += 1
          if (future.isDone) {
            doneCount += 1
          }
          if (future.isCancelled) {
            cancelledCount += 1
          }
        }
      }
      val head = Option(partitionRecords.get(currentPartitionToWrite))
        .flatMap(queue => Option(queue.peek()))
      s"batch=$batchId,currentPartition=$currentPartitionToWrite," +
        s"maxPartitionQueued=${maxPartitionIdQueued.get()}," +
        s"partitionQueues=$queueCount,queuedRecords=$recordCount," +
        s"doneRecords=$doneCount,cancelledRecords=$cancelledCount," +
        s"headPresent=${head.isDefined},headDone=${head.exists(_.isDone)}," +
        s"headCancelled=${head.exists(_.isCancelled)}," +
        s"scheduled=${scheduled.get()},stepFuturePresent=${stepFuture.get() != null}," +
        s"stepFutureDone=${Option(stepFuture.get()).exists(_.isDone)}," +
        s"completionDone=${completionFuture.isDone}," +
        s"outputStreamOpen=${outputStream != null}," +
        s"mergerStepsStarted=${mergerStepsStarted.get()}," +
        s"mergerStepsCompleted=${mergerStepsCompleted.get()}"
    }

    private def currentWorkState: WorkState = {
      if (currentPartitionToWrite >= numPartitions) {
        Complete
      } else {
        val maxQueued = maxPartitionIdQueued.get()
        if (currentPartitionToWrite > maxQueued) {
          NotReady
        } else {
          val recordQueue = partitionRecords.get(currentPartitionToWrite)
          if (recordQueue == null) {
            EmptyPartition
          } else {
            val future = recordQueue.peek()
            if (future != null && future.isDone) {
              ReadyRecord(recordQueue, future)
            } else if (future == null && currentPartitionToWrite < maxQueued) {
              FinishedPartition
            } else {
              NotReady
            }
          }
        }
      }
    }

    private def hasReadyWork: Boolean = currentWorkState != NotReady

    private def writeRecord(record: CompressedRecord): Unit = {
      val writeStartNs = System.nanoTime()
      try {
        if (record.compressedSize > 0) {
          outputStream.write(record.buffer.getBuf, 0, record.compressedSize.toInt)
        }
      } finally {
        mergerWriteTimeNs.addAndGet(System.nanoTime() - writeStartNs)
        record.buffer.close()
        limiter.release(record.remainingQuota)
      }
    }

    private def closeOutputStream(): Unit = {
      if (outputStream != null) {
        outputStream.close()
        outputStream = null
      }
    }

    private def closeOutputStreamQuietly(): Unit = {
      try {
        closeOutputStream()
      } catch {
        case _: Exception =>
      }
    }

    private def fail(t: Throwable): Unit = {
      closeOutputStreamQuietly()
      limiter.abort(t)
      completionFuture.completeExceptionally(t)
    }
  }

  /**
   * Encapsulates all state for processing one GPU batch in the multi-batch shuffle write.
   *
   * In multi-batch mode, each GPU batch gets its own BatchState with independent buffers,
   * futures, and a cooperative merger. This enables pipeline parallelism where:
   * - Main thread: processes records and queues compression tasks (non-blocking)
   * - Writer threads: execute compression tasks in parallel (each record gets its own buffer)
   * - Merger steps: write ready partitions sequentially and yield while waiting for work
   *
   * Key design: Each record uses an INDEPENDENT buffer to avoid the 2GB array limit.
   * When a partition has many records, instead of accumulating in one giant buffer,
   * each record's compressed data is in its own small buffer that gets written and
   * released immediately by a merger step.
   *
   * The merger writes partitions in order (0, 1, 2, ...) because Spark's
   * ShuffleMapOutputWriter requires sequential partition writes.
   *
   * @param batchId Unique identifier for this batch (for debugging/logging)
   * @param mapOutputWriter Shuffle output writer for this batch
   * @param partitionRecords Maps partitionId -> queue of compressed record futures.
   *                         Each future completes with an independent CompressedRecord.
   * @param maxPartitionIdQueued Highest partition ID that main thread has queued tasks for.
   *                             The merger uses this to know when a partition is complete.
   * @param merger Cooperative merger state and completion future.
   */
  private case class BatchState(
    batchId: Int,
    mapOutputWriter: ShuffleMapOutputWriter,
    partitionRecords: ConcurrentHashMap[Int,
      ConcurrentLinkedQueue[Future[CompressedRecord]]],
    maxPartitionIdQueued: AtomicInteger,
    merger: BatchMerger) {
    def mergerFuture: Future[_] = merger.completionFuture
    def scheduleMerger(): Unit = merger.schedule()
    def cancelMerger(): Unit = merger.cancel()
  }

  /**
   * Increment the reference count and get the memory size for a value.
   * This method handles ColumnarBatch values with SlicedGpuColumnVector or
   * SlicedSerializedColumnVector columns.
   *
   * @param value the value to process (typically a ColumnarBatch)
   * @return a tuple of (ColumnarBatch with incremented ref count, memory size)
   * @throws IllegalStateException if value is not a ColumnarBatch or contains
   *         unsupported column types
   */
  private def incRefCountAndGetSize(value: Any): (ColumnarBatch, Long) = {
    value match {
      case columnarBatch: ColumnarBatch =>
        if (columnarBatch.numCols() > 0) {
          columnarBatch.column(0) match {
            case _: SlicedGpuColumnVector =>
              (SlicedGpuColumnVector.incRefCount(columnarBatch),
                SlicedGpuColumnVector.getTotalHostMemoryUsed(columnarBatch))
            case _: SlicedSerializedColumnVector =>
              (SlicedSerializedColumnVector.incRefCount(columnarBatch),
                SlicedSerializedColumnVector.getTotalHostMemoryUsed(
                  columnarBatch))
            case other =>
              throw new IllegalStateException(
                s"Unexpected column type in ColumnarBatch: ${other.getClass.getName}. " +
                  "Expected SlicedGpuColumnVector or SlicedSerializedColumnVector.")
          }
        } else {
          (columnarBatch, 0L)
        }
      case other =>
        throw new IllegalStateException(
          s"Unexpected value type: ${if (other == null) "null" else other.getClass.getName}. " +
            "Expected ColumnarBatch.")
    }
  }

  /**
   * Create independent state for processing one GPU batch.
   * This allows multiple batches to be processed in pipeline without blocking.
   */
  private def createBatchState(
      batchId: Int,
      writer: ShuffleMapOutputWriter): BatchState = {

    // Each partition has a queue of compressed record futures.
    // Each record has its own independent buffer for memory isolation.
    val partitionRecords = new ConcurrentHashMap[Int,
      ConcurrentLinkedQueue[Future[CompressedRecord]]]()


    // maxPartitionIdQueued: Tracks the highest partition ID queued by main thread.
    //   - Main thread: updates via set() after adding futures
    //   - Merger step: reads via get() to check if current partition is complete
    //     (currentPartition < maxPartitionIdQueued means all data for currentPartition
    //     has been queued)
    val maxPartitionIdQueued = new AtomicInteger(-1)
    val merger = new BatchMerger(writer, partitionRecords, maxPartitionIdQueued)

    BatchState(
      batchId,
      writer,
      partitionRecords,
      maxPartitionIdQueued,
      merger)
  }

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val mapOutputWriter = shuffleExecutorComponents.createMapOutputWriter(
      shuffleId,
      mapId,
      numPartitions)
    mapOutputWriters += mapOutputWriter  // Track for cleanup

    val partLengths = if (!records.hasNext) {
      commitAllPartitions(mapOutputWriter, true)
    } else {
      writePartitionedGpuBatches(records, mapOutputWriter)
    }

    myMapStatus = Some(getMapStatus(blockManager.shuffleServerId, partLengths, mapId))

    if (shuffleWriteRange != null) {
      shuffleWriteRange.pop()
      shuffleWriteRange = null
    }
  }

  /**
   * Unified write path that handles both single batch and multi-batch tasks.
   * Uses streaming parallel processing with pipelined partition writing.
   *
   * Data flow for each record:
   * 1. ColumnarBatch (already copied to host memory, may be split from GPU batches based on
   *    spark.rapids.shuffle.partitioning.maxCpuBatchSize) -> Main thread acquires limiter quota
   * 2. Writer thread: serialize + compress -> OpenByteArrayOutputStream (JVM heap)
   * 3. Writer thread: release excess quota (recordSize - compressedSize)
   * 4. Merger step: heap buffer -> ShuffleMapOutputWriter (via SpillablePartialFileHandle)
   *    - If MEMORY_WITH_SPILL mode: data may stay in host memory until spill/commit
   *    - If FILE_ONLY mode or spilled: data goes to disk
   * 5. Merger step: release remaining quota after writing to output stream
   * 6. (Multi-batch only) Main thread: mergePartialFiles() combines all batch outputs into
   *    final shuffle file, reading from each SpillablePartialFileHandle sequentially
   *
   * Threading model (same for both scenarios):
   * - Main thread: Processes all records without blocking, queues compression tasks
   * - Merger steps: Run on a shared bounded pool, write ready partitions in order, and yield
   *   when the next compression task is incomplete
   * - Worker threads: Execute compression tasks in parallel
   *
   * Single batch: Cooperative merger steps write directly to the final output file
   *
   * Multi-batch: Detects partition ID decreasing (indicates new batch), creates
   * independent state for each batch (each with its own cooperative merger),
   * then merges all batch outputs into final file.
   */
  private def writePartitionedGpuBatches(
      records: Iterator[Product2[Any, Any]],
      mapOutputWriter: ShuffleMapOutputWriter): Array[Long] = {

    val serializerInstance = serializer
    var recordsWritten: Long = 0L

    // Track timing for metrics
    val writeStartTime = System.nanoTime()
    // Track total written size (compressed size)
    val totalCompressedSize = new AtomicLong(0L)
    val compressionQueueWaitTimeNs = new AtomicLong(0L)
    val compressionExecutionTimeNs = new AtomicLong(0L)
    var waitTimeOnLimiterNs: Long = 0L
    var inputFetchTimeNs: Long = 0L
    var partialFileMergeTimeNs: Long = 0L

    val taskContext = TaskContext.get()
    val taskThread = Thread.currentThread()
    val diagnosticPhase = new AtomicReference[String]("initializing")
    val diagnosticProgressNs = new AtomicLong(System.nanoTime())
    def markDiagnosticProgress(phase: String): Unit = {
      diagnosticPhase.set(phase)
      diagnosticProgressNs.set(System.nanoTime())
    }
    val hangDiagnostic = RapidsShuffleInternalManagerBase.scheduleHangDiagnostic(new Runnable {
      override def run(): Unit = {
        val stalledNs = System.nanoTime() - diagnosticProgressNs.get()
        if (stalledNs >= TimeUnit.SECONDS.toNanos(30L)) {
          val stack = taskThread.getStackTrace.take(16).mkString(" <- ")
          logWarning(
            s"RAPIDS_SHUFFLE_TASK_HEARTBEAT " +
              s"stage=${taskContext.stageId()},partition=${taskContext.partitionId()}," +
              s"attempt=${taskContext.attemptNumber()}," +
              s"taskAttempt=${taskContext.taskAttemptId()}," +
              s"shuffle=$shuffleId,map=$mapId,phase=${diagnosticPhase.get()}," +
              s"stalledMs=${TimeUnit.NANOSECONDS.toMillis(stalledNs)}," +
              s"taskThreadState=${taskThread.getState},bytesInFlight=${limiter.getBytesInFlight}," +
              s"compressionSubmitted=${compressionTasksSubmitted.get()}," +
              s"compressionStarted=${compressionTasksStarted.get()}," +
              s"compressionCompleted=${compressionTasksCompleted.get()}," +
              s"compressionFailed=${compressionTasksFailed.get()}," +
              s"pools={${RapidsShuffleInternalManagerBase.threadPoolDiagnosticSnapshot}}," +
              s"taskStack=$stack")
        }
      }
    })

    // Multi-batch tracking
    val batchStates = new ArrayBuffer[BatchState]()
    val partialFiles = new ArrayBuffer[PartialFile]()
    var currentBatchId: Int = 0
    var previousMaxPartition: Int = -1
    var isMultiBatch: Boolean = false

    // Create initial batch state
    var currentBatch = createBatchState(currentBatchId, mapOutputWriter)

    try {
      var inputFetchStart = System.nanoTime()
      while (records.hasNext) {
        markDiagnosticProgress("input_next")
        val record = records.next()
        inputFetchTimeNs += System.nanoTime() - inputFetchStart

        val key = record._1
        val value = record._2
        val reducePartitionId: Int = partitioner.getPartition(key)

        // Detect multi-batch: partition ID must be strictly increasing within a batch.
        // If current partition ID < previous max, it means we've jumped back to an earlier
        // partition, indicating a new upstream GPU batch. Note: we use < instead of <= because
        // consecutive identical partition IDs can occur in two scenarios:
        // 1. Reslicing: when a partition's data exceeds maxCpuBatchSize
        // 2. Data skew: multiple GPU batches each containing only the same partition's data
        // In both cases, merging them into a single shuffle batch is correct and more efficient
        // (fewer partial files, less merge overhead).
        if (reducePartitionId < previousMaxPartition) {
          if (!isMultiBatch) {
            isMultiBatch = true
            logDebug(s"Detected multi-batch scenario for shuffle $shuffleId, " +
              s"transitioning to pipeline mode")
          }

          // Signal current batch is complete by setting maxPartitionIdQueued to numPartitions.
          // This tells the merger thread that all partitions (0 to numPartitions-1) have been
          // queued, so it can finish writing remaining partitions without waiting.
          // Schedule the merger in case it yielded while waiting for more work.
          // Note: We don't block here - the merger runs in parallel while we start next batch.
          currentBatch.maxPartitionIdQueued.set(numPartitions)
          currentBatch.scheduleMerger()

          // Add to list for later finalization
          batchStates += currentBatch

          // Immediately create new batch and continue processing (pipeline!)
          currentBatchId += 1
          val newWriter = shuffleExecutorComponents.createMapOutputWriter(
            shuffleId,
            mapId,
            numPartitions)
          mapOutputWriters += newWriter  // Track for cleanup
          currentBatch = createBatchState(currentBatchId, newWriter)

          // Reset to -1 for new batch. This ensures the first record of the new batch
          // (with any valid partition ID >= 0) won't trigger another batch switch,
          // since reducePartitionId > -1 will always be true.
          previousMaxPartition = -1
        }

        recordsWritten += 1
        previousMaxPartition = math.max(previousMaxPartition, reducePartitionId)

        // Get or create record queue for this partition in current batch
        val recordQueue = currentBatch.partitionRecords.computeIfAbsent(reducePartitionId,
          _ => new ConcurrentLinkedQueue[Future[CompressedRecord]]())

        val (cb, recordSize) = incRefCountAndGetSize(value)

        // Acquire limiter and process compression task immediately
        val waitOnLimiterStart = System.nanoTime()
        markDiagnosticProgress("limiter_acquire")
        try {
          limiter.acquireOrBlock(recordSize)
        } catch {
          case t: Throwable =>
            cb.close()
            throw t
        }
        waitTimeOnLimiterNs += System.nanoTime() - waitOnLimiterStart

        val batchForRecord = currentBatch
        val compressionQueuedNs = System.nanoTime()
        val compressionTask = new FutureTask[CompressedRecord](new Callable[CompressedRecord] {
          override def call(): CompressedRecord = {
            val compressionExecutionStartNs = System.nanoTime()
            compressionQueueWaitTimeNs.addAndGet(
              compressionExecutionStartNs - compressionQueuedNs)
            compressionTasksStarted.incrementAndGet()
            var releasedQuota = 0L
            try {
              val result = withResource(cb) { _ =>
                val compressionStartNs = System.nanoTime()
                // Create a new buffer for this record.
                // The buffer is closed by the merger thread after writing to disk.
                val buffer = new OpenByteArrayOutputStream()

                val adaptiveVector = cb.column(0) match {
                  case adaptive: AdaptiveSerializedColumnVector =>
                    if (adaptive.shouldReportDecision()) {
                      val proposedBackend = if (adaptive.isGpuProposed()) {
                        ShuffleCompressionBackend.NvcompGpuZstd
                      } else {
                        ShuffleCompressionBackend.SparkCpuZstd
                      }
                      val selectedBackend = if (adaptive.isGpuSelected()) {
                        ShuffleCompressionBackend.NvcompGpuZstd
                      } else {
                        ShuffleCompressionBackend.SparkCpuZstd
                      }
                      AdaptiveShuffleCompressionMetrics.record(
                        shuffleId, TaskCompressionPlan(selectedBackend, proposedBackend))
                    }
                    Some(adaptive)
                  case _ =>
                    None
                }

                closeOnExcept(buffer) { _ =>
                  cb.column(0) match {
                    case precompressed: PrecompressedSerializedColumnVector =>
                      writePrecompressedFrames(precompressed.getWrap, buffer)
                    case _ =>
                      val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, reducePartitionId)
                      val compressedOutputStream = blockManager.serializerManager.wrapStream(
                        shuffleBlockId, buffer)
                      val serializationStream = serializerInstance.serializeStream(
                        compressedOutputStream)
                      withResource(serializationStream) { serializer =>
                        serializer.writeKey(key.asInstanceOf[Any])
                        serializer.writeValue(value.asInstanceOf[Any])
                      }
                  }

                  // Track total written data size (compressed size)
                  val compressedSize = buffer.getCount.toLong
                  totalCompressedSize.addAndGet(compressedSize)
                  adaptiveVector.foreach { adaptive =>
                    val backend = if (adaptive.isGpuSelected()) {
                      ShuffleCompressionBackend.NvcompGpuZstd
                    } else {
                      ShuffleCompressionBackend.SparkCpuZstd
                    }
                    val rawBytes = adaptive match {
                      case precompressed: PrecompressedSerializedColumnVector =>
                        precompressed.getUncompressedLength
                      case _ =>
                        recordSize
                    }
                    val compressionTimeNs = if (adaptive.isGpuSelected()) {
                      adaptive.getGpuCompressionTimeNs
                    } else {
                      System.nanoTime() - compressionStartNs
                    }
                    AdaptiveShuffleCompressionMetrics.recordWork(
                      shuffleId,
                      backend,
                      rawBytes,
                      compressedSize,
                      compressionTimeNs,
                      reservationTimeNs = 0L)
                  }

                  // Release excess quota immediately after compression.
                  // Data is now in OpenByteArrayOutputStream (heap), only need to hold
                  // compressedSize quota until Merger writes to disk.
                  // Note: excessQuota can be 0 if compression doesn't reduce size (or expands)
                  val excessQuota = math.max(0L, recordSize - compressedSize)
                  if (excessQuota > 0) {
                    limiter.release(excessQuota)
                    releasedQuota = excessQuota
                  }

                  // Return CompressedRecord with buffer and remaining quota for Merger
                  // Total released = excessQuota + remainingQuota should equal recordSize
                  val remainingQuota = recordSize - excessQuota
                  CompressedRecord(buffer, compressedSize, remainingQuota)
                }
              }
              compressionTasksCompleted.incrementAndGet()
              result
            } catch {
              case e: Exception =>
                compressionTasksFailed.incrementAndGet()
                limiter.release(recordSize - releasedQuota)
                throw new IOException(
                  s"Failed compression task for shuffle $shuffleId, map $mapId, " +
                    s"partition $reducePartitionId", e)
              case t: Throwable =>
                compressionTasksFailed.incrementAndGet()
                limiter.release(recordSize - releasedQuota)
                throw t
            } finally {
              compressionExecutionTimeNs.addAndGet(
                System.nanoTime() - compressionExecutionStartNs)
            }
          }
        }) {
          override def done(): Unit = {
            // FutureTask invokes done only after isDone becomes true.
            batchForRecord.scheduleMerger()
          }
        }
        compressionTasksSubmitted.incrementAndGet()
        markDiagnosticProgress("compression_submit")
        val future = RapidsShuffleInternalManagerBase.queueWriteTask(compressionTask)

        currentBatch.maxPartitionIdQueued.synchronized {
          recordQueue.add(future)
          currentBatch.maxPartitionIdQueued.set(
            math.max(currentBatch.maxPartitionIdQueued.get(), reducePartitionId))
        }

        // Schedule a merger step to process this record when compression is ready.
        currentBatch.scheduleMerger()

        // Reset timer for next iteration's hasNext/next
        markDiagnosticProgress("input_has_next")
        inputFetchStart = System.nanoTime()
      }
      // Account for the final hasNext call that returned false
      inputFetchTimeNs += System.nanoTime() - inputFetchStart

      // Mark end of last batch by setting maxPartitionIdQueued to numPartitions.
      markDiagnosticProgress("finalize_last_batch")
      // This signals the merger that all partitions have been queued.
      currentBatch.maxPartitionIdQueued.set(numPartitions)
      currentBatch.scheduleMerger()

      // Add last batch to list
      batchStates += currentBatch

      // Wait for all batches to complete (now they can finish in parallel!)
      var totalSerializationWaitTimeNs: Long = 0L
      batchStates.foreach { batch =>
        try {
          val waitStart = System.nanoTime()
          var mergerComplete = false
          while (!mergerComplete) {
            try {
              markDiagnosticProgress(s"merger_wait_batch_${batch.batchId}")
              batch.mergerFuture.get(mergerWaitDiagnosticIntervalSeconds, TimeUnit.SECONDS)
              mergerComplete = true
            } catch {
              case _: TimeoutException =>
                val context = TaskContext.get()
                logWarning(
                  s"RAPIDS_SHUFFLE_HANG_DIAGNOSTIC " +
                    s"stage=${context.stageId()},partition=${context.partitionId()}," +
                    s"attempt=${context.attemptNumber()},taskAttempt=${context.taskAttemptId()}," +
                    s"shuffle=$shuffleId,map=$mapId," +
                    s"bytesInFlight=${limiter.getBytesInFlight}," +
                    s"compressionSubmitted=${compressionTasksSubmitted.get()}," +
                    s"compressionStarted=${compressionTasksStarted.get()}," +
                    s"compressionCompleted=${compressionTasksCompleted.get()}," +
                    s"compressionFailed=${compressionTasksFailed.get()}," +
                    s"${batch.merger.diagnosticSnapshot(batch.batchId)}," +
                    s"pools={${RapidsShuffleInternalManagerBase.threadPoolDiagnosticSnapshot}}")
            }
          }
          totalSerializationWaitTimeNs += System.nanoTime() - waitStart
        } catch {
          case ee: ExecutionException => throw ee.getCause
        }

        // CRITICAL: Preserve handle before any commit
        // commitAllPartitions() would flush/rename data, so we extract first
        val mtCatalog = GpuShuffleEnv.getMultithreadedCatalog
        if (isMultiBatch || mtCatalog.isDefined) {
          // For multi-batch or when using catalog mode, extract handle
          val (handle, partLengths) = extractHandleAndLengthsFromWriter(
            batch.mapOutputWriter)
          partialFiles += PartialFile(handle, partLengths, batch.mapOutputWriter)
        } else {
          // Single batch without catalog: commit normally
          commitAllPartitions(batch.mapOutputWriter, true)
        }
      }

      // Update write metrics (except writeTime which is calculated at the end)
      writeMetrics.incRecordsWritten(recordsWritten)
      writeMetrics.incBytesWritten(totalCompressedSize.get())
      limiterWaitTimeMetric.foreach(_ += waitTimeOnLimiterNs)
      serializationWaitTimeMetric.foreach(_ += totalSerializationWaitTimeNs)
      inputFetchTimeMetric.foreach(_ += inputFetchTimeNs)
      compressionQueueWaitTimeMetric.foreach(_ += compressionQueueWaitTimeNs.get())
      compressionTimeMetric.foreach(_ += compressionExecutionTimeNs.get())
      mergerWriteTimeMetric.foreach(_ += mergerWriteTimeNs.get())
      compressionTaskCountMetric.foreach(_ += compressionTasksSubmitted.get())

    } finally {
      hangDiagnostic.cancel(false)
      // Helper to cleanup a single batch
      def cleanupBatch(batch: BatchState): Unit = {
        // Cancel merger completion and any currently scheduled step.
        batch.cancelMerger()

        // Cancel pending futures and close their buffers
        batch.partitionRecords.values().asScala.foreach { recordQueue =>
          var future = recordQueue.poll()
          while (future != null) {
            future.cancel(true)
            // If future already completed, close the buffer and release the quota that
            // would otherwise have been released by the merger after writing the record.
            if (future.isDone && !future.isCancelled) {
              try {
                val record = future.get()
                record.buffer.close()
                limiter.release(record.remainingQuota)
              } catch {
                case _: Exception => // Ignore cleanup errors
              }
            }
            future = recordQueue.poll()
          }
        }
      }

      // Cleanup all tracked batch states
      batchStates.foreach(cleanupBatch)

      // Also cleanup currentBatch if it was never added to batchStates
      // (exception occurred before batchStates += currentBatch)
      if (currentBatch != null && !batchStates.contains(currentBatch)) {
        cleanupBatch(currentBatch)
      }

    }

    // Track whether handles have been transferred to catalog or merged
    var handlesTransferred = false

    try {
      // Handle final output
      val mtCatalog = GpuShuffleEnv.getMultithreadedCatalog

      val result = mtCatalog match {
        case Some(catalog) =>
          // Store data in MultithreadedShuffleBufferCatalog instead of merging.
          // The catalog takes ownership of the handles.
          val lengths = storePartialFilesInCatalog(catalog, partialFiles.toSeq, isMultiBatch)
          handlesTransferred = true
          lengths
        case None =>
          // Fallback to original merge behavior
          if (isMultiBatch) {
            // Multi-batch: create NEW writer for final merge
            val finalMergeWriter = shuffleExecutorComponents.createMapOutputWriter(
              shuffleId,
              mapId,
              numPartitions)
            mapOutputWriters += finalMergeWriter

            finalMergeWriter match {
              case rapidsWriter: RapidsLocalDiskShuffleMapOutputWriter =>
                rapidsWriter.setForceFileOnlyMode()
              case _ =>
            }

            // mergePartialFiles closes handles in its finally block
            val partialFileMergeStartNs = System.nanoTime()
            val lengths = mergePartialFiles(partialFiles.toSeq, finalMergeWriter)
            partialFileMergeTimeNs += System.nanoTime() - partialFileMergeStartNs
            handlesTransferred = true
            lengths
          } else {
            getPartitionLengths
          }
      }

      // Update write time: total time from start minus input fetch time
      val totalWriteTime = System.nanoTime() - writeStartTime
      writeMetrics.incWriteTime(totalWriteTime - inputFetchTimeNs)
      partialFileMergeTimeMetric.foreach(_ += partialFileMergeTimeNs)
      result
    } finally {
      // Clean up handles if they weren't transferred to catalog or merged
      if (!handlesTransferred) {
        partialFiles.foreach { pf =>
          try {
            pf.handle.close()
          } catch {
            case e: Exception =>
              logWarning(s"Failed to close partial file handle during cleanup", e)
          }
        }
      }
    }
  }

  /**
   * Store partial files in MultithreadedShuffleBufferCatalog instead of merging.
   * This avoids the I/O cost of merging while keeping data in memory when possible.
   *
   * @param catalog the MultithreadedShuffleBufferCatalog to store data
   * @param partialFiles list of partial files from all batches
   * @param isMultiBatch whether this is a multi-batch scenario
   * @return array of partition lengths (sum across all batches for each partition)
   */
  private def storePartialFilesInCatalog(
      catalog: MultithreadedShuffleBufferCatalog,
      partialFiles: Seq[PartialFile],
      isMultiBatch: Boolean): Array[Long] = {
    val accumulatedLengths = new Array[Long](numPartitions)

    if (isMultiBatch) {
      // Multi-batch: store each partial file's partitions in catalog
      partialFiles.foreach { pf =>
        var offset = 0L
        for (partId <- 0 until numPartitions) {
          val length = pf.partitionLengths(partId)
          if (length > 0) {
            catalog.addPartition(shuffleId, mapId, partId, pf.handle, offset, length)
          }
          accumulatedLengths(partId) += length
          offset += length
        }
        // Don't close the handle here - it will be closed when shuffle is unregistered
        // Disk write savings are recorded by the reducer when reading the data
      }
    } else {
      // Single batch: use handle already extracted in the write loop
      // (partialFiles should have exactly one element in single-batch mode)
      val pf = partialFiles.head
      var offset = 0L
      for (partId <- 0 until numPartitions) {
        val length = pf.partitionLengths(partId)
        if (length > 0) {
          catalog.addPartition(shuffleId, mapId, partId, pf.handle, offset, length)
        }
        accumulatedLengths(partId) = length
        offset += length
      }
      // Disk write savings are recorded by the reducer when reading the data
    }

    accumulatedLengths
  }

  /**
   * Merge multiple partial sorted files into final output.
   * Each partial file contains data for all partitions (0 to N) from one GPU batch.
   * The merged file will have: partition 0 from all batches, partition 1 from all batches, etc.
   *
   * Layout of merged file:
   *   partition 0 data from partial file 0
   *   partition 0 data from partial file 1
   *   ...
   *   partition 0 data from partial file M
   *   partition 1 data from partial file 0
   *   partition 1 data from partial file 1
   *   ...
   */
  private def mergePartialFiles(
      partialFiles: Seq[PartialFile],
      finalWriter: ShuffleMapOutputWriter): Array[Long] = {

    try {
      // For each partition, copy data from all partial files in order
      // Note: Each partial file is read sequentially from beginning to end,
      // so no need to reset read position between partitions
      (0 until numPartitions).foreach { partitionId =>
        val partWriter = finalWriter.getPartitionWriter(partitionId)

        withResource(partWriter.openStream()) { os =>
          partialFiles.foreach { partialFile =>
            val partitionLength = partialFile.partitionLengths(partitionId)
            if (partitionLength > 0) {
              val handle = partialFile.handle

              // Read partition data sequentially
              // No reset needed - handle maintains read position automatically
              val temp = new Array[Byte](fileBufferSize)
              var remaining = partitionLength
              while (remaining > 0) {
                val bytesToRead = math.min(remaining, temp.length).toInt
                val bytesRead = handle.read(temp, 0, bytesToRead)
                if (bytesRead > 0) {
                  os.write(temp, 0, bytesRead)
                  remaining -= bytesRead
                } else {
                  throw new IOException(
                    s"EOF reading partition $partitionId " +
                      s"from partial file ${partialFiles.indexOf(partialFile)}, " +
                      s"expected $partitionLength bytes, got ${partitionLength - remaining}")
                }
              }
            }
          }
        }
      }
    } finally {
      // Cleanup partial file handles
      partialFiles.foreach { pf =>
        try {
          pf.handle.close()
        } catch {
          case e: Exception =>
            logWarning(s"Failed to close partial file handle during cleanup", e)
        }
      }
    }

    // Commit final merged output
    commitAllPartitions(finalWriter, true)
  }

  /**
   * Extract partial file handle and partitionLengths from ShuffleMapOutputWriter.
   * Since we always use RapidsLocalDiskShuffleMapOutputWriter, this is straightforward.
   */
  private def extractHandleAndLengthsFromWriter(writer: ShuffleMapOutputWriter):
  (SpillablePartialFileHandle, Array[Long]) = {
    writer match {
      case rapidsWriter: RapidsLocalDiskShuffleMapOutputWriter =>
        // finishWritePhase() will enable spill
        rapidsWriter.finishWritePhase()
        val handle = rapidsWriter.getPartialFileHandle().getOrElse {
          throw new IllegalStateException("RAPIDS writer should have a handle")
        }
        val lengths = rapidsWriter.getPartitionLengths()
        (handle, lengths)
      case _ =>
        throw new IllegalStateException(
          s"Unexpected writer type: ${writer.getClass.getName}. " +
            "RapidsShuffleManager should always use RapidsLocalDiskShuffleMapOutputWriter.")
    }
  }

  def getBytesInFlight: Long = limiter.getBytesInFlight
}

class BytesInFlightLimiter(maxBytesInFlight: Long) {
  private var inFlight: Long = 0L
  private var abortCause: Throwable = _

  def acquire(sz: Long): Boolean = {
    if (sz == 0) {
      true
    } else {
      synchronized {
        if (inFlight == 0 || sz + inFlight < maxBytesInFlight) {
          inFlight += sz
          true
        } else {
          false
        }
      }
    }
  }

  def acquireOrBlock(sz: Long): Unit = {
    synchronized {
      var acquired = acquire(sz)
      while (!acquired && abortCause == null) {
        wait()
        acquired = acquire(sz)
      }
      if (abortCause != null) {
        if (acquired) {
          inFlight -= sz
        }
        throw abortCause
      }
    }
  }

  def abort(cause: Throwable): Unit = synchronized {
    if (abortCause == null) {
      abortCause = cause
    }
    notifyAll()
  }

  def release(sz: Long): Unit = synchronized {
    inFlight -= sz
    notifyAll()
  }

  def getBytesInFlight: Long = synchronized {
    inFlight
  }
}

abstract class RapidsShuffleThreadedReaderBase[K, C](
    handle: ShuffleHandleWithMetrics[K, C, C],
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    maxBytesInFlight: Long,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker,
    canUseBatchFetch: Boolean = false,
    numReaderThreads: Int = 0,
    readerTaskAdmissionConfig: Option[ReaderTaskAdmissionConfig] = None)
  extends ShuffleReader[K, C] with Logging {

  case class GetMapSizesResult(
      blocksByAddress: Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])],
      canEnableBatchFetch: Boolean)

  protected def getMapSizes: GetMapSizesResult

  private val GetMapSizesResult(blocksByAddress, canEnableBatchFetch) = getMapSizes

  // For spark versions 3.2.0+ `canEnableBatchFetch` can be false given merged
  // map output
  private val shouldBatchFetch = canUseBatchFetch && canEnableBatchFetch

  private val sqlMetrics = handle.metrics
  private val dep = handle.dependency
  private val deserializationTimeNs = sqlMetrics.get(METRIC_SHUFFLE_DESERIALIZATION_TIME)
  private val shuffleReadTimeNs = sqlMetrics.get(METRIC_SHUFFLE_READ_TIME)
  private val dataReadSize = sqlMetrics.get(METRIC_DATA_READ_SIZE)
  // New metrics for wall time breakdown
  private val ioWaitTimeNs = sqlMetrics.get(METRIC_THREADED_READER_IO_WAIT_TIME)
  private val deserWaitTimeNs = sqlMetrics.get(METRIC_THREADED_READER_DESER_WAIT_TIME)
  private val futureWaitTimeNs = sqlMetrics.get(METRIC_THREADED_READER_FUTURE_WAIT_TIME)
  private val resultQueueWaitTimeNs =
    sqlMetrics.get(METRIC_THREADED_READER_RESULT_QUEUE_WAIT_TIME)
  private val workerQueueDelayNs = sqlMetrics.get(METRIC_THREADED_READER_WORKER_QUEUE_DELAY)
  private val workerActiveTimeNs = sqlMetrics.get(METRIC_THREADED_READER_WORKER_ACTIVE_TIME)
  private val workerCpuTimeNs = sqlMetrics.get(METRIC_THREADED_READER_WORKER_CPU_TIME)
  private val workerTaskCount = sqlMetrics.get(METRIC_THREADED_READER_WORKER_TASK_COUNT)
  // Limiter metrics
  private val limiterAcquireCount =
    sqlMetrics.get(METRIC_THREADED_READER_LIMITER_ACQUIRE_COUNT)
  private val limiterAcquireFailCount =
    sqlMetrics.get(METRIC_THREADED_READER_LIMITER_ACQUIRE_FAIL_COUNT)
  private val limiterPendingBlockCount =
    sqlMetrics.get(METRIC_THREADED_READER_LIMITER_PENDING_BLOCK_COUNT)
  private val admissionWaitTimeNs =
    sqlMetrics.get(METRIC_THREADED_READER_ADMISSION_WAIT_TIME)
  private val admissionAcquireCount =
    sqlMetrics.get(METRIC_THREADED_READER_ADMISSION_ACQUIRE_COUNT)
  private val admissionDecisionCount =
    sqlMetrics.get(METRIC_THREADED_READER_ADMISSION_DECISION_COUNT)
  private val admissionIncreaseCount =
    sqlMetrics.get(METRIC_THREADED_READER_ADMISSION_INCREASE_COUNT)
  private val admissionDecreaseCount =
    sqlMetrics.get(METRIC_THREADED_READER_ADMISSION_DECREASE_COUNT)
  private val admissionHoldCount =
    sqlMetrics.get(METRIC_THREADED_READER_ADMISSION_HOLD_COUNT)
  private val admissionDesiredPermitsSum =
    sqlMetrics.get(METRIC_THREADED_READER_ADMISSION_DESIRED_PERMITS_SUM)
  private val admissionGpuTargetSum =
    sqlMetrics.get(METRIC_THREADED_READER_ADMISSION_GPU_TARGET_SUM)
  private val localWorkerQueueDelayNs = new AtomicLong()
  private val localWorkerActiveTimeNs = new AtomicLong()
  private val localLimiterAcquireCount = new AtomicLong()
  private val localLimiterFailureCount = new AtomicLong()

  private var shuffleReadRange: NvtxId = NvtxRegistry.THREADED_READER_READ.push()

  private def closeShuffleReadRange(): Unit = {
    if (shuffleReadRange != null) {
      shuffleReadRange.pop()
      shuffleReadRange = null
    }
  }

  onTaskCompletion(context) {
    // should not be needed, but just in case
    closeShuffleReadRange()
  }

  private def fetchContinuousBlocksInBatch: Boolean = {
    val conf = SparkEnv.get.conf
    val serializerRelocatable = dep.serializer.supportsRelocationOfSerializedObjects
    val compressed = conf.get(config.SHUFFLE_COMPRESS)
    val codecConcatenation = if (compressed) {
      CompressionCodec.supportsConcatenationOfSerializedStreams(CompressionCodec.createCodec(conf))
    } else {
      true
    }
    val useOldFetchProtocol = conf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)
    // SPARK-34790: Fetching continuous blocks in batch is incompatible with io encryption.
    val ioEncryption = conf.get(config.IO_ENCRYPTION_ENABLED)

    val doBatchFetch = shouldBatchFetch && serializerRelocatable &&
      (!compressed || codecConcatenation) && !useOldFetchProtocol && !ioEncryption
    if (shouldBatchFetch && !doBatchFetch) {
      logDebug("The feature tag of continuous shuffle block fetching is set to true, but " +
        "we can not enable the feature because other conditions are not satisfied. " +
        s"Shuffle compress: $compressed, serializer relocatable: $serializerRelocatable, " +
        s"codec concatenation: $codecConcatenation, use old shuffle fetch protocol: " +
        s"$useOldFetchProtocol, io encryption: $ioEncryption.")
    }
    doBatchFetch
  }


  class RapidsShuffleThreadedBlockIterator(
      fetcherIterator: RapidsShuffleBlockFetcherIterator,
      serializer: GpuColumnarBatchSerializer)
    extends Iterator[(Any, Any)] {
    private val queued = new LinkedBlockingQueue[(Any, Any)]
    private val futures = new mutable.Queue[Future[Option[BlockState]]]()
    private val serializerInstance = serializer.newInstance()
    private val limiter = new BytesInFlightLimiter(maxBytesInFlight)
    private val fallbackIter: Iterator[(Any, Any)] with AutoCloseable =
      if (numReaderThreads == 1) {
        // this is the non-optimized case, where we add metrics to capture the blocked
        // time and the deserialization time as part of the shuffle read time.
        new Iterator[(Any, Any)]() with AutoCloseable {
          private var currentIter: Iterator[(Any, Any)] = _
          private var currentStream: AutoCloseable = _
          override def hasNext: Boolean = fetcherIterator.hasNext || (
            currentIter != null && currentIter.hasNext)

          override def close(): Unit = {
            if (currentStream != null) {
              currentStream.close()
              currentStream = null
            }
          }

          override def next(): (Any, Any) = {
            val fetchTimeStart = System.nanoTime()
            var readBlockedTime = 0L
            if (currentIter == null || !currentIter.hasNext) {
              val readBlockedStart = System.nanoTime()
              val (_, stream) = fetcherIterator.next()
              readBlockedTime = System.nanoTime() - readBlockedStart
              // this is stored only to call close on it
              currentStream = stream
              currentIter = serializerInstance.deserializeStream(stream).asKeyValueIterator
            }
            val res = currentIter.next()
            val fetchTime = System.nanoTime() - fetchTimeStart
            deserializationTimeNs.foreach(_ += (fetchTime - readBlockedTime))
            shuffleReadTimeNs.foreach(_ += fetchTime)
            res
          }
        }
      } else {
        null
      }

    // Register a completion handler to close any queued cbs,
    // pending iterators, or futures
    onTaskCompletion(context) {
      // remove any materialized batches
      queued.forEach {
        case (_, cb:ColumnarBatch) => cb.close()
      }
      queued.clear()

      // close any materialized BlockState objects that are holding onto netty buffers or
      // file descriptors
      pendingIts.safeClose()
      pendingIts.clear()

      // we could have futures left that are either done or in flight
      // we need to cancel them and then close out any `BlockState`
      // objects that were created (to remove netty buffers or file descriptors)
      val futuresAndCancellations = futures.map { f =>
        val didCancel = f.cancel(true)
        (f, didCancel)
      }

      // if we weren't able to cancel, we are going to make a best attempt at getting the future
      // and we are going to close it. The timeout is to prevent an (unlikely) infinite wait.
      // If we do timeout then this handler is going to throw.
      var failedFuture: Option[Throwable] = None
      futuresAndCancellations
        .filter { case (_, didCancel) => !didCancel }
        .foreach { case (future, _) =>
          try {
            // this could either be a successful future, or it finished with exception
            // the case when it will fail with exception is when the underlying stream is closed
            // as part of the shutdown process of the task.
            future.get(10, TimeUnit.MILLISECONDS)
              .foreach(_.close())
          } catch {
            case t: Throwable =>
              // this is going to capture the first exception and not worry about others
              // because we probably don't want to spam the UI or log with an exception per
              // block we are fetching
              if (failedFuture.isEmpty) {
                failedFuture = Some(t)
              }
          }
        }
      futures.clear()
      try {
        if (fallbackIter != null) {
          fallbackIter.close()
        }
      } catch {
        case t: Throwable =>
          if (failedFuture.isEmpty) {
            failedFuture = Some(t)
          } else {
            failedFuture.get.addSuppressed(t)
          }
      } finally {
        failedFuture.foreach { e =>
          throw e
        }
      }
    }

    override def hasNext: Boolean = {
      if (fallbackIter != null) {
        fallbackIter.hasNext
      } else {
        pendingIts.nonEmpty || futures.nonEmpty || queued.size() > 0 ||
          fetcherIterator.hasNext
      }
    }

    case class BlockState(
        blockId: BlockId,
        batchIter: BaseSerializedTableIterator,
        origStream: AutoCloseable)
      extends Iterator[(Any, Any)] with AutoCloseable {

      private var nextBatchSize = {
        var success = false
        try {
          val res = batchIter.peekNextBatchSize().getOrElse(0L)
          success = true
          res
        } finally {
          if (!success) {
            // we tried to read from a stream, but something happened
            // lets close it
            close()
          }
        }
      }

      def getNextBatchSize: Long = nextBatchSize

      override def hasNext: Boolean = batchIter.hasNext

      override def next(): (Any, Any) = {
        val nextBatch = batchIter.next()
        var success = false
        try {
          nextBatchSize = batchIter.peekNextBatchSize().getOrElse(0L)
          success = true
          nextBatch
        } finally {
          if (!success) {
            // the call to get a next header threw. We need to close `nextBatch`.
            nextBatch match {
              case (_, cb: ColumnarBatch) => cb.close()
            }
          }
        }
      }

      override def close(): Unit = {
        origStream.close() // make sure we call this on error
      }
    }

    private val pendingIts = new mutable.Queue[BlockState]()

    override def next(): (Any, Any) = {
      require(hasNext, "called next on an empty iterator")
      val res = NvtxRegistry.PARALLEL_DESERIALIZER_ITERATOR_NEXT {
        val result = if (fallbackIter != null) {
          fallbackIter.next()
        } else {
          var waitTime: Long = 0L
          var waitTimeStart: Long = 0L
          popFetchedIfAvailable()
          waitTime = 0L
          if (futures.nonEmpty) {
            NvtxRegistry.BATCH_WAIT {
              waitTimeStart = System.nanoTime()
              val pending = futures.dequeue().get // wait for one future
              val futureWaitThisCall = System.nanoTime() - waitTimeStart
              waitTime += futureWaitThisCall
              deserWaitTimeNs.foreach(_ += futureWaitThisCall)
              futureWaitTimeNs.foreach(_ += futureWaitThisCall)
              // if the future returned a block state, we have more work to do
              pending match {
                case Some(leftOver@BlockState(_, _, _)) =>
                  pendingIts.enqueue(leftOver)
                case _ => // done
              }
            }
          }

          if (pendingIts.nonEmpty) {
            // if we had pending iterators, we should try to see if now one can be handled
            popFetchedIfAvailable()
          }

          // We either have added futures and so will have items queued
          // or we already exhausted the fetchIterator and are just waiting
          // for our futures to finish. Either way, it's safe to block
          // here while we wait.
          waitTimeStart = System.nanoTime()
          val res = queued.take()
          val queueWaitThisCall = System.nanoTime() - waitTimeStart
          // limiter is now released immediately after deserialization in deserializeTask
          res match {
            case (_, _: ColumnarBatch) =>
              popFetchedIfAvailable()
            case _ => // do nothing
          }
          waitTime += queueWaitThisCall
          deserWaitTimeNs.foreach(_ += queueWaitThisCall)
          resultQueueWaitTimeNs.foreach(_ += queueWaitThisCall)
          deserializationTimeNs.foreach(_ += waitTime)
          shuffleReadTimeNs.foreach(_ += waitTime)
          res
        }

        val uncompressedSize = result match {
          case (_, cb: ColumnarBatch) => SerializedTableColumn.getMemoryUsed(cb)
          case _ => 0 // TODO: do we need to handle other types here?
        }

        dataReadSize.foreach(_ += uncompressedSize)
        result
      }

      // if this is the last call, close our range
      if (!hasNext) {
        closeShuffleReadRange()
      }

      res
    }

    private def deserializeTask(blockState: BlockState, acquiredSize: Long): Unit = {
      val submittedAt = System.nanoTime()
      futures += RapidsShuffleInternalManagerBase.queueReadTask(() => {
        val activeStart = System.nanoTime()
        val cpuStart = ReaderThreadCpuTime.now()
        workerQueueDelayNs.foreach(_ += activeStart - submittedAt)
        localWorkerQueueDelayNs.addAndGet(activeStart - submittedAt)
        workerTaskCount.foreach(_ += 1L)
        var success = false
        // Track the size we need to release (starts with the pre-acquired size)
        var sizeToRelease = acquiredSize
        try {
          var currentBatchSize = blockState.getNextBatchSize
          var didFit = true
          while (blockState.hasNext && didFit) {
            val batch = blockState.next()
            queued.offer(batch)
            // peek at the next batch
            currentBatchSize = blockState.getNextBatchSize
            limiterAcquireCount.foreach(_ += 1)
            didFit = limiter.acquire(currentBatchSize)
            if (didFit) {
              // Successfully acquired, add to sizeToRelease for later release
              sizeToRelease += currentBatchSize
            } else {
              limiterAcquireFailCount.foreach(_ += 1)
            }
          }
          success = true
          if (!didFit) {
            Some(blockState)
          } else {
            None // no further batches
          }
        } finally {
          val activeTimeNs = System.nanoTime() - activeStart
          workerActiveTimeNs.foreach(_ += activeTimeNs)
          localWorkerActiveTimeNs.addAndGet(activeTimeNs)
          workerCpuTimeNs.foreach(_ += math.max(0L, ReaderThreadCpuTime.now() - cpuStart))
          // Release limiter immediately after deserialization completes
          limiter.release(sizeToRelease)
          // Close blockState (Netty buffer) immediately if:
          // - failed (success = false), or
          // - all batches processed (success = true and returned None)
          if (!success || !blockState.hasNext) {
            blockState.close()
          }
        }
      })
    }

    private def popFetchedIfAvailable(): Unit = {
      // If fetcherIterator is not exhausted, we try and get as many
      // ready results.
      if (pendingIts.nonEmpty) {
        var continue = true
        while(pendingIts.nonEmpty && continue) {
          val blockState = pendingIts.head
          // check if we can handle the head batch now
          val nextBatchSize = blockState.getNextBatchSize
          limiterAcquireCount.foreach(_ += 1)
          localLimiterAcquireCount.incrementAndGet()
          if (limiter.acquire(nextBatchSize)) {
            // kick off deserialization task
            pendingIts.dequeue()
            deserializeTask(blockState, nextBatchSize)
          } else {
            limiterAcquireFailCount.foreach(_ += 1)
            localLimiterFailureCount.incrementAndGet()
            continue = false
          }
        }
      } else {
        if (fetcherIterator.hasNext) {
          NvtxRegistry.QUEUE_FETCHED {
            // `resultCount` is exposed from the fetcher iterator and if non-zero,
            // it means that there are pending results that need to be handled.
            // We max with 1, because there could be a race condition where
            // we are trying to get a batch and we haven't received any results
            // yet, we need to block on the fetch for this case so we have
            // something to return.
            var amountToDrain = Math.max(fetcherIterator.resultCount, 1)
            val fetchTimeStart = System.nanoTime()

            // We drain fetched results. That is, we push decode tasks
            // onto our queue until the results in the fetcher iterator
            // are all dequeued (the ones that were completed up until now).
            var readBlockedTime = 0L
            var didFit = true
            while (amountToDrain > 0 && fetcherIterator.hasNext && didFit) {
              amountToDrain -= 1
              // fetch block time accounts for time spent waiting for streams.next()
              val readBlockedStart = System.nanoTime()
              val (blockId: BlockId, inputStream) = fetcherIterator.next()
              val ioWaitThisBlock = System.nanoTime() - readBlockedStart
              readBlockedTime += ioWaitThisBlock
              ioWaitTimeNs.foreach(_ += ioWaitThisBlock)

              val deserStream = serializerInstance.deserializeStream(inputStream)
              val batchIter = deserStream.asKeyValueIterator
                .asInstanceOf[BaseSerializedTableIterator]
              val blockState = BlockState(blockId, batchIter, inputStream)
              // get the next known batch size (there could be multiple batches)
              val nextBatchSize = blockState.getNextBatchSize
              limiterAcquireCount.foreach(_ += 1)
              localLimiterAcquireCount.incrementAndGet()
              if (limiter.acquire(nextBatchSize)) {
                // we can fit at least the first batch in this block
                // kick off a deserialization task
                deserializeTask(blockState, nextBatchSize)
              } else {
                // first batch didn't fit, put iterator aside and stop asking for results
                // from the fetcher
                limiterAcquireFailCount.foreach(_ += 1)
                localLimiterFailureCount.incrementAndGet()
                limiterPendingBlockCount.foreach(_ += 1)
                pendingIts.enqueue(blockState)
                didFit = false
              }
            }
            // keep track of the overall metric which includes blocked time
            val fetchTime = System.nanoTime() - fetchTimeStart
            deserializationTimeNs.foreach(_ += (fetchTime - readBlockedTime))
            shuffleReadTimeNs.foreach(_ += fetchTime)
          }
        }
      }
    }
  }

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    val admission = RapidsShuffleInternalManagerBase.acquireReaderTaskAdmission(
      context, readerTaskAdmissionConfig)
    if (admission.acquired) {
      admissionWaitTimeNs.foreach(_ += admission.waitTimeNs)
      admissionAcquireCount.foreach(_ += 1L)
    }

    val wrappedStreams = RapidsShuffleBlockFetcherIterator.makeIterator(
      context,
      blockManager,
      SparkEnv.get,
      blocksByAddress,
      serializerManager,
      readMetrics,
      fetchContinuousBlocksInBatch)

    val recordIter = new RapidsShuffleThreadedBlockIterator(
      wrappedStreams,
      dep.serializer.asInstanceOf[GpuColumnarBatchSerializer])

    // Update the context task metrics for each record read.
    val recordMetricIter = recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      }
    val admittedIter = if (readerTaskAdmissionConfig.nonEmpty) {
      CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
        recordMetricIter,
        RapidsShuffleInternalManagerBase.releaseReaderTaskAdmission(
          context,
          readerTaskAdmissionConfig,
          ReaderTaskObservation(
            localWorkerQueueDelayNs.get(),
            localWorkerActiveTimeNs.get(),
            localLimiterAcquireCount.get(),
            localLimiterFailureCount.get())).foreach { decision =>
          admissionDecisionCount.foreach(_ += 1L)
          admissionDesiredPermitsSum.foreach(_ += decision.newPermits.toLong)
          admissionGpuTargetSum.foreach(_ += decision.gpuTarget.toLong)
          decision.reason match {
            case "gpu-target-increase" => admissionIncreaseCount.foreach(_ += 1L)
            case "gpu-target-decrease" => admissionDecreaseCount.foreach(_ += 1L)
            case _ => admissionHoldCount.foreach(_ += 1L)
          }
        })
    } else {
      recordMetricIter
    }
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      admittedIter, context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    val resultIter = dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data.
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        // Use completion callback to stop sorter if task was finished/cancelled.
        onTaskCompletion(context) {
          sorter.stop()
        }
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        aggregatedIter
    }

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as aggregator
        // or(and) sorter may have consumed previous interruptible iterator.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }
}

class RapidsCachingWriter[K, V](
    blockManager: BlockManager,
    // Never keep a reference to the ShuffleHandle in the cache as it being GCed triggers
    // the data being released
    handle: GpuShuffleHandle[K, V],
    mapId: Long,
    metricsReporter: ShuffleWriteMetricsReporter,
    catalog: ShuffleBufferCatalog,
    rapidsShuffleServer: Option[RapidsShuffleServer],
    metrics: Map[String, SQLMetric])
  extends RapidsCachingWriterBase[K, V](blockManager, handle, mapId, rapidsShuffleServer, catalog) {

  private val uncompressedMetric: SQLMetric = metrics(METRIC_DATA_SIZE)

  // This is here for the special case where we have no columns like with the .count
  // case or when we have 0-byte columns. We pick 100 as an arbitrary number so that
  // we can shuffle these degenerate batches, which have valid metadata and should be
  // used on the reducer side for computation.
  private val DEGENERATE_PARTITION_BYTE_SIZE_DEFAULT: Long = 100L

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // NOTE: This MUST NOT CLOSE the incoming batches because they are
    //       closed by the input iterator generated by GpuShuffleExchangeExec
    NvtxRegistry.RAPIDS_CACHING_WRITER_WRITE {
      var bytesWritten: Long = 0L
      var recordsWritten: Long = 0L
      records.foreach { p =>
        val partId = p._1.asInstanceOf[Int]
        val batch = p._2.asInstanceOf[ColumnarBatch]
        logDebug(s"Caching shuffle_id=${handle.shuffleId} map_id=$mapId, partId=$partId, "
          + s"batch=[num_cols=${batch.numCols()}, num_rows=${batch.numRows()}]")
        recordsWritten = recordsWritten + batch.numRows()
        var partSize: Long = 0
        val blockId = ShuffleBlockId(handle.shuffleId, mapId, partId)
        if (batch.numRows > 0 && batch.numCols > 0) {
          // Add the table to the shuffle store
          batch.column(0) match {
            case c: GpuPackedTableColumn =>
              val contigTable = c.getContiguousTable
              partSize = c.getTableBuffer.getLength
              uncompressedMetric += partSize
              catalog.addContiguousTable(
                blockId,
                contigTable,
                SpillPriorities.OUTPUT_FOR_SHUFFLE_INITIAL_TASK_PRIORITY)
            case c: GpuCompressedColumnVector =>
              partSize = c.getTableBuffer.getLength
              uncompressedMetric += c.getTableMeta.bufferMeta().uncompressedSize()
              catalog.addCompressedBatch(
                blockId,
                batch,
                SpillPriorities.OUTPUT_FOR_SHUFFLE_INITIAL_TASK_PRIORITY)
            case c =>
              throw new IllegalStateException(s"Unexpected column type: ${c.getClass}")
          }
          bytesWritten += partSize
          // if the size is 0 and we have rows, we are in a case where there are columns
          // but the type is such that there isn't a buffer in the GPU backing it.
          // For example, a Struct column without any members. We treat such a case as if it
          // were a degenerate table.
          if (partSize == 0 && batch.numRows() > 0) {
            sizes(partId) += DEGENERATE_PARTITION_BYTE_SIZE_DEFAULT
          } else {
            sizes(partId) += partSize
          }
        } else {
          // no device data, tracking only metadata
          val tableMeta = MetaUtils.buildDegenerateTableMeta(batch)
          catalog.addDegenerateRapidsBuffer(
            blockId,
            tableMeta)

          // ensure that we set the partition size to the default in this case if
          // we have non-zero rows, so this degenerate batch is shuffled.
          if (batch.numRows > 0) {
            sizes(partId) += DEGENERATE_PARTITION_BYTE_SIZE_DEFAULT
          }
        }
      }
      metricsReporter.incBytesWritten(bytesWritten)
      metricsReporter.incRecordsWritten(recordsWritten)
    }
  }


  def getPartitionLengths(): Array[Long] = {
    throw new UnsupportedOperationException("TODO")
  }
}

/**
 * A shuffle manager optimized for the RAPIDS Plugin For Apache Spark.
 * @note This is an internal class to obtain access to the private
 *       `ShuffleManager` and `SortShuffleManager` classes. When configuring
 *       Apache Spark to use the RAPIDS shuffle manager,
 */
class RapidsShuffleInternalManagerBase(conf: SparkConf, val isDriver: Boolean)
  extends ShuffleManager with RapidsShuffleHeartbeatHandler with Logging
  with RapidsShuffleReaderShim with ProxyShuffleReaderDelegate {

  def getServerId: BlockManagerId = server.fold(blockManager.blockManagerId)(_.getId)

  override def addPeer(peer: BlockManagerId): Unit = {
    transport.foreach { t =>
      try {
        t.connect(peer)
      } catch {
        case ex: Exception =>
          // We ignore the exception after logging in this instance because
          // we may have a peer that doesn't exist anymore by the time `addPeer` is invoked
          // due to a heartbeat response from the driver, or the peer may have a temporary network
          // issue.
          //
          // This is safe because `addPeer` is only invoked due to a heartbeat that is used to
          // opportunistically hide cost of initializing transport connections. The transport
          // will re-try if it must fetch from this executor at a later time, in that case
          // a connection failure causes the tasks to fail.
          logWarning(s"Unable to connect to peer $peer, ignoring!", ex)
      }
    }
  }

  private val rapidsConf = new RapidsConf(conf)

  if (!isDriver && rapidsConf.isMultiThreadedShuffleManagerMode) {
    RapidsShuffleInternalManagerBase.startThreadPoolIfNeeded(
      rapidsConf.shuffleMultiThreadedWriterThreads,
      rapidsConf.shuffleMultiThreadedReaderThreads)
  }

  protected val wrapped = new SortShuffleManager(conf)

  private[this] val transportEnabledMessage =
    if (!rapidsConf.isUCXShuffleManagerMode) {
      if (rapidsConf.isCacheOnlyShuffleManagerMode) {
        "Transport disabled (local cached blocks only)"
      } else {
        val numWriteThreads = rapidsConf.shuffleMultiThreadedWriterThreads
        val numReadThreads = rapidsConf.shuffleMultiThreadedReaderThreads
        s"Multi-threaded shuffle mode " +
          s"(write threads=$numWriteThreads, read threads=$numReadThreads)"
      }
    } else {
      s"Transport enabled (remote fetches will use ${rapidsConf.shuffleTransportClassName}"
    }

  logWarning(s"Rapids Shuffle Plugin enabled. ${transportEnabledMessage}. To disable the " +
    s"RAPIDS Shuffle Manager set `${RapidsConf.SHUFFLE_MANAGER_ENABLED}` to false")

  //Many of these values like blockManager are not initialized when the constructor is called,
  // so they all need to be lazy values that are executed when things are first called

  // NOTE: this can be null in the driver side.
  protected lazy val env = SparkEnv.get
  protected lazy val blockManager = env.blockManager
  protected lazy val shouldFallThroughOnEverything = {
    val fallThroughReasons = new ListBuffer[String]()
    if (!rapidsConf.isMultiThreadedShuffleManagerMode) {
      if (GpuShuffleEnv.isExternalShuffleEnabled) {
        fallThroughReasons += "External Shuffle Service is enabled"
      }
      if (GpuShuffleEnv.isSparkAuthenticateEnabled) {
        fallThroughReasons += "Spark authentication is enabled"
      }
    }
    if (rapidsConf.isSqlExplainOnlyEnabled) {
      fallThroughReasons += "Plugin is in explain only mode"
    }
    if (GpuShuffleEnv.isRowBasedChecksumEnabled) {
      fallThroughReasons += "Detected order-independent checksum enabled " +
        "(spark.sql.shuffle.orderIndependentChecksum.enabled or " +
        "enableFullRetryOnMismatch). " +
        "This Spark 4.1+ feature is not yet supported by Spark-Rapids."
    }
    if (fallThroughReasons.nonEmpty) {
      logWarning(s"Rapids Shuffle Plugin is falling back to SortShuffleManager " +
        s"because: ${fallThroughReasons.mkString(", ")}")
    }
    fallThroughReasons.nonEmpty
  }

  private lazy val localBlockManagerId = blockManager.blockManagerId

  // Used to prevent stopping multiple times RAPIDS Shuffle Manager internals.
  // see the `stop` method
  private var stopped: Boolean = false

  // Code that expects the shuffle catalog to be initialized gets it this way,
  // with error checking in case we are in a bad state.
  protected def getCatalogOrThrow: ShuffleBufferCatalog =
    Option(GpuShuffleEnv.getCatalog).getOrElse(
      throw new IllegalStateException("The ShuffleBufferCatalog is not initialized but the " +
        "RapidsShuffleManager is configured"))

  protected lazy val resolver =
    if (shouldFallThroughOnEverything) {
      wrapped.shuffleBlockResolver
    } else if (rapidsConf.isMultiThreadedShuffleManagerMode) {
      // MULTITHREADED mode: use GpuShuffleBlockResolver
      // mtCatalog will be fetched dynamically in getBlockData() since it may not be
      // initialized yet when this resolver is created
      new GpuShuffleBlockResolver(
        wrapped.shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
        null) // No UCX catalog in MULTITHREADED mode
    } else { // we didn't fallback && we are using the UCX shuffle
      val catalog = GpuShuffleEnv.getCatalog
      if (catalog == null) {
        if (isDriver) {
          // this is an OK state to be in. It means we didn't fall back
          // (`shouldFallbackThroughOnEverything` is false) and this is just the driver
          // in a job with RapidsShuffleManager enabled. We want to just use the regular
          // shuffle block resolver here, since we don't do anything on the driver.
          wrapped.shuffleBlockResolver
        } else {
          // this would be bad: if we are an executor, didn't fallback, and RapidsShuffleManager
          // is enabled, we need to fail.
          throw new IllegalStateException(
            "An executor with RapidsShuffleManager is trying to use a ShuffleBufferCatalog " +
              "that isn't initialized."
          )
        }
      } else {
        // A driver in local mode with the RapidsShuffleManager enabled would go through this
        // else statement, because the "executor" is the driver, and isDriver=true, or
        // The regular case where the executor has RapidsShuffleManager enabled.
        // What these cases have in common is that `catalog` is defined.
        new GpuShuffleBlockResolver(wrapped.shuffleBlockResolver, catalog)
      }
    }

  private[this] lazy val transport: Option[RapidsShuffleTransport] = {
    if (rapidsConf.isUCXShuffleManagerMode && !isDriver) {
      Some(RapidsShuffleTransport.makeTransport(blockManager.shuffleServerId, rapidsConf))
    } else {
      None
    }
  }

  private[this] lazy val server: Option[RapidsShuffleServer] = {
    if (rapidsConf.isGPUShuffle && !isDriver) {
      val catalog = getCatalogOrThrow
      val requestHandler = new RapidsShuffleRequestHandler() {
        override def getShuffleHandle(tableId: Int): RapidsShuffleHandle = {
          catalog.getShuffleBufferHandle(tableId)
        }

        override def getShuffleBufferMetas(sbbId: ShuffleBlockBatchId): Seq[TableMeta] = {
          (sbbId.startReduceId to sbbId.endReduceId).flatMap(rid => {
            catalog.blockIdToMetas(ShuffleBlockId(sbbId.shuffleId, sbbId.mapId, rid))
          })
        }
      }
      val server = transport.get.makeServer(requestHandler)
      server.start()
      Some(server)
    } else {
      None
    }
  }

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    // Always register with the wrapped handler so we can write to it ourselves if needed
    val orig = wrapped.registerShuffle(shuffleId, dependency)

    dependency match {
      case _ if shouldFallThroughOnEverything ||
        rapidsConf.isMultiThreadedShuffleManagerMode => orig
      case gpuDependency: GpuShuffleDependency[K, V, C] if gpuDependency.useGPUShuffle =>
        new GpuShuffleHandle(orig,
          dependency.asInstanceOf[GpuShuffleDependency[K, V, V]])
      case _ => orig
    }
  }

  lazy val execComponents: Option[ShuffleExecutorComponents] = {
    // Check if user configured a different ShuffleDataIO plugin
    val configuredPlugin = conf.get("spark.shuffle.sort.io.plugin.class", "")
    val rapidsPlugin = "org.apache.spark.shuffle.sort.io.RapidsLocalDiskShuffleDataIO"

    if (configuredPlugin.nonEmpty && !configuredPlugin.endsWith("RapidsLocalDiskShuffleDataIO")) {
      throw new IllegalArgumentException(
        s"RapidsShuffleManager requires 'spark.shuffle.sort.io.plugin.class' to be " +
          s"'$rapidsPlugin' or unset, but found '$configuredPlugin'. " +
          s"Please update your configuration.")
    }

    val rapidsDataIO = new RapidsLocalDiskShuffleDataIO(conf)
    val executorComponents = rapidsDataIO.executor()

    val extraConfigs = conf.getAllWithPrefix(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX).toMap
    executorComponents.initializeExecutor(
      conf.getAppId,
      SparkEnv.get.executorId,
      extraConfigs.asJava)
    Some(executorComponents)
  }

  /**
   * A mapping from shuffle ids to the task ids of mappers producing output for those shuffles.
   */
  protected val taskIdMapsForShuffle = new ConcurrentHashMap[Int, OpenHashSet[Long]]()

  private def trackMapTaskForCleanup(shuffleId: Int, mapId: Long): Unit = {
    // this uses OpenHashSet as it is copied from Spark
    val mapTaskIds = taskIdMapsForShuffle.computeIfAbsent(
      shuffleId, _ => new OpenHashSet[Long](16))
    mapTaskIds.synchronized {
      mapTaskIds.add(mapId)
    }
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metricsReporter: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    handle match {
      case gpu: GpuShuffleHandle[_, _] =>
        registerGpuShuffle(handle.shuffleId)
        new RapidsCachingWriter(
          env.blockManager,
          gpu.asInstanceOf[GpuShuffleHandle[K, V]],
          mapId,
          metricsReporter,
          getCatalogOrThrow,
          server,
          gpu.dependency.metrics)
      case handle: BaseShuffleHandle[_, _, _] =>
        handle.dependency match {
          case gpuDep: GpuShuffleDependency[_, _, _]
            if gpuDep.useMultiThreadedShuffle &&
              rapidsConf.shuffleMultiThreadedWriterThreads > 0 =>
            // use the threaded writer if the number of threads specified is 1 or above,
            // with 0 threads we fallback to the Spark-provided writer.
            // Register shuffle with MultithreadedShuffleBufferCatalog
            registerGpuShuffle(handle.shuffleId)
            val handleWithMetrics = new ShuffleHandleWithMetrics(
              handle.shuffleId,
              gpuDep.metrics,
              // cast the handle with specific generic types due to type-erasure
              gpuDep.asInstanceOf[GpuShuffleDependency[K, V, V]])
            // we need to track this mapId so we can clean it up later on unregisterShuffle
            trackMapTaskForCleanup(handle.shuffleId, context.taskAttemptId())
            // in most scenarios, the pools have already started, except for local mode
            // here we try to start them if we see they haven't
            RapidsShuffleInternalManagerBase.startThreadPoolIfNeeded(
              rapidsConf.shuffleMultiThreadedWriterThreads,
              rapidsConf.shuffleMultiThreadedReaderThreads)
            new RapidsShuffleThreadedWriter[K, V](
              blockManager,
              handleWithMetrics,
              mapId,
              conf,
              new ThreadSafeShuffleWriteMetricsReporter(metricsReporter),
              rapidsConf.shuffleMultiThreadedMaxBytesInFlight,
              execComponents.get,
              rapidsConf.shuffleMultiThreadedWriterThreads)
          case _ =>
            wrapped.getWriter(handle, mapId, context, metricsReporter)
        }
      case _ =>
        wrapped.getWriter(handle, mapId, context, metricsReporter)
    }
  }

  def getReaderImpl[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    handle match {
      case gpuHandle: GpuShuffleHandle[_, _] =>
        logInfo(s"Asking map output tracker for dependency ${gpuHandle.dependency}, " +
          s"map output sizes for: ${gpuHandle.shuffleId}, parts=$startPartition-$endPartition")
        if (gpuHandle.dependency.keyOrdering.isDefined) {
          // very unlikely, but just in case
          throw new IllegalStateException("A key ordering was requested for a gpu shuffle "
            + s"dependency ${gpuHandle.dependency.keyOrdering.get}, this is not supported.")
        }

        val blocksByAddress = NvtxRegistry.GET_MAP_SIZES_BY_EXEC_ID {
          SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(gpuHandle.shuffleId,
            startMapIndex, endMapIndex, startPartition, endPartition)
        }

        new RapidsCachingReader(rapidsConf, localBlockManagerId,
          blocksByAddress,
          context,
          metrics,
          transport,
          getCatalogOrThrow,
          gpuHandle.dependency.sparkTypes)
      case other: ShuffleHandle if
        rapidsConf.isMultiThreadedShuffleManagerMode
          && rapidsConf.shuffleMultiThreadedReaderThreads > 0 =>
        // we enable a multi-threaded reader in the case where we have 1 or
        // more threads and we have enbled the MULTITHREADED shuffle mode.
        // We special case the threads=1 case in the reader to behave like regular
        // spark, but this allows us to add extra metrics that Spark normally
        // doesn't look at while materializing blocks.
        val baseHandle = other.asInstanceOf[BaseShuffleHandle[K, C, C]]

        // we check that the dependency is a `GpuShuffleDependency` and if not
        // we go back to the regular path (e.g. is a GpuColumnarExchange?)
        // TODO: it may make sense to expand this code (and the writer code) to include
        //   regular Exchange nodes. For now this is being conservative and a few changes
        //   would need to be made to deal with missing metrics, for example, for a regular
        //   Exchange node.
        baseHandle.dependency match {
          case gpuDep: GpuShuffleDependency[K, C, C] if gpuDep.useMultiThreadedShuffle =>
            // We want to use batch fetch in the non-push shuffle case. Spark
            // checks for a config to see if batch fetch is enabled (this check), and
            // it also checks when getting (potentially merged) map status from
            // the MapOutputTracker.
            val canUseBatchFetch =
              SortShuffleManager.canUseBatchFetch(startPartition, endPartition, context)

            val shuffleHandleWithMetrics = new ShuffleHandleWithMetrics(
              baseHandle.shuffleId, gpuDep.metrics, gpuDep)
            // in most scenarios, the pools have already started, except for local mode
            // here we try to start them if we see they haven't
            RapidsShuffleInternalManagerBase.startThreadPoolIfNeeded(
              rapidsConf.shuffleMultiThreadedWriterThreads,
              rapidsConf.shuffleMultiThreadedReaderThreads)
            new RapidsShuffleThreadedReader(
              startMapIndex,
              endMapIndex,
              startPartition,
              endPartition,
              shuffleHandleWithMetrics,
              context,
              metrics,
              rapidsConf.shuffleMultiThreadedMaxBytesInFlight,
              canUseBatchFetch = canUseBatchFetch,
              numReaderThreads = rapidsConf.shuffleMultiThreadedReaderThreads,
              readerTaskAdmissionConfig = {
                val adaptive = rapidsConf.shuffleMultiThreadedReaderAdaptiveAdmissionEnabled
                val fixed = rapidsConf.shuffleMultiThreadedReaderMaxConcurrentTasks
                if (!adaptive && fixed == 0) {
                  None
                } else {
                  val initial = if (adaptive) {
                    rapidsConf.shuffleMultiThreadedReaderAdaptiveInitialConcurrentTasks
                  } else {
                    fixed
                  }
                  Some(ReaderTaskAdmissionConfig(
                    initialConcurrentTasks = initial,
                    adaptiveEnabled = adaptive,
                    minConcurrentTasks = if (adaptive) {
                      rapidsConf.shuffleMultiThreadedReaderAdaptiveMinConcurrentTasks
                    } else initial,
                    maxConcurrentTasks = if (adaptive) {
                      rapidsConf.shuffleMultiThreadedReaderAdaptiveMaxConcurrentTasks
                    } else initial,
                    gpuConcurrencyMultiplier =
                      rapidsConf.shuffleMultiThreadedReaderAdaptiveGpuConcurrencyMultiplier,
                    decisionWindowTasks =
                      rapidsConf.shuffleMultiThreadedReaderAdaptiveDecisionWindowTasks,
                    stableTargetWindows =
                      rapidsConf.shuffleMultiThreadedReaderAdaptiveStableTargetWindows,
                    maxAdjustmentStep =
                      rapidsConf.shuffleMultiThreadedReaderAdaptiveMaxAdjustmentStep,
                    detailedLoggingEnabled =
                      rapidsConf.shuffleMultiThreadedReaderAdaptiveDetailedLoggingEnabled,
                    immediateDecreaseEnabled =
                      rapidsConf.shuffleMultiThreadedReaderAdaptiveImmediateDecreaseEnabled,
                    stageBoundaryDecreaseEnabled =
                      rapidsConf.shuffleMultiThreadedReaderAdaptiveStageBoundaryDecreaseEnabled))
                }
              })
          case _ =>
            val shuffleHandle = RapidsShuffleInternalManagerBase.unwrapHandle(other)
            ShuffleManagerShims.getReader(wrapped, shuffleHandle, startMapIndex, endMapIndex,
              startPartition, endPartition, context, metrics)
        }
      case other =>
        val shuffleHandle = RapidsShuffleInternalManagerBase.unwrapHandle(other)
        ShuffleManagerShims.getReader(wrapped, shuffleHandle, startMapIndex, endMapIndex,
          startPartition, endPartition, context, metrics)
    }
  }

  def registerGpuShuffle(shuffleId: Int): Unit = {
    val catalog = GpuShuffleEnv.getCatalog
    if (catalog != null) {
      // Note that in local mode this can be called multiple times.
      logInfo(s"Registering shuffle $shuffleId")
      catalog.registerShuffle(shuffleId)
    }
    // Also register with MultithreadedShuffleBufferCatalog if available
    GpuShuffleEnv.getMultithreadedCatalog.foreach { mtCatalog =>
      logInfo(s"Registering shuffle $shuffleId with multithreaded catalog")
      mtCatalog.registerShuffle(shuffleId)
    }
  }

  def unregisterGpuShuffle(shuffleId: Int): Unit = {
    val catalog = GpuShuffleEnv.getCatalog
    if (catalog != null) {
      logInfo(s"Unregistering shuffle $shuffleId from shuffle buffer catalog")
      catalog.unregisterShuffle(shuffleId)
    }
    // For MultithreadedShuffleBufferCatalog:
    // Cleanup is triggered by ShuffleCleanupListener on job end, not here.
    // The ShuffleCleanupEndpoint polls the driver for shuffles to clean and calls
    // mtCatalog.unregisterShuffle on executors.
    //
    // Note: This method is called via GC-triggered ContextCleaner.doCleanupShuffle().
    // We do not register for cleanup here because:
    // 1. GC timing is unpredictable and often happens too late (at app shutdown)
    // 2. By that time, executors may already be shutting down
    // 3. ShuffleCleanupListener triggers cleanup proactively on job end
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    unregisterGpuShuffle(shuffleId)
    // We need to remove old shuffle blocks when Spark GC's a shuffle Id upstream.
    // In order to do so, we need to find the IndexShuffleBlockResolver in use.
    // We have two scenarios:
    // 1) We could be running in some compatibility mode where IndexShuffleBlockResolver
    //    (which comes from Spark) is the resolver we are using.
    // 2) We are using our own GpuShuffleBlockResolver, which can keep data in its own
    //    internal catalog, and it will also use the block manager to write map output
    //    to disk.
    val isbr = shuffleBlockResolver match {
      case isbr: IndexShuffleBlockResolver => isbr
      case gpur: GpuShuffleBlockResolverBase => gpur.wrapped
      case _ =>
        throw new IllegalStateException(
          "unregisterShuffle called with unexpected resolver " +
            s"$shuffleBlockResolver and blocks left to be cleaned")
    }
    Option(taskIdMapsForShuffle.remove(shuffleId)).foreach { mapTaskIds =>
      mapTaskIds.synchronized {
        mapTaskIds.iterator.foreach { mapTaskId =>
          isbr.removeDataByMap(shuffleId, mapTaskId)
        }
      }
    }
    wrapped.unregisterShuffle(shuffleId)
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = resolver

  override def stop(): Unit = synchronized {
    wrapped.stop()
    if (!stopped) {
      stopped = true
      server.foreach(_.close())
      transport.foreach(_.close())
      if (rapidsConf.isMultiThreadedShuffleManagerMode) {
        RapidsShuffleInternalManagerBase.stopThreadPool()
      }
    }
  }
}
