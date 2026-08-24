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

import java.lang.management.{ManagementFactory, ThreadInfo}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.internal.Logging

/**
 * Samples the executor task threads admitted in a short window after the first task starts.
 *
 * The sampler is diagnostic-only and disabled by default. The executor plugin callback runs
 * before Spark materializes the task-binary broadcast and deserializes the RDD closure, so the
 * samples cover both that work and the beginning of the production input iterator. Threads are
 * removed when their original task completes and are not re-added after the registration window.
 */
private[rapids] object FirstTaskStackSampler extends Logging {
  val ENABLED_KEY = "spark.rapids.executor.firstTaskStackSampler.enabled"
  val DURATION_MS_KEY = "spark.rapids.executor.firstTaskStackSampler.durationMs"
  val INTERVAL_MS_KEY = "spark.rapids.executor.firstTaskStackSampler.intervalMs"
  val REGISTRATION_WINDOW_MS_KEY =
    "spark.rapids.executor.firstTaskStackSampler.registrationWindowMs"
  val MAX_DEPTH_KEY = "spark.rapids.executor.firstTaskStackSampler.maxDepth"
  val TOP_STACK_COUNT_KEY = "spark.rapids.executor.firstTaskStackSampler.topStackCount"
  val MAX_TASKS_KEY = "spark.rapids.executor.firstTaskStackSampler.maxTasks"

  private val DefaultDurationMs = 12000L
  private val DefaultIntervalMs = 20L
  private val DefaultRegistrationWindowMs = 500L
  private val DefaultMaxDepth = 32
  private val DefaultTopStackCount = 20
  private val DefaultMaxTasks = 64
  private val BucketMs = 500L

  private[rapids] case class Settings(
      durationMs: Long,
      intervalMs: Long,
      registrationWindowMs: Long,
      maxDepth: Int,
      topStackCount: Int,
      maxTasks: Int)

  private case class TaskMetadata(stageId: Int, taskAttemptId: Long)

  private[rapids] def create(sparkConf: SparkConf, executorId: String): Option[Sampler] = {
    if (sparkConf.getBoolean(ENABLED_KEY, false)) {
      Some(new Sampler(parseSettings(sparkConf), executorId))
    } else {
      None
    }
  }

  private[rapids] def parseSettings(conf: SparkConf): Settings = {
    val settings = Settings(
      conf.getLong(DURATION_MS_KEY, DefaultDurationMs),
      conf.getLong(INTERVAL_MS_KEY, DefaultIntervalMs),
      conf.getLong(REGISTRATION_WINDOW_MS_KEY, DefaultRegistrationWindowMs),
      conf.getInt(MAX_DEPTH_KEY, DefaultMaxDepth),
      conf.getInt(TOP_STACK_COUNT_KEY, DefaultTopStackCount),
      conf.getInt(MAX_TASKS_KEY, DefaultMaxTasks))
    require(settings.durationMs >= 100L && settings.durationMs <= 60000L,
      s"$DURATION_MS_KEY must be between 100 and 60000")
    require(settings.intervalMs >= 5L && settings.intervalMs <= 1000L,
      s"$INTERVAL_MS_KEY must be between 5 and 1000")
    require(settings.registrationWindowMs >= 0L && settings.registrationWindowMs <= 5000L,
      s"$REGISTRATION_WINDOW_MS_KEY must be between 0 and 5000")
    require(settings.maxDepth >= 1 && settings.maxDepth <= 128,
      s"$MAX_DEPTH_KEY must be between 1 and 128")
    require(settings.topStackCount >= 1 && settings.topStackCount <= 100,
      s"$TOP_STACK_COUNT_KEY must be between 1 and 100")
    require(settings.maxTasks >= 1 && settings.maxTasks <= 1024,
      s"$MAX_TASKS_KEY must be between 1 and 1024")
    settings
  }

  private[rapids] final class Sampler(settings: Settings, executorId: String) {
    private val activeTasks = new ConcurrentHashMap[Long, TaskMetadata]()
    private val registeredTaskMetadata = new ConcurrentHashMap[Long, TaskMetadata]()
    private val started = new AtomicBoolean(false)
    private val stopped = new AtomicBoolean(false)
    private val done = new CountDownLatch(1)
    private val registeredTasks = new AtomicInteger(0)
    private val peakActiveTasks = new AtomicInteger(0)
    @volatile private var firstTaskStartNanos = 0L
    @volatile private var samplerThread: Thread = null

    def onTaskStart(taskContext: TaskContext): Unit = synchronized {
      if (!stopped.get()) {
        val now = System.nanoTime()
        if (firstTaskStartNanos == 0L) {
          firstTaskStartNanos = now
        }
        val registrationElapsedMs = elapsedMs(firstTaskStartNanos, now)
        if (registrationElapsedMs <= settings.registrationWindowMs &&
            activeTasks.size() < settings.maxTasks) {
          val threadId = Thread.currentThread().getId
          val taskMetadata = TaskMetadata(taskContext.stageId(), taskContext.taskAttemptId())
          val previous = activeTasks.putIfAbsent(threadId, taskMetadata)
          if (previous == null) {
            registeredTaskMetadata.put(threadId, taskMetadata)
            registeredTasks.incrementAndGet()
            updatePeak(activeTasks.size())
          }
        }
        if (started.compareAndSet(false, true)) {
          samplerThread = new Thread(() => sample(),
            s"rapids-first-task-stack-sampler-${metricValue(executorId)}")
          samplerThread.setDaemon(true)
          samplerThread.start()
          logInfo(s"RAPIDS_FIRST_TASK_STACK_METRIC event=started " +
            s"executor_id=${metricValue(executorId)} duration_ms=${settings.durationMs} " +
            s"interval_ms=${settings.intervalMs} " +
            s"registration_window_ms=${settings.registrationWindowMs} " +
            s"max_depth=${settings.maxDepth} max_tasks=${settings.maxTasks}")
        }
      }
    }

    def onTaskEnd(): Unit = activeTasks.remove(Thread.currentThread().getId)

    def shutdown(): Unit = {
      stopped.set(true)
      Option(samplerThread).foreach(_.interrupt())
    }

    private[rapids] def await(timeoutMs: Long): Boolean =
      done.await(timeoutMs, TimeUnit.MILLISECONDS)

    private def sample(): Unit = {
      val stateCounts = mutable.HashMap.empty[String, Long].withDefaultValue(0L)
      val categoryCounts = mutable.HashMap.empty[String, Long].withDefaultValue(0L)
      val bucketCategoryCounts = mutable.HashMap.empty[(Long, String), Long].withDefaultValue(0L)
      val stackCounts = mutable.HashMap.empty[(String, String), Long].withDefaultValue(0L)
      var ticks = 0L
      var threadSamples = 0L
      var status = "completed"
      try {
        val bean = ManagementFactory.getThreadMXBean
        val deadlineNanos = firstTaskStartNanos + TimeUnit.MILLISECONDS.toNanos(settings.durationMs)
        while (!stopped.get() && System.nanoTime() < deadlineNanos) {
          val sampleStartNanos = System.nanoTime()
          val threadIds = activeTasks.keySet().asScala.map(_.longValue()).toArray
          val infos = if (threadIds.nonEmpty) {
            bean.getThreadInfo(threadIds, settings.maxDepth)
          } else {
            Array.empty[ThreadInfo]
          }
          val bucketStartMs =
            (elapsedMs(firstTaskStartNanos, sampleStartNanos) / BucketMs) * BucketMs
          infos.filter(_ != null).foreach { info =>
            val state = info.getThreadState.name()
            val stack = collapseStack(info.getStackTrace)
            val category = classify(info.getStackTrace)
            stateCounts(state) += 1L
            categoryCounts(category) += 1L
            bucketCategoryCounts((bucketStartMs, category)) += 1L
            stackCounts((state, stack)) += 1L
            threadSamples += 1L
          }
          ticks += 1L
          val sampleElapsedMs = elapsedMs(sampleStartNanos, System.nanoTime())
          val sleepMs = math.max(1L, settings.intervalMs - sampleElapsedMs)
          Thread.sleep(sleepMs)
        }
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          status = "cancelled"
        case NonFatal(e) =>
          status = "failed"
          logWarning("First-task stack sampler failed", e)
      } finally {
        logResults(status, ticks, threadSamples, stateCounts.toMap, categoryCounts.toMap,
          bucketCategoryCounts.toMap, stackCounts.toMap)
        done.countDown()
      }
    }

    private def logResults(
        status: String,
        ticks: Long,
        threadSamples: Long,
        stateCounts: Map[String, Long],
        categoryCounts: Map[String, Long],
        bucketCategoryCounts: Map[(Long, String), Long],
        stackCounts: Map[(String, String), Long]): Unit = {
      val safeExecutorId = metricValue(executorId)
      val metadata = registeredTaskMetadata.values().asScala.toSeq
      val stageIds = metadata.map(_.stageId).distinct.sorted.mkString(",")
      val taskAttemptIds = metadata.map(_.taskAttemptId)
      val minTaskAttemptId = taskAttemptIds.reduceOption(_ min _).map(_.toString).getOrElse("none")
      val maxTaskAttemptId = taskAttemptIds.reduceOption(_ max _).map(_.toString).getOrElse("none")
      logInfo(s"RAPIDS_FIRST_TASK_STACK_METRIC event=completed status=$status " +
        s"executor_id=$safeExecutorId registered_tasks=${registeredTasks.get()} " +
        s"peak_active_tasks=${peakActiveTasks.get()} ticks=$ticks thread_samples=$threadSamples " +
        s"elapsed_ms=${elapsedMs(firstTaskStartNanos, System.nanoTime())} " +
        s"stage_ids=$stageIds min_task_attempt_id=$minTaskAttemptId " +
        s"max_task_attempt_id=$maxTaskAttemptId")
      stateCounts.toSeq.sortBy { case (_, count) => -count }.foreach { case (state, count) =>
        logInfo(s"RAPIDS_FIRST_TASK_STACK_METRIC event=state executor_id=$safeExecutorId " +
          s"state=$state samples=$count")
      }
      categoryCounts.toSeq.sortBy { case (_, count) => -count }.foreach {
        case (category, count) =>
          logInfo(s"RAPIDS_FIRST_TASK_STACK_METRIC event=category executor_id=$safeExecutorId " +
            s"category=$category samples=$count")
      }
      bucketCategoryCounts.toSeq.sortBy { case ((bucket, category), _) => (bucket, category) }
          .foreach { case ((bucket, category), count) =>
            logInfo(s"RAPIDS_FIRST_TASK_STACK_METRIC event=bucket_category " +
              s"executor_id=$safeExecutorId bucket_start_ms=$bucket category=$category " +
              s"samples=$count")
          }
      stackCounts.toSeq.sortBy { case (_, count) => -count }.take(settings.topStackCount)
          .zipWithIndex.foreach { case (((state, stack), count), index) =>
            logInfo(s"RAPIDS_FIRST_TASK_STACK_METRIC event=stack executor_id=$safeExecutorId " +
              s"rank=${index + 1} state=$state samples=$count stack=$stack")
          }
    }

    private def updatePeak(candidate: Int): Unit = {
      var current = peakActiveTasks.get()
      while (candidate > current && !peakActiveTasks.compareAndSet(current, candidate)) {
        current = peakActiveTasks.get()
      }
    }
  }

  private[rapids] def classify(stack: Array[StackTraceElement]): String = {
    val frames = stack.iterator.map(frame => s"${frame.getClassName}.${frame.getMethodName}").toSeq
    if (frames.exists(_.contains("GpuSemaphore"))) {
      "gpu_semaphore"
    } else if (frames.exists(frame => frame.contains("TorrentBroadcast") ||
        frame.contains("BroadcastBlockId") || frame.contains("BlockManager"))) {
      "broadcast_block_manager"
    } else if (frames.exists(frame => frame.contains("ClassLoader") ||
        frame.contains("JarFile") || frame.contains("ZipFile"))) {
      "class_loading"
    } else if (frames.exists(frame => frame.contains("Kryo") ||
        frame.contains("Serializer") || frame.contains("ObjectInputStream"))) {
      "serializer"
    } else if (frames.exists(frame => frame.contains("FutureTask.get") ||
        frame.contains("CompletableFuture") || frame.contains("CountDownLatch") ||
        frame.contains("ForkJoinTask.awaitDone"))) {
      "async_wait"
    } else if (frames.exists(frame => frame.contains("GpuFileScanRDD") ||
        frame.contains("MultiFile") || frame.toLowerCase.contains("parquet") ||
        frame.startsWith("ai.rapids.cudf"))) {
      "production_scan"
    } else {
      "other"
    }
  }

  private[rapids] def collapseStack(stack: Array[StackTraceElement]): String = {
    if (stack.isEmpty) {
      "empty"
    } else {
      stack.iterator.map(frame => metricValue(s"${frame.getClassName}.${frame.getMethodName}"))
        .mkString(">")
    }
  }

  private def elapsedMs(startNanos: Long, endNanos: Long): Long =
    TimeUnit.NANOSECONDS.toMillis(endNanos - startNanos)

  private def metricValue(value: String): String =
    value.replaceAll("[^A-Za-z0-9._:/@+>-]", "_")
}
