/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import java.{lang => jl}
import java.io.ObjectInputStream
import java.util.Locale
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.mutable

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.jni.RmmSpark
import org.apache.spark.{SparkContext, TaskContext}

import org.apache.spark.internal.Logging
import org.apache.spark.util.{AccumulatorV2, LongAccumulator, Utils}

case class NanoTime(value: java.lang.Long) {
  override def toString: String = {
    val hours = TimeUnit.NANOSECONDS.toHours(value)
    var remaining = value - TimeUnit.HOURS.toNanos(hours)
    val minutes = TimeUnit.NANOSECONDS.toMinutes(remaining)
    remaining = remaining - TimeUnit.MINUTES.toNanos(minutes)
    val seconds = remaining.toDouble / TimeUnit.SECONDS.toNanos(1)
    val locale = Locale.US
    "%02d:%02d:%06.3f".formatLocal(locale, hours, minutes, seconds)
  }
}

class NanoSecondAccumulator extends AccumulatorV2[jl.Long, NanoTime] {
  private var _sum = 0L
  override def isZero: Boolean = _sum == 0


  override def copy(): NanoSecondAccumulator = {
    val newAcc = new NanoSecondAccumulator
    newAcc._sum = this._sum
    newAcc
  }

  override def reset(): Unit = {
    _sum = 0
  }

  override def add(v: jl.Long): Unit = {
    _sum += v
  }

  def add (v: Long): Unit = {
    _sum += v
  }

  override def merge(other: AccumulatorV2[jl.Long, NanoTime]): Unit = other match {
    case ns: NanoSecondAccumulator =>
      _sum += ns._sum
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: NanoTime = NanoTime(_sum)
}

class MaxLongAccumulator extends AccumulatorV2[jl.Long, jl.Long] {
  private var _v = 0L

  override def isZero: Boolean = _v == 0

  override def copy(): MaxLongAccumulator = {
    val newAcc = new MaxLongAccumulator
    newAcc._v = this._v
    newAcc
  }

  override def reset(): Unit = {
    _v = 0L
  }

  override def add(v: jl.Long): Unit = {
    if(v > _v) {
      _v = v
    }
  }

  def add(v: Long): Unit = {
    if(v > _v) {
      _v = v
    }
  }

  override def merge(other: AccumulatorV2[jl.Long, jl.Long]): Unit = other match {
    case o: MaxLongAccumulator =>
      add(o.value)
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: jl.Long = _v
}

class GpuTaskMetrics extends Serializable {
  private val semWaitTimeNs = new NanoSecondAccumulator
  private val retryCount = new LongAccumulator
  private val splitAndRetryCount = new LongAccumulator
  private val retryBlockTime = new NanoSecondAccumulator
  private val retryComputationTime = new NanoSecondAccumulator
  // onGpuTask means a task that has data in GPU memory.
  // Since it's not easy to decided if a task has data in GPU memory,
  // We only count the tasks that had held semaphore before,
  // so it's very likely to have data in GPU memory
  private val onGpuTasksRecordedTimes = new LongAccumulator
  private val onGpuTasksInWaitingQueueTotalCount = new LongAccumulator
  private val onGpuTasksInWaitingQueueMaxCount = new MaxLongAccumulator

  // Spill
  private val spillToHostTimeNs = new NanoSecondAccumulator
  private val spillToDiskTimeNs = new NanoSecondAccumulator
  private val readSpillFromHostTimeNs = new NanoSecondAccumulator
  private val readSpillFromDiskTimeNs = new NanoSecondAccumulator

  private val metrics = Map[String, AccumulatorV2[_, _]](
    "gpuSemaphoreWait" -> semWaitTimeNs,
    "gpuRetryCount" -> retryCount,
    "gpuSplitAndRetryCount" -> splitAndRetryCount,
    "gpuRetryBlockTime" -> retryBlockTime,
    "gpuRetryComputationTime" -> retryComputationTime,
    "gpuSpillToHostTime" -> spillToHostTimeNs,
    "gpuSpillToDiskTime" -> spillToDiskTimeNs,
    "gpuReadSpillFromHostTime" -> readSpillFromHostTimeNs,
    "gpuReadSpillFromDiskTime" -> readSpillFromDiskTimeNs,
    "gpuOnGpuTasksRecordedTimes" -> onGpuTasksRecordedTimes,
    "gpuOnGpuTasksObservedInWaitingQueueTotalCount" -> onGpuTasksInWaitingQueueTotalCount,
    "gpuOnGpuTasksObservedInWaitingQueueMaxCount" -> onGpuTasksInWaitingQueueMaxCount
  )

  def register(sc: SparkContext): Unit = {
    metrics.foreach { case (k, m) =>
      // This is not a public API, but the only way to get failed task
      // If we run into problems we can use use sc.register(m, k), but
      // it would not allow us to collect metrics for failed tasks.
      m.register(sc, Some(k), true)
    }
  }

  def makeSureRegistered(): Unit = {
    // This is a noop for now, but need to make sure something happens
  }

  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    // Now we need to make sure that we are registered with the proper task
    GpuTaskMetrics.registerOnTask(this)
  }

  private def timeIt[A](timer: NanoSecondAccumulator,
      range: String,
      color: NvtxColor,
      f: => A): A = {
    val start = System.nanoTime()
    withResource(new NvtxRange(range, color)) { _ =>
      try {
        f
      } finally {
        timer.add(System.nanoTime() - start)
      }
    }
  }

  def getSemWaitTime(): Long = semWaitTimeNs.value.value

  def semWaitTime[A](f: => A): A = timeIt(semWaitTimeNs, "Acquire GPU", NvtxColor.RED, f)

  def spillToHostTime[A](f: => A): A = {
    timeIt(spillToHostTimeNs, "spillToHostTime", NvtxColor.RED, f)
  }

  def spillToDiskTime[A](f: => A): A = {
    timeIt(spillToDiskTimeNs, "spillToDiskTime", NvtxColor.RED, f)
  }

  def readSpillFromHostTime[A](f: => A): A = {
    timeIt(readSpillFromHostTimeNs, "readSpillFromHostTime", NvtxColor.ORANGE, f)
  }

  def readSpillFromDiskTime[A](f: => A): A = {
    timeIt(readSpillFromDiskTimeNs, "readSpillFromDiskTime", NvtxColor.ORANGE, f)
  }

  def updateRetry(taskAttemptId: Long): Unit = {
    val rc = RmmSpark.getAndResetNumRetryThrow(taskAttemptId)
    if (rc > 0) {
      retryCount.add(rc)
    }

    val src = RmmSpark.getAndResetNumSplitRetryThrow(taskAttemptId)
    if (src > 0) {
      splitAndRetryCount.add(src)
    }

    val timeNs = RmmSpark.getAndResetBlockTimeNs(taskAttemptId)
    if (timeNs > 0) {
      retryBlockTime.add(timeNs)
    }

    val compNs = RmmSpark.getAndResetComputeTimeLostToRetryNs(taskAttemptId)
    if (compNs > 0) {
      retryComputationTime.add(compNs)
    }
  }

  def recordOnGpuTasksNumber(num: Int): Unit = {
    onGpuTasksRecordedTimes.add(1)
    onGpuTasksInWaitingQueueTotalCount.add(num)
    onGpuTasksInWaitingQueueMaxCount.add(num)
  }
}

/**
 * Provides task level metrics
 */
object GpuTaskMetrics extends Logging {
  object DynamicGpuTaskMetricsSummary {
    val flowControlPermitTasks = ConcurrentHashMap.newKeySet[Long]()
    var flowControlMaxTasks = -1

    private var totalRunTimeMs = 0L
    private var totalSpillTimeMs = 0L
    private var lastStageId = -1L
    private var excludedTasks: Set[Long] = Set()

    def resetFlowControlIfNecessary(stageId: Long, defaultFlowControlMaxTasks: Int): Unit =
      synchronized {
        if (stageId != lastStageId) {
          lastStageId = stageId
          flowControlPermitTasks.clear()
          flowControlMaxTasks = defaultFlowControlMaxTasks
          excludeTasks(Set())
          resetStats()
        }
      }

    def excludeTasks(tasks: Set[Long]): Unit = synchronized {
      excludedTasks = tasks
    }

    def resetStats(): Unit = synchronized {
      totalRunTimeMs = 0L
      totalSpillTimeMs = 0L
    }

    def updateTime(taskId: Long, runTime: Long, spillTime: Long): Unit = synchronized {
      println(s"Updating time for task $taskId: runTime: $runTime spillTime: $spillTime")
      if (!excludedTasks.contains(taskId)) {
        this.totalRunTimeMs += runTime
        this.totalSpillTimeMs += spillTime
      }
    }

    def getSummary: (Long, Long) = synchronized {
      (totalRunTimeMs, totalSpillTimeMs)
    }
  }

  private val taskLevelMetrics = mutable.Map[Long, GpuTaskMetrics]()

  def registerOnTask(metrics: GpuTaskMetrics): Unit = synchronized {
    val tc = TaskContext.get()
    if (tc != null) {
      val id = tc.taskAttemptId()
      // avoid double registering the task metrics...
      if (!taskLevelMetrics.contains(id)) {
        taskLevelMetrics.put(id, metrics)
        val startTime = System.nanoTime()
        onTaskCompletion(tc, tc =>
          synchronized {
            taskLevelMetrics.remove(tc.taskAttemptId()).map(metrics => {
              DynamicGpuTaskMetricsSummary.updateTime(
                id,
                TimeUnit.NANOSECONDS.toMillis(
                  System.nanoTime() - startTime
                ),
                TimeUnit.NANOSECONDS.toMillis(
                  metrics.spillToHostTimeNs.value.value +
                    metrics.spillToDiskTimeNs.value.value +
                    metrics.readSpillFromHostTimeNs.value.value +
                    metrics.readSpillFromDiskTimeNs.value.value))

            })
          }
        )
      }
    }
  }

  def get: GpuTaskMetrics = synchronized {
    val tc = TaskContext.get()
    val metrics = if (tc != null) {
      taskLevelMetrics.get(tc.taskAttemptId())
    } else {
      None
    }
    // As a backstop better to not have metrics than to crash...
    // Spark does this too for regular task metrics
    metrics.getOrElse(new GpuTaskMetrics)
  }
}
