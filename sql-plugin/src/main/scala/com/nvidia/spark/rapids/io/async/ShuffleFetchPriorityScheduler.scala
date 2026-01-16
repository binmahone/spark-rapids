/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.io.async

import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.locks.{Condition, ReentrantLock}

import org.apache.spark.internal.Logging

/**
 * A global bytes-in-flight limiter for shuffle fetch requests that supports priority-based
 * scheduling.
 *
 * When multiple tasks are competing for shuffle fetch bandwidth, this limiter ensures that
 * tasks with higher priority (based on TaskOverridePriority) get their fetch requests
 * processed first.
 *
 * The global capacity is calculated as: maxBytesInFlightPerTask * maxConcurrentGpuTasks
 * This ensures the total capacity is not more restrictive than the original per-task design.
 */
object ShuffleFetchPriorityScheduler extends Logging {

  // Configuration
  @volatile private var enabled: Boolean = false
  @volatile private var globalMaxBytesInFlight: Long = 0L
  @volatile private var strategy: CloudReaderSchedulingStrategy.Value =
    CloudReaderSchedulingStrategy.DISABLED
  @volatile private var fuzzyTopPercentile: Int = 50

  // State
  private val lock = new ReentrantLock()
  private var currentBytesInFlight: Long = 0L
  private var initialized: Boolean = false

  // Waiting tasks queue with priority
  private case class WaitingTask(
      taskAttemptId: Long,
      requestedBytes: Long,
      priority: Long,
      condition: Condition,
      @volatile var signaled: Boolean = false)

  // Priority comparator: higher priority value = higher priority (should be dequeued first)
  private val priorityComparator = new java.util.Comparator[WaitingTask] {
    override def compare(t1: WaitingTask, t2: WaitingTask): Int = {
      strategy match {
        case CloudReaderSchedulingStrategy.STRICT =>
          // Higher priority value should come first (reverse order for PriorityBlockingQueue)
          java.lang.Long.compare(t2.priority, t1.priority)

        case CloudReaderSchedulingStrategy.FUZZY =>
          val isTop1 = TaskOverridePriority.isInTopPercentile(t1.taskAttemptId, fuzzyTopPercentile)
          val isTop2 = TaskOverridePriority.isInTopPercentile(t2.taskAttemptId, fuzzyTopPercentile)
          if (isTop1 && !isTop2) {
            -1 // t1 is in top tier, should come first
          } else if (!isTop1 && isTop2) {
            1 // t2 is in top tier, should come first
          } else {
            // Same tier, compare by priority
            java.lang.Long.compare(t2.priority, t1.priority)
          }

        case _ =>
          // Disabled: FIFO order, use task attempt id as tie breaker
          java.lang.Long.compare(t1.taskAttemptId, t2.taskAttemptId)
      }
    }
  }

  private val waitingQueue = new PriorityBlockingQueue[WaitingTask](100, priorityComparator)

  /**
   * Initialize the scheduler with configuration.
   * Should be called during executor startup.
   *
   * @param enableScheduling whether to enable priority scheduling
   * @param maxBytesPerTask the per-task maxBytesInFlight (spark.reducer.maxSizeInFlight)
   * @param maxConcurrentTasks the number of concurrent GPU tasks per executor
   * @param schedulingStrategy the scheduling strategy (STRICT, FUZZY, DISABLED)
   * @param topPercentile the percentile for fuzzy scheduling
   */
  def init(
      enableScheduling: Boolean,
      maxBytesPerTask: Long,
      maxConcurrentTasks: Int,
      schedulingStrategy: CloudReaderSchedulingStrategy.Value,
      topPercentile: Int): Unit = lock.synchronized {
    if (!initialized) {
      enabled = enableScheduling
      // Global capacity = per-task capacity * number of concurrent tasks
      globalMaxBytesInFlight = maxBytesPerTask * maxConcurrentTasks
      strategy = schedulingStrategy
      fuzzyTopPercentile = topPercentile
      initialized = true
      logInfo(s"ShuffleFetchPriorityScheduler initialized: enabled=$enabled, " +
          s"globalMaxBytesInFlight=${globalMaxBytesInFlight / (1024 * 1024)}MB " +
          s"($maxBytesPerTask bytes/task * $maxConcurrentTasks tasks), " +
          s"strategy=$strategy, fuzzyTopPercentile=$fuzzyTopPercentile")
    }
  }

  /**
   * Acquire bytes quota to send a shuffle fetch request.
   * Blocks until enough bytes are available, with priority-based ordering.
   *
   * @param taskAttemptId the task attempting to acquire
   * @param bytes the number of bytes to acquire
   * @return true if bytes were acquired (always true unless interrupted)
   */
  def acquireBytes(taskAttemptId: Long, bytes: Long): Boolean = {
    if (!enabled) {
      return true
    }

    lock.lock()
    try {
      // Check if we can acquire immediately
      if (currentBytesInFlight + bytes <= globalMaxBytesInFlight) {
        currentBytesInFlight += bytes
        logDebug(s"Task $taskAttemptId acquired $bytes bytes immediately, " +
            s"currentBytesInFlight=$currentBytesInFlight")
        return true
      }

      // Need to wait - create a waiting task with priority
      val priority = TaskOverridePriority.getEffectivePriority(taskAttemptId)
      val condition = lock.newCondition()
      val waitingTask = WaitingTask(taskAttemptId, bytes, priority, condition)
      waitingQueue.offer(waitingTask)

      logDebug(s"Task $taskAttemptId queued for $bytes bytes with priority=$priority, " +
          s"queueSize=${waitingQueue.size()}, currentBytesInFlight=$currentBytesInFlight")

      try {
        while (!waitingTask.signaled) {
          condition.await()
        }

        logDebug(s"Task $taskAttemptId acquired $bytes bytes after waiting, " +
            s"currentBytesInFlight=$currentBytesInFlight")
        true
      } catch {
        case _: InterruptedException =>
          waitingQueue.remove(waitingTask)
          Thread.currentThread().interrupt()
          false
      }
    } finally {
      lock.unlock()
    }
  }

  /**
   * Release bytes quota after a shuffle fetch completes.
   * Signals waiting tasks in priority order.
   *
   * @param bytes the number of bytes to release
   */
  def releaseBytes(bytes: Long): Unit = {
    if (!enabled) {
      return
    }

    lock.lock()
    try {
      currentBytesInFlight -= bytes
      logDebug(s"Released $bytes bytes, currentBytesInFlight=$currentBytesInFlight, " +
          s"waitingQueue.size=${waitingQueue.size()}")

      // Try to signal waiting tasks that can now proceed
      var signaled = true
      while (signaled && !waitingQueue.isEmpty) {
        val nextTask = waitingQueue.peek()
        if (nextTask != null &&
            currentBytesInFlight + nextTask.requestedBytes <= globalMaxBytesInFlight) {
          waitingQueue.poll()
          currentBytesInFlight += nextTask.requestedBytes
          nextTask.signaled = true
          nextTask.condition.signal()
          logDebug(s"Signaled task ${nextTask.taskAttemptId} for ${nextTask.requestedBytes} " +
              s"bytes with priority=${nextTask.priority}")
        } else {
          signaled = false
        }
      }
    } finally {
      lock.unlock()
    }
  }

  /**
   * Legacy method for compatibility - acquires a permit without byte tracking.
   * Use acquireBytes for proper byte-based flow control.
   */
  def acquirePermit(taskAttemptId: Long, timeoutMs: Long = 0): Boolean = {
    // For backward compatibility, treat as acquiring 0 bytes (just priority ordering)
    acquireBytes(taskAttemptId, 0)
  }

  /**
   * Legacy method for compatibility - releases a permit.
   * Use releaseBytes for proper byte-based flow control.
   */
  def releasePermit(): Unit = {
    releaseBytes(0)
  }

  /**
   * Get current statistics for monitoring.
   */
  def getStats: Map[String, Any] = lock.synchronized {
    Map(
      "enabled" -> enabled,
      "currentBytesInFlight" -> currentBytesInFlight,
      "globalMaxBytesInFlight" -> globalMaxBytesInFlight,
      "waitingQueueSize" -> waitingQueue.size(),
      "strategy" -> strategy.toString
    )
  }

  /**
   * Reset the scheduler state. Used for testing.
   */
  def reset(): Unit = lock.synchronized {
    waitingQueue.clear()
    currentBytesInFlight = 0
    initialized = false
    enabled = false
  }
}
