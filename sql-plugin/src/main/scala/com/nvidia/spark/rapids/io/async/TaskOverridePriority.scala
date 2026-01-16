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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.jni.TaskPriority

import org.apache.spark.internal.Logging

/**
 * Scheduling strategy for cloud reader thread pool.
 */
object CloudReaderSchedulingStrategy extends Enumeration {
  type CloudReaderSchedulingStrategy = Value

  /**
   * STRICT: Strictly follow priority order. Tasks with higher priority are always
   * scheduled before tasks with lower priority.
   */
  val STRICT: Value = Value("STRICT")

  /**
   * FUZZY: Divide active tasks into two tiers (top 50% and bottom 50%) based on
   * priority. Tasks in the top tier are scheduled before tasks in the bottom tier,
   * but within each tier, the order is not strictly enforced.
   */
  val FUZZY: Value = Value("FUZZY")

  /**
   * DISABLED: Disable override priority scheduling. Fall back to the original
   * TaskPriority-based scheduling.
   */
  val DISABLED: Value = Value("DISABLED")
}

/**
 * Manages override priority for tasks that access cloud storage (e.g., GCS).
 *
 * The override priority is based on the timestamp of the first cloud storage access.
 * Earlier access means higher priority (smaller timestamp = higher priority).
 *
 * Priority comparison rules:
 * 1. If both tasks have override priority, compare by timestamp (smaller wins)
 * 2. If only one task has override priority, it wins
 * 3. If neither has override priority, fall back to original TaskPriority comparison
 */
object TaskOverridePriority extends Logging {

  // Map from taskAttemptId to first cloud access timestamp
  // Smaller timestamp means earlier access, which means higher priority
  private val overridePriorities = new ConcurrentHashMap[Long, Long]()

  // Track all active tasks that have override priority for fuzzy scheduling
  private val activeTasks = new ConcurrentHashMap[Long, Long]()

  // Cache for percentile threshold, updated periodically
  private val percentileThresholdCache = new AtomicReference[Option[Long]](None)
  private val lastThresholdUpdateTime = new AtomicReference[Long](0L)
  private val THRESHOLD_UPDATE_INTERVAL_MS = 100L // Update threshold every 100ms

  /**
   * Record the first cloud storage access time for a task.
   * Only the first call for a given taskAttemptId will be recorded.
   *
   * @param taskAttemptId the task attempt ID
   * @return the recorded timestamp (either newly recorded or existing)
   */
  def recordFirstCloudAccess(taskAttemptId: Long): Long = {
    val timestamp = System.nanoTime()
    val existing = Option(overridePriorities.putIfAbsent(taskAttemptId, timestamp))
    existing match {
      case None =>
        activeTasks.put(taskAttemptId, timestamp)
        logDebug(s"Recorded first cloud access for task $taskAttemptId at $timestamp")
        timestamp
      case Some(ts) =>
        ts
    }
  }

  /**
   * Check if a task has override priority.
   */
  def hasOverridePriority(taskAttemptId: Long): Boolean = {
    overridePriorities.containsKey(taskAttemptId)
  }

  /**
   * Get the override priority timestamp for a task.
   *
   * @return Some(timestamp) if the task has override priority, None otherwise
   */
  def getOverridePriority(taskAttemptId: Long): Option[Long] = {
    Option(overridePriorities.get(taskAttemptId))
  }

  /**
   * Clean up override priority when a task completes.
   * Should be called when the task finishes.
   */
  def taskDone(taskAttemptId: Long): Unit = {
    overridePriorities.remove(taskAttemptId)
    activeTasks.remove(taskAttemptId)
    logDebug(s"Cleaned up override priority for task $taskAttemptId")
  }

  /**
   * Compare two tasks' priorities.
   *
   * @return negative if task1 has higher priority, positive if task2 has higher priority,
   *         0 if equal
   */
  def comparePriority(taskAttemptId1: Long, taskAttemptId2: Long): Int = {
    val override1 = getOverridePriority(taskAttemptId1)
    val override2 = getOverridePriority(taskAttemptId2)

    (override1, override2) match {
      // Both have override priority: smaller timestamp = higher priority
      case (Some(ts1), Some(ts2)) =>
        java.lang.Long.compare(ts1, ts2)

      // Only task1 has override priority: task1 wins
      case (Some(_), None) => -1

      // Only task2 has override priority: task2 wins
      case (None, Some(_)) => 1

      // Neither has override priority: fall back to original TaskPriority
      // Original: larger priority value = higher priority, so we reverse the comparison
      case (None, None) =>
        val p1 = TaskPriority.getTaskPriority(taskAttemptId1)
        val p2 = TaskPriority.getTaskPriority(taskAttemptId2)
        // Reverse because larger value means higher priority in original system
        java.lang.Long.compare(p2, p1)
    }
  }

  /**
   * Get the effective priority for scheduling in strict mode.
   * Returns a Long where LARGER value = HIGHER priority (consistent with original TaskPriority).
   *
   * Priority ordering (highest to lowest):
   * 1. Tasks with override priority, ordered by timestamp (earlier = higher priority)
   * 2. Tasks without override priority, ordered by original TaskPriority
   *
   * Implementation:
   * - Override priority values are in range [Long.MaxValue/2, Long.MaxValue]
   * - Non-override priority values are shifted to be in range [0, Long.MaxValue/2)
   */
  def getEffectivePriority(taskAttemptId: Long): Long = {
    getOverridePriority(taskAttemptId) match {
      case Some(timestamp) =>
        // Convert timestamp to priority: earlier = higher priority = larger value
        // We use a monotonically increasing counter based on recording order
        // to ensure consistent ordering regardless of timestamp wraparound
        val baseOffset = Long.MaxValue / 2
        // Earlier recorded tasks have smaller timestamps, should get higher priority
        // The first recorded task gets the highest value
        val maxTimestampSeen = activeTasks.values().asScala
            .reduceOption(_ max _).getOrElse(timestamp)
        baseOffset + (maxTimestampSeen - timestamp)
      case None =>
        // Shift original priority to lower half of the range
        // Original TaskPriority values are typically large (close to Long.MaxValue)
        // We divide by 2 to ensure they're smaller than override priorities
        val originalPriority = TaskPriority.getTaskPriority(taskAttemptId)
        // Map from [0, Long.MaxValue] to [0, Long.MaxValue/2)
        originalPriority / 2
    }
  }

  /**
   * Check if a task is in the top percentile based on fuzzy scheduling.
   * Used for the fuzzy priority strategy.
   *
   * @param taskAttemptId the task to check
   * @param topPercentile the percentile threshold (e.g., 50 for top 50%)
   * @return true if the task is in the top percentile, false otherwise
   */
  def isInTopPercentile(taskAttemptId: Long, topPercentile: Int = 50): Boolean = {
    val myPriority = getEffectivePriority(taskAttemptId)

    // Update threshold cache if needed
    val now = System.currentTimeMillis()
    val lastUpdate = lastThresholdUpdateTime.get()
    if (now - lastUpdate > THRESHOLD_UPDATE_INTERVAL_MS) {
      if (lastThresholdUpdateTime.compareAndSet(lastUpdate, now)) {
        updatePercentileThreshold(topPercentile)
      }
    }

    percentileThresholdCache.get() match {
      case Some(threshold) => myPriority <= threshold
      case None =>
        // If no threshold cached, consider all tasks as top priority
        true
    }
  }

  private def updatePercentileThreshold(topPercentile: Int): Unit = {
    import scala.collection.JavaConverters._

    val allPriorities = activeTasks.keys().asScala.map(getEffectivePriority).toArray.sorted
    if (allPriorities.isEmpty) {
      percentileThresholdCache.set(None)
    } else {
      val index = Math.max(0, (allPriorities.length * topPercentile / 100) - 1)
      percentileThresholdCache.set(Some(allPriorities(index)))
    }
  }

  /**
   * Get statistics for debugging/monitoring.
   */
  def getStats: Map[String, Any] = {
    Map(
      "totalTasksWithOverride" -> overridePriorities.size(),
      "activeTasks" -> activeTasks.size()
    )
  }
}
