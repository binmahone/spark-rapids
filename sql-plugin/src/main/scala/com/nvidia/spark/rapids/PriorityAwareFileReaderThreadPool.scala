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

package com.nvidia.spark.rapids

import java.util.Comparator
import java.util.concurrent._

import com.nvidia.spark.rapids.io.async.{AsyncResult, AsyncRunner, Cancelled, Init, RapidsFutureTask, Running, ScheduleFailed}

import org.apache.spark.internal.Logging
import org.apache.spark.TaskContext

/**
 * Priority-aware file reader thread pool for bandwidth-aware scheduling.
 * 
 * IMPORTANT: This is MUTUALLY EXCLUSIVE with ResourceBoundedThreadExecutor
 * (memory-bounded pool). They serve different purposes and should not be used together:
 * 
 * - PriorityAwareFileReaderThreadPool: Optimizes I/O bandwidth allocation based on
 *   task priority to reduce contention and improve high-priority task throughput.
 *   Best for scenarios where I/O bandwidth is the bottleneck.
 * 
 * - ResourceBoundedThreadExecutor: Limits concurrent file reading based on available
 *   host memory to prevent OOM. Best for memory-constrained scenarios.
 * 
 * If both are enabled, ResourceBoundedThreadExecutor takes precedence for safety.
 */

/**
 * Priority scheduling strategy for file reading.
 */
sealed trait PrioritySchedulingStrategy
case object NoneStrategy extends PrioritySchedulingStrategy
case object MedianStrategy extends PrioritySchedulingStrategy
case object StrictStrategy extends PrioritySchedulingStrategy

object PrioritySchedulingStrategy {
  def fromString(s: String): PrioritySchedulingStrategy = s.toUpperCase match {
    case "NONE" => NoneStrategy
    case "MEDIAN" => MedianStrategy
    case "STRICT" => StrictStrategy
    case _ => NoneStrategy
  }
}

/**
 * Comparator for RapidsFutureTask that implements bandwidth-aware scheduling.
 * Extends the standard priority-based ordering with MEDIAN strategy support.
 * 
 * Two strategies available:
 * 
 * 1. MEDIAN Strategy (default):
 *    - Classify tasks as HIGH or LOW based on median of all task priorities
 *    - Tasks with priority >= median are HIGH, others are LOW
 *    - Tasks already served once are always HIGH (avoid starvation)
 *    - Schedule order: HIGH > LOW (within class, order by priority)
 * 
 * 2. STRICT Strategy:
 *    - Strictly order by task priority value (same as RapidsFutureTaskComparator)
 *    - Higher priority value = schedule first
 *    - No special handling for already-served tasks
 * 
 * Rationale:
 * - Low-priority tasks that read data early but can't get GPU semaphore waste bandwidth
 * - These strategies defer low-priority I/O to prioritize high-priority tasks
 */
private class BandwidthAwareRapidsFutureTaskComparator[T](
    strategy: PrioritySchedulingStrategy,
    servedTasks: ConcurrentHashMap.KeySetView[Long, java.lang.Boolean])
  extends Comparator[Runnable] with Logging {
  
  // Metrics for performance analysis
  private val compareCount = new java.util.concurrent.atomic.AtomicLong(0)
  private val compareTotalNanos = new java.util.concurrent.atomic.AtomicLong(0)
  
  def getCompareCount: Long = compareCount.get()
  def getCompareTotalNanos: Long = compareTotalNanos.get()
  def getCompareAvgNanos: Double = {
    val count = compareCount.get()
    if (count > 0) compareTotalNanos.get().toDouble / count else 0.0
  }
  
  // Median priority threshold (only used by MEDIAN strategy)
  @volatile private var medianPriority: Long = Long.MinValue
  
  def updateMedian(priorities: Array[Long]): Unit = {
    if (priorities.nonEmpty) {
      val sorted = priorities.sorted
      val median = if (sorted.length % 2 == 0) {
        (sorted(sorted.length / 2 - 1) + sorted(sorted.length / 2)) / 2
      } else {
        sorted(sorted.length / 2)
      }
      medianPriority = median
      logDebug(s"Updated median priority to $median " +
        s"(from ${priorities.length} tasks, range: [${sorted.head}, ${sorted.last}])")
    }
  }
  
  private def isHighPriorityMedian(task: RapidsFutureTask[_]): Boolean = {
    // Rule 1: If already served, always HIGH priority (avoid starvation)
    task.runner.sparkTaskContext match {
      case Some(ctx) if servedTasks.contains(ctx.taskAttemptId()) =>
        true
      case Some(_) =>
        // Rule 2: Compare against median (higher value = higher scheduling priority)
        task.runner.priority >= medianPriority
      case None =>
        true  // No task context, treat as high priority
    }
  }
  
  override def compare(r1: Runnable, r2: Runnable): Int = {
    val startNanos = System.nanoTime()
    try {
      (r1, r2) match {
        case (t1: RapidsFutureTask[_], t2: RapidsFutureTask[_]) =>
          strategy match {
            case NoneStrategy =>
              // Should not reach here, but for safety use FIFO
              0
            
            case MedianStrategy =>
              val high1 = isHighPriorityMedian(t1)
              val high2 = isHighPriorityMedian(t2)
              
              if (high1 && !high2) {
                -1  // t1 scheduled first
              } else if (!high1 && high2) {
                1   // t2 scheduled first
              } else {
                // Both same class (HIGH or LOW), order by priority value
                // Higher priority value = schedule first
                java.lang.Long.compare(t2.runner.priority, t1.runner.priority)
              }
            
            case StrictStrategy =>
              // Strictly order by priority value (higher = earlier)
              // Same as standard RapidsFutureTaskComparator
              java.lang.Long.compare(t2.runner.priority, t1.runner.priority)
          }
        case _ =>
          0  // Not RapidsFutureTasks, treat as equal
      }
    } finally {
      compareCount.incrementAndGet()
      compareTotalNanos.addAndGet(System.nanoTime() - startNanos)
    }
  }
}

/**
 * Priority-aware thread pool for file reading with bandwidth-aware scheduling.
 * Uses RapidsFutureTask to wrap AsyncRunners for efficient priority access.
 * 
 * Features:
 * - Uses RapidsFutureTask.runner.priority directly (no reflection needed)
 * - Supports MEDIAN and STRICT priority strategies
 * - Prevents low-priority tasks from consuming I/O bandwidth prematurely
 */
class PriorityAwareFileReaderThreadPool private(
    numThreads: Int,
    name: String,
    servedTasks: ConcurrentHashMap.KeySetView[Long, java.lang.Boolean],
    strategy: PrioritySchedulingStrategy)
  extends ThreadPoolExecutor(
    numThreads,
    numThreads,
    60L,
    TimeUnit.SECONDS,
    new PriorityBlockingQueue[Runnable](1024, 
      new BandwidthAwareRapidsFutureTaskComparator(strategy, servedTasks)),
    new ThreadFactoryBuilder().setNameFormat(s"$name-%d").setDaemon(true).build()
  ) with Logging {
  
  private val comparator = getQueue.asInstanceOf[PriorityBlockingQueue[Runnable]]
    .comparator().asInstanceOf[BandwidthAwareRapidsFutureTaskComparator[_]]
  
  // Track all priorities for median calculation
  private val allPriorities = new ConcurrentHashMap[Long, java.lang.Long]()
  
  // Update median periodically (only relevant for MEDIAN strategy)
  private val submissionCount = new java.util.concurrent.atomic.AtomicInteger(0)
  private val MEDIAN_UPDATE_INTERVAL = 20  // Update every 20 submissions
  
  // Metrics for performance analysis
  private val submitRunnerCount = new java.util.concurrent.atomic.AtomicLong(0)
  private val submitRunnerTotalNanos = new java.util.concurrent.atomic.AtomicLong(0)
  private val beforeExecuteCount = new java.util.concurrent.atomic.AtomicLong(0)
  private val beforeExecuteTotalNanos = new java.util.concurrent.atomic.AtomicLong(0)
  private val afterExecuteCount = new java.util.concurrent.atomic.AtomicLong(0)
  private val afterExecuteTotalNanos = new java.util.concurrent.atomic.AtomicLong(0)
  
  logInfo(s"Created priority-aware file reader thread pool with strategy: $strategy")
  
  /**
   * Submit an AsyncRunner using RapidsFutureTask wrapper.
   * This is the primary method to use.
   */
  def submitRunner[T](runner: AsyncRunner[T]): Future[AsyncResult[T]] = {
    val startNanos = System.nanoTime()
    try {
      // Track priority if task context is available
      runner.sparkTaskContext.foreach { ctx =>
        val taskAttemptId = ctx.taskAttemptId()
        val taskPriority = runner.priority
        
        allPriorities.put(taskAttemptId, taskPriority)
        
        // Periodically recalculate median (for MEDIAN strategy)
        if (strategy == MedianStrategy &&
            submissionCount.incrementAndGet() % MEDIAN_UPDATE_INTERVAL == 0) {
          val priorities = allPriorities.values().toArray(new Array[java.lang.Long](0))
            .map(_.longValue())
          comparator.updateMedian(priorities)
        }
      }
      
      // Create RapidsFutureTask and submit
      val task = new RapidsFutureTask[T](runner)
      execute(task)
      task
    } finally {
      submitRunnerCount.incrementAndGet()
      submitRunnerTotalNanos.addAndGet(System.nanoTime() - startNanos)
    }
  }
  
  override def beforeExecute(t: Thread, r: Runnable): Unit = {
    val startNanos = System.nanoTime()
    try {
      super.beforeExecute(t, r)
      
      r match {
        case task: RapidsFutureTask[_] =>
          task.runner.withStateLock { rr =>
            // Check if the Spark task has been interrupted before execution
            rr.sparkTaskContext.foreach {
              case ctx if ctx.isInterrupted() => rr.setState(Cancelled)
              case _ => // do nothing
            }
            
            // Handle state transition
            rr.getState match {
              case Cancelled =>
                rr.setState(ScheduleFailed(new IllegalStateException("cancelled")))
                logWarning(s"Runner being cancelled ahead of execution: $rr")
                
              case _: Init =>
                // For priority-aware pool without resource management,
                // directly transition to Running state
                rr.setState(Running)
                
              case _ =>
                rr.setState(ScheduleFailed(new IllegalStateException("Unexpected state")))
                logError(s"Unexpected state before schedule: $rr")
            }
          }
        case _ =>
      }
    } finally {
      beforeExecuteCount.incrementAndGet()
      beforeExecuteTotalNanos.addAndGet(System.nanoTime() - startNanos)
    }
  }
  
  override def afterExecute(r: Runnable, t: Throwable): Unit = {
    val startNanos = System.nanoTime()
    try {
      super.afterExecute(r, t)
      
      // Mark task as served after execution
      r match {
        case task: RapidsFutureTask[_] =>
          task.runner.sparkTaskContext.foreach { ctx =>
            val taskAttemptId = ctx.taskAttemptId()
            servedTasks.add(taskAttemptId)
            logDebug(s"Task $taskAttemptId completed, marked as served")
          }
        case _ =>
      }
    } finally {
      afterExecuteCount.incrementAndGet()
      afterExecuteTotalNanos.addAndGet(System.nanoTime() - startNanos)
    }
  }
  
  /**
   * Clear priority tracking for a specific task (called when task completes).
   */
  def taskCompleted(taskAttemptId: Long): Unit = {
    allPriorities.remove(taskAttemptId)
    servedTasks.remove(taskAttemptId)
  }
  
  /**
   * Get metrics summary for performance analysis.
   */
  def getMetricsSummary: String = {
    val submitCount = submitRunnerCount.get()
    val submitAvgUs = if (submitCount > 0) {
      submitRunnerTotalNanos.get() / 1000.0 / submitCount
    } else 0.0
    
    val beforeCount = beforeExecuteCount.get()
    val beforeAvgUs = if (beforeCount > 0) {
      beforeExecuteTotalNanos.get() / 1000.0 / beforeCount
    } else 0.0
    
    val afterCount = afterExecuteCount.get()
    val afterAvgUs = if (afterCount > 0) {
      afterExecuteTotalNanos.get() / 1000.0 / afterCount
    } else 0.0
    
    val compareCount = comparator.getCompareCount
    val compareAvgNs = comparator.getCompareAvgNanos
    
    s"""PriorityAwareFileReaderThreadPool Metrics (strategy=$strategy):
       |  submitRunner: count=$submitCount, totalMs=${submitRunnerTotalNanos.get()/1e6}ms, avgUs=${"%.2f".format(submitAvgUs)}us
       |  beforeExecute: count=$beforeCount, totalMs=${beforeExecuteTotalNanos.get()/1e6}ms, avgUs=${"%.2f".format(beforeAvgUs)}us
       |  afterExecute: count=$afterCount, totalMs=${afterExecuteTotalNanos.get()/1e6}ms, avgUs=${"%.2f".format(afterAvgUs)}us
       |  comparator: count=$compareCount, totalMs=${comparator.getCompareTotalNanos/1e6}ms, avgNs=${"%.2f".format(compareAvgNs)}ns
       |  Total overhead: ${"%.2f".format((submitRunnerTotalNanos.get() + beforeExecuteTotalNanos.get() + afterExecuteTotalNanos.get() + comparator.getCompareTotalNanos) / 1e6)}ms""".stripMargin
  }
  
  /**
   * Log metrics summary.
   */
  def logMetrics(): Unit = {
    logInfo(getMetricsSummary)
  }
}

object PriorityAwareFileReaderThreadPool extends Logging {
  @volatile
  private var globalPool: Option[PriorityAwareFileReaderThreadPool] = None
  
  // Stage-level pools for better isolation
  private val stagePools = new ConcurrentHashMap[Int, PriorityAwareFileReaderThreadPool]()
  
  // Global served tasks tracking (shared across all pools)
  private val globalServedTasks = ConcurrentHashMap.newKeySet[Long]()
  
  // Register shutdown hook to log metrics on JVM exit
  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      logMetricsOnShutdown()
    }
  })
  
  private def logMetricsOnShutdown(): Unit = {
    globalPool.foreach { pool =>
      logInfo("=== PriorityAwareFileReaderThreadPool Shutdown Metrics ===")
      pool.logMetrics()
    }
    stagePools.values().forEach { pool =>
      pool.logMetrics()
    }
  }
  
  def getOrCreate(
      numThreads: Int,
      name: String,
      useStageLevel: Boolean = false,
      strategyStr: String = "MEDIAN"): PriorityAwareFileReaderThreadPool = {
    
    val strategy = PrioritySchedulingStrategy.fromString(strategyStr)
    
    if (useStageLevel) {
      val taskContext = TaskContext.get()
      if (taskContext != null) {
        val stageId = taskContext.stageId()
        stagePools.computeIfAbsent(stageId, _ => {
          val pool = new PriorityAwareFileReaderThreadPool(
            numThreads, 
            s"$name-stage-$stageId",
            globalServedTasks,
            strategy)
          pool.allowCoreThreadTimeOut(true)
          logInfo(s"Created priority-aware file reader thread pool for stage $stageId " +
            s"with $numThreads threads, strategy: $strategy")
          pool
        })
      } else {
        // Fallback to global if no task context
        getOrCreateGlobalPool(numThreads, name, strategy)
      }
    } else {
      getOrCreateGlobalPool(numThreads, name, strategy)
    }
  }
  
  private def getOrCreateGlobalPool(
      numThreads: Int,
      name: String,
      strategy: PrioritySchedulingStrategy): PriorityAwareFileReaderThreadPool = {
    globalPool.getOrElse {
      synchronized {
        globalPool.getOrElse {
          val pool = new PriorityAwareFileReaderThreadPool(
            numThreads, name, globalServedTasks, strategy)
          pool.allowCoreThreadTimeOut(true)
          globalPool = Some(pool)
          logInfo(s"Created global priority-aware file reader thread pool with " +
            s"$numThreads threads, strategy: $strategy")
          pool
        }
      }
    }
  }
  
  def shutdown(): Unit = synchronized {
    globalPool.foreach { pool =>
      // Log metrics before shutdown
      pool.logMetrics()
      pool.shutdown()
      if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
        pool.shutdownNow()
      }
    }
    globalPool = None
    
    stagePools.values().forEach { pool =>
      // Log metrics before shutdown
      pool.logMetrics()
      pool.shutdown()
      if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
        pool.shutdownNow()
      }
    }
    stagePools.clear()
    
    // Clear served tasks tracking
    globalServedTasks.clear()
  }
  
  /**
   * Get global pool metrics (for testing).
   */
  def getGlobalPoolMetrics: Option[String] = globalPool.map(_.getMetricsSummary)
}

