/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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
import java.util.concurrent.locks.{Condition, ReentrantLock}

import scala.collection.mutable
import scala.math.Ordering.comparatorToOrdering

import org.apache.spark.TaskContext
import org.apache.spark.sql.rapids.GpuTaskMetrics
import org.apache.spark.sql.rapids.GpuTaskMetrics.DynamicGpuTaskMetricsSummary
import org.apache.spark.sql.rapids.GpuTaskMetrics.DynamicGpuTaskMetricsSummary._

class PrioritySemaphore(val maxPermits: Int)(implicit ordering: Ordering[Long]) {
  // This lock is used to generate condition variables, which affords us the flexibility to notify
  // specific threads at a time. If we use the regular synchronized pattern, we have to either
  // notify randomly, or if we try creating condition variables not tied to a shared lock, they
  // won't work together properly, and we see things like deadlocks.
  private val lock = new ReentrantLock()
  private var occupiedSlots: Int = 0
  private val priorityForUnstarted = Long.MaxValue

  private case class ThreadInfo(priority: Long, condition: Condition, numPermits: Int,
      taskId: Long) {
    var signaled: Boolean = false
  }

  // We expect a relatively small number of threads to be contending for this lock at any given
  // time, therefore we are not concerned with the insertion/removal time complexity.
  private val waitingQueue: mutable.ListBuffer[ThreadInfo] = mutable.ListBuffer()

  private val threadInfoComp = new Comparator[ThreadInfo] {
    override def compare(t1: ThreadInfo, t2: ThreadInfo): Int = {
      val t1Permitted = DynamicGpuTaskMetricsSummary.flowControlPermitTasks.contains(t1.taskId)
      val t2Permitted = DynamicGpuTaskMetricsSummary.flowControlPermitTasks.contains(t2.taskId)
      if (t1Permitted && !t2Permitted) {
        -1
      } else if (!t1Permitted && t2Permitted) {
        1
      } else {
        ordering.compare(t1.priority, t2.priority)
      }
    }
  }

  def tryAcquire(numPermits: Int, priority: Long, numCores: Int): Boolean = {
    lock.lock()
    try {
      if (waitingQueue.nonEmpty &&
        waitingQueue.exists(info => ordering.lt(info.priority, priority))) {
        false
      } else if (!canAcquire(numPermits, numCores, TaskContext.get().taskAttemptId())) {
        false
      } else {
        commitAcquire(numPermits)
        true
      }
    } finally {
      lock.unlock()
    }
  }

  def acquire(numPermits: Int, priority: Long, numCores: Int = 1): Unit = {
    lock.lock()
    try {
      if (!tryAcquire(numPermits, priority, numCores)) {
        val condition = lock.newCondition()
        val taskId = TaskContext.get().taskAttemptId()
        val info = ThreadInfo(priority, condition, numPermits, taskId)
        try {
          waitingQueue += info

          // only count tasks that had held semaphore before,
          // so they're very likely to have remaining data on GPU
          GpuTaskMetrics.get.recordOnGpuTasksNumber(
            waitingQueue.count(_.priority != priorityForUnstarted))

          while (!info.signaled) {
            info.condition.await()
          }
        } catch {
          case e: Exception =>
            waitingQueue -= info
            if (info.signaled) {
              release(numPermits, numCores)
            }
            throw e
        }
      }
    } finally {
      lock.unlock()
    }
  }

  private def commitAcquire(numPermits: Int): Unit = {
    occupiedSlots += numPermits
  }


  def signalOthers(numCores: Int): Unit = {
    lock.lock()
    try {
      // acquire and wakeup for all threads that now have enough permits
      var done = false
      while (!done && waitingQueue.nonEmpty) {
        val nextThread = waitingQueue.min(comparatorToOrdering(threadInfoComp))
        if (canAcquire(nextThread.numPermits, numCores, nextThread.taskId)) {
          waitingQueue -= nextThread
          println(s"Flow control released!" +
            s" task id: ${nextThread.taskId} current " +
            s"permit tasks: ${DynamicGpuTaskMetricsSummary.flowControlPermitTasks}")
          commitAcquire(nextThread.numPermits)
          nextThread.signaled = true
          nextThread.condition.signal()
        } else {
          done = true
        }
      }
    } finally {
      lock.unlock()
    }
  }

  def release(numPermits: Int, numCores: Int = 1): Unit = {
    lock.lock()
    try {
      occupiedSlots -= numPermits
      signalOthers(numCores)
    } finally {
      lock.unlock()
    }
  }

  private def canAcquire(numPermits: Int, numCores: Int, taskId: Long): Boolean = {
    // Flow control logic
    def updateFlowControl(): Unit = {
      val summary = DynamicGpuTaskMetricsSummary.getSummary
      println(s"updating Flow control: " +
        s"runtime: ${summary._1} spill time: ${summary._2} " +
        s"cores: ${numCores} max tasks: ${flowControlMaxTasks}")
      if (summary._2 * numCores > summary._1 * 0.5) {
        flowControlMaxTasks = Math.max(1, flowControlMaxTasks / 2)
        println(s"Flow control updated!!!!! " +
          s"max tasks: ${flowControlMaxTasks} trigger task: ${taskId}")
//        flowControlPermitTasks.clear()
        GpuTaskMetrics.DynamicGpuTaskMetricsSummary.excludeTasks(
            GpuSemaphore.activeTaskIds --
              waitingQueue.filter(_.priority == priorityForUnstarted).map(_.taskId))
        GpuTaskMetrics.DynamicGpuTaskMetricsSummary.reset()
      }
    }
    if (!flowControlPermitTasks.contains(taskId)) {
      updateFlowControl()
      if (flowControlPermitTasks.size < flowControlMaxTasks) {
        println(s"Flow control accepted!" +
          s" task id: ${taskId} current permit tasks: ${flowControlPermitTasks}" +
          s" max tasks: ${flowControlMaxTasks}")
        flowControlPermitTasks.add(taskId)
      } else {
        println(s"Flow control rejected!" +
          s" task id: ${taskId} current permit tasks: ${flowControlPermitTasks}" +
          s" max tasks: ${flowControlMaxTasks}")
        return false
      }
    }

    // basic permits
    occupiedSlots + numPermits <= maxPermits
  }

}
