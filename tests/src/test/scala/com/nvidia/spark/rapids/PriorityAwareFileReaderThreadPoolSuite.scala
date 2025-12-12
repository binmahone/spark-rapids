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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import com.nvidia.spark.rapids.io.async.{AsyncResult, AsyncRunner}

import org.apache.spark.TaskContext
import org.scalatest.funsuite.AnyFunSuite

class PriorityAwareFileReaderThreadPoolSuite extends AnyFunSuite {
  
  /**
   * Mock AsyncRunner for testing with configurable priority
   */
  class TestRunner(
      val taskAttemptId: Long,
      val runnerPriority: Long,
      val taskContext: Option[TaskContext],
      val executionOrder: AtomicInteger,
      val startLatch: CountDownLatch,
      val completionLatch: CountDownLatch)
    extends AsyncRunner[Int] {
    
    override def priority: Long = runnerPriority
    override def sparkTaskContext: Option[TaskContext] = taskContext
    override def resource: io.async.AsyncRunResource = 
      io.async.AsyncRunResource.newCpuResource(0)
    
    private var executionIndex: Int = -1
    
    override protected def callImpl(): Int = {
      executionIndex = executionOrder.incrementAndGet()
      startLatch.countDown()
      completionLatch.await(5, TimeUnit.SECONDS)
      executionIndex
    }
    
    override protected def buildResult(
        resultData: Int, 
        metrics: io.async.AsyncMetrics): AsyncResult[Int] = {
      new io.async.FastReleaseResult(resultData, metrics)
    }
    
    def getExecutionIndex: Int = executionIndex
  }
  
  test("STRICT strategy orders tasks strictly by priority") {
    val pool = PriorityAwareFileReaderThreadPool.getOrCreate(
      numThreads = 2,
      name = "test-strict-pool",
      useStageLevel = false,
      strategyStr = "STRICT")
    
    try {
      val executionOrder = new AtomicInteger(0)
      val startLatch = new CountDownLatch(4)
      val completionLatch = new CountDownLatch(1)
      
      // Create 4 runners with different priorities
      // Higher priority value = should execute first
      val runners = Seq(
        new TestRunner(1001L, 100L, None, executionOrder, startLatch, completionLatch),
        new TestRunner(1002L, 300L, None, executionOrder, startLatch, completionLatch),
        new TestRunner(1003L, 200L, None, executionOrder, startLatch, completionLatch),
        new TestRunner(1004L, 400L, None, executionOrder, startLatch, completionLatch)
      )
      
      // Submit all runners
      val futures = runners.map(r => pool.submitRunner(r))
      
      // Wait for all to start
      assert(startLatch.await(5, TimeUnit.SECONDS), "Tasks did not start in time")
      
      // Let them complete
      completionLatch.countDown()
      
      // Wait for all to finish
      futures.foreach(_.get(5, TimeUnit.SECONDS))
      
      // Verify execution order: should be 400, 300, 200, 100
      // Task with priority 400 should execute first (index 1)
      // Task with priority 300 should execute second (index 2)
      // etc.
      assert(runners(3).getExecutionIndex < runners(1).getExecutionIndex,
        s"Priority 400 task should execute before priority 300 task")
      assert(runners(1).getExecutionIndex < runners(2).getExecutionIndex,
        s"Priority 300 task should execute before priority 200 task")
      assert(runners(2).getExecutionIndex < runners(0).getExecutionIndex,
        s"Priority 200 task should execute before priority 100 task")
      
    } finally {
      pool.shutdown()
      pool.awaitTermination(5, TimeUnit.SECONDS)
    }
  }
  
  test("MEDIAN strategy classifies tasks into HIGH and LOW") {
    val pool = PriorityAwareFileReaderThreadPool.getOrCreate(
      numThreads = 1,  // Single thread to control execution order
      name = "test-median-pool",
      useStageLevel = false,
      strategyStr = "MEDIAN")
    
    try {
      val executionOrder = new AtomicInteger(0)
      val allStarted = new CountDownLatch(4)
      val completionLatch = new CountDownLatch(1)
      
      // Create 4 runners: priorities 100, 200, 300, 400
      // Median = (200 + 300) / 2 = 250
      // HIGH: 300, 400 (>= median)
      // LOW: 100, 200 (< median)
      val runners = Seq(
        new TestRunner(2001L, 100L, None, executionOrder, allStarted, completionLatch),
        new TestRunner(2002L, 200L, None, executionOrder, allStarted, completionLatch),
        new TestRunner(2003L, 300L, None, executionOrder, allStarted, completionLatch),
        new TestRunner(2004L, 400L, None, executionOrder, allStarted, completionLatch)
      )
      
      // Submit all runners to populate priority tracking
      val futures = runners.map { r =>
        pool.submitRunner(r)
      }
      
      // Give time for median to be calculated
      Thread.sleep(100)
      
      // Let tasks complete
      completionLatch.countDown()
      
      // Wait for all to finish
      futures.foreach(_.get(5, TimeUnit.SECONDS))
      
      // HIGH priority tasks (300, 400) should execute before LOW (100, 200)
      val high1 = runners(2).getExecutionIndex  // priority 300
      val high2 = runners(3).getExecutionIndex  // priority 400
      val low1 = runners(0).getExecutionIndex   // priority 100
      val low2 = runners(1).getExecutionIndex   // priority 200
      
      // All HIGH should be < all LOW
      assert(high1 < low1 && high1 < low2, 
        s"HIGH priority 300 (idx=$high1) should execute before LOW tasks")
      assert(high2 < low1 && high2 < low2,
        s"HIGH priority 400 (idx=$high2) should execute before LOW tasks")
      
    } finally {
      pool.shutdown()
      pool.awaitTermination(5, TimeUnit.SECONDS)
    }
  }
  
  test("NONE strategy should not be used with PriorityAwareFileReaderThreadPool") {
    // NONE strategy means don't use priority pool at all
    // This test just verifies the pool can be created but won't reorder
    val pool = PriorityAwareFileReaderThreadPool.getOrCreate(
      numThreads = 2,
      name = "test-none-pool",
      useStageLevel = false,
      strategyStr = "NONE")
    
    try {
      val executionOrder = new AtomicInteger(0)
      val startLatch = new CountDownLatch(2)
      val completionLatch = new CountDownLatch(1)
      
      val runners = Seq(
        new TestRunner(3001L, 100L, None, executionOrder, startLatch, completionLatch),
        new TestRunner(3002L, 200L, None, executionOrder, startLatch, completionLatch)
      )
      
      val futures = runners.map(r => pool.submitRunner(r))
      
      startLatch.await(5, TimeUnit.SECONDS)
      completionLatch.countDown()
      
      futures.foreach(_.get(5, TimeUnit.SECONDS))
      
      // Just verify they both executed
      assert(runners(0).getExecutionIndex > 0)
      assert(runners(1).getExecutionIndex > 0)
      
    } finally {
      pool.shutdown()
      pool.awaitTermination(5, TimeUnit.SECONDS)
    }
  }
  
  test("Integration with RapidsFutureTask") {
    val pool = PriorityAwareFileReaderThreadPool.getOrCreate(
      numThreads = 2,
      name = "test-integration-pool",
      useStageLevel = false,
      strategyStr = "STRICT")
    
    try {
      val executionOrder = new AtomicInteger(0)
      val startLatch = new CountDownLatch(3)
      val completionLatch = new CountDownLatch(1)
      
      val runners = Seq(
        new TestRunner(4001L, 100L, None, executionOrder, startLatch, completionLatch),
        new TestRunner(4002L, 300L, None, executionOrder, startLatch, completionLatch),
        new TestRunner(4003L, 200L, None, executionOrder, startLatch, completionLatch)
      )
      
      // Submit using submitRunner which creates RapidsFutureTask internally
      val futures = runners.map(r => pool.submitRunner(r))
      
      startLatch.await(5, TimeUnit.SECONDS)
      completionLatch.countDown()
      
      // Verify all completed successfully
      val results = futures.map(_.get(5, TimeUnit.SECONDS))
      results.foreach { asyncResult =>
        assert(asyncResult.data > 0, "Task should have executed")
        asyncResult.close()
      }
      
      // Verify priority ordering: 300 > 200 > 100
      assert(runners(1).getExecutionIndex < runners(2).getExecutionIndex)
      assert(runners(2).getExecutionIndex < runners(0).getExecutionIndex)
      
    } finally {
      pool.shutdown()
      pool.awaitTermination(5, TimeUnit.SECONDS)
    }
  }
  
  test("Cleanup after shutdown") {
    val pool = PriorityAwareFileReaderThreadPool.getOrCreate(
      numThreads = 1,
      name = "test-cleanup-pool",
      useStageLevel = false,
      strategyStr = "MEDIAN")
    
    val executionOrder = new AtomicInteger(0)
    val startLatch = new CountDownLatch(1)
    val completionLatch = new CountDownLatch(1)
    
    val runner = new TestRunner(
      5001L, 100L, None, executionOrder, startLatch, completionLatch)
    
    val future = pool.submitRunner(runner)
    
    startLatch.await(5, TimeUnit.SECONDS)
    completionLatch.countDown()
    future.get(5, TimeUnit.SECONDS).close()
    
    // Shutdown and verify
    pool.shutdown()
    assert(pool.awaitTermination(5, TimeUnit.SECONDS), "Pool should terminate cleanly")
    assert(pool.isTerminated, "Pool should be terminated")
  }
}

