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

import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite for ASIO (Adaptive Saturation I/O) components.
 * 
 * ASIO is a dynamic I/O optimization mechanism that maximizes cloud storage throughput
 * by intelligently splitting large read tasks into parallel byte-range requests.
 */
class AsioSuite extends AnyFunSuite {

  // ============================================================================
  // Range Distribution Tests
  // ============================================================================

  test("distributeRanges - single range split into multiple chunks") {
    val method = ParallelFileIO.getClass.getDeclaredMethod(
      "splitSingleRange", 
      classOf[Long], classOf[Long], classOf[Long], classOf[Int])
    method.setAccessible(true)
    
    // 100MB range, split into 4
    val result = method.invoke(ParallelFileIO, 
      Long.box(0L), Long.box(0L), Long.box(100L * 1024 * 1024), Int.box(4))
      .asInstanceOf[Seq[Seq[(Long, Long, Long)]]]
    
    assert(result.size === 4, s"Expected 4 groups, got ${result.size}")
    
    // Verify all bytes are covered
    val totalBytes = result.flatMap(_.map(_._3)).sum
    assert(totalBytes === 100L * 1024 * 1024)
    
    // Verify ranges are contiguous
    var expectedFileOffset = 0L
    var expectedBufferOffset = 0L
    result.foreach { group =>
      assert(group.size === 1)
      val (fileOffset, bufferOffset, length) = group.head
      assert(fileOffset === expectedFileOffset)
      assert(bufferOffset === expectedBufferOffset)
      expectedFileOffset += length
      expectedBufferOffset += length
    }
  }

  test("distributeRanges - multiple small ranges grouped") {
    val method = ParallelFileIO.getClass.getDeclaredMethod(
      "distributeRanges", 
      classOf[Seq[(Long, Long, Long)]], classOf[Int])
    method.setAccessible(true)
    
    // 10 ranges of 10MB each = 100MB total, split into 4 groups
    val ranges = (0 until 10).map { i =>
      val offset = i * 10L * 1024 * 1024
      (offset, offset, 10L * 1024 * 1024)
    }
    
    val result = method.invoke(ParallelFileIO, ranges, Int.box(4))
      .asInstanceOf[Seq[Seq[(Long, Long, Long)]]]
    
    assert(result.size === 4, s"Expected 4 groups, got ${result.size}")
    assert(result.map(_.size).sum === 10)
  }

  test("distributeRanges - respects MIN_PARALLEL_SIZE") {
    val method = ParallelFileIO.getClass.getDeclaredMethod(
      "splitSingleRange", 
      classOf[Long], classOf[Long], classOf[Long], classOf[Int])
    method.setAccessible(true)
    
    // 30MB range, try to split into 10 (would be 3MB each, below 10MB min)
    val result = method.invoke(ParallelFileIO, 
      Long.box(0L), Long.box(0L), Long.box(30L * 1024 * 1024), Int.box(10))
      .asInstanceOf[Seq[Seq[(Long, Long, Long)]]]
    
    // Should respect MIN_PARALLEL_SIZE (10MB), so max 3 chunks
    assert(result.size <= 3, s"Expected <= 3 groups, got ${result.size}")
    assert(result.flatMap(_.map(_._3)).sum === 30L * 1024 * 1024)
  }

  // ============================================================================
  // LoadPredictor Tests
  // ============================================================================

  test("LoadPredictor - getIdleSlots returns current idle") {
    val sampler = new MockPoolSampler(numThreads = 32)
    val predictor = new LoadPredictor(sampler)
    
    // All idle
    assert(predictor.getIdleSlots === 32)
    
    // Some active
    sampler.setActiveCount(20)
    sampler.setQueueSize(5)
    assert(predictor.getIdleSlots === 7)  // 32 - 20 - 5
    
    // Overloaded
    sampler.setActiveCount(30)
    sampler.setQueueSize(10)
    assert(predictor.getIdleSlots === 0)  // max(0, 32 - 30 - 10)
  }

  test("LoadPredictor - getAvailableSlots reserves buffer") {
    val sampler = new MockPoolSampler(numThreads = 32)
    // Reserve 20% = 6.4, min 2, so reserve 6
    val predictor = new LoadPredictor(sampler, reserveRatio = 0.2, minReserve = 2)
    
    // All idle: 32 - 6 reserve = 26 available
    assert(predictor.getAvailableSlots === 26)
    
    // Some active: 32 - 20 - 5 = 7 idle, - 6 reserve = 1 available
    sampler.setActiveCount(20)
    sampler.setQueueSize(5)
    assert(predictor.getAvailableSlots === 1)
    
    // Low idle: 32 - 28 - 0 = 4 idle, - 6 reserve = 0 available
    sampler.setActiveCount(28)
    sampler.setQueueSize(0)
    assert(predictor.getAvailableSlots === 0)
  }

  test("LoadPredictor - size history tracking") {
    val sampler = new MockPoolSampler(numThreads = 32)
    val predictor = new LoadPredictor(sampler, sizeHistoryLength = 5)
    
    // No history yet
    assert(predictor.getAverageReaderSize === 0L)
    
    // Add some sizes
    predictor.recordReaderSize(10L * 1024 * 1024)  // 10MB
    predictor.recordReaderSize(20L * 1024 * 1024)  // 20MB
    predictor.recordReaderSize(30L * 1024 * 1024)  // 30MB
    
    // Average should be 20MB
    assert(predictor.getAverageReaderSize === 20L * 1024 * 1024)
    
    // Add more to fill history
    predictor.recordReaderSize(40L * 1024 * 1024)  // 40MB
    predictor.recordReaderSize(50L * 1024 * 1024)  // 50MB
    
    // Average of 10, 20, 30, 40, 50 = 30MB
    assert(predictor.getAverageReaderSize === 30L * 1024 * 1024)
    
    // Add one more (circular buffer wraps)
    predictor.recordReaderSize(100L * 1024 * 1024)  // 100MB
    
    // Now: 100, 20, 30, 40, 50 -> avg = 48MB
    assert(predictor.getAverageReaderSize === 48L * 1024 * 1024)
  }

  test("LoadPredictor - isLargeReader identifies large readers") {
    val sampler = new MockPoolSampler(numThreads = 32)
    val predictor = new LoadPredictor(sampler, sizeHistoryLength = 10)
    
    // Build up history with 50MB average
    for (_ <- 1 to 10) {
      predictor.recordReaderSize(50L * 1024 * 1024)
    }
    assert(predictor.getAverageReaderSize === 50L * 1024 * 1024)
    
    // Large = >= 1.5x average = 75MB
    assert(!predictor.isLargeReader(50L * 1024 * 1024))  // average, not large
    assert(!predictor.isLargeReader(70L * 1024 * 1024))  // below 1.5x
    assert(predictor.isLargeReader(75L * 1024 * 1024))   // exactly 1.5x
    assert(predictor.isLargeReader(100L * 1024 * 1024))  // above 1.5x
  }

  test("LoadPredictor - isLargeReader with no history") {
    val sampler = new MockPoolSampler(numThreads = 32)
    val predictor = new LoadPredictor(sampler)
    
    // No history: uses MIN_PARALLEL_SIZE * 2 = 20MB threshold
    assert(!predictor.isLargeReader(10L * 1024 * 1024))  // 10MB - not large
    assert(!predictor.isLargeReader(19L * 1024 * 1024))  // 19MB - not large
    assert(predictor.isLargeReader(20L * 1024 * 1024))   // 20MB - large
    assert(predictor.isLargeReader(100L * 1024 * 1024))  // 100MB - large
  }

  test("LoadPredictor - reset clears history") {
    val sampler = new MockPoolSampler(numThreads = 32)
    val predictor = new LoadPredictor(sampler)
    
    // Add some history
    predictor.recordReaderSize(100L * 1024 * 1024)
    predictor.recordReaderSize(200L * 1024 * 1024)
    assert(predictor.getAverageReaderSize > 0)
    
    // Reset
    predictor.reset()
    assert(predictor.getAverageReaderSize === 0L)
  }

  // ============================================================================
  // Integration-like Tests
  // ============================================================================

  test("LoadPredictor - prevents overload via natural mechanism") {
    val sampler = new MockPoolSampler(numThreads = 32)
    val predictor = new LoadPredictor(sampler, reserveRatio = 0.2, minReserve = 2)
    
    // Initially: 26 available (32 - 6 reserve)
    assert(predictor.getAvailableSlots === 26)
    
    // After first split uses 10 slots: 22 active, 0 queue
    sampler.setActiveCount(22)
    sampler.setQueueSize(0)
    assert(predictor.getAvailableSlots === 4)  // 32 - 22 - 6
    
    // After more splits: 30 active
    sampler.setActiveCount(30)
    sampler.setQueueSize(0)
    assert(predictor.getAvailableSlots === 0)  // no room for more splits
    
    // Tasks complete: back to 10 active
    sampler.setActiveCount(10)
    sampler.setQueueSize(0)
    assert(predictor.getAvailableSlots === 16)  // 32 - 10 - 6
  }
}

/**
 * Mock pool state sampler for testing.
 */
class MockPoolSampler(numThreads: Int) extends PoolStateSampler {
  @volatile private var mockActiveCount: Int = 0
  @volatile private var mockQueueSize: Int = 0
  
  def setActiveCount(count: Int): Unit = mockActiveCount = count
  def setQueueSize(size: Int): Unit = mockQueueSize = size
  
  override def getActiveCount: Int = mockActiveCount
  override def getQueueSize: Int = mockQueueSize
  override def getNumThreads: Int = numThreads
}
