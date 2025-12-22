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

import java.util.concurrent.Future
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.io.async._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

/**
 * Trait for sampling pool state, allows mocking in tests.
 */
trait PoolStateSampler {
  def getActiveCount: Int
  def getQueueSize: Int
  def getNumThreads: Int
}

/**
 * Default sampler that reads from a real PriorityAwareFileReaderThreadPool.
 */
class ThreadPoolSampler(pool: PriorityAwareFileReaderThreadPool) extends PoolStateSampler {
  override def getActiveCount: Int = pool.getActiveCount
  override def getQueueSize: Int = pool.getQueue.size()
  override def getNumThreads: Int = pool.getCorePoolSize
}

/**
 * Load monitor that tracks pool state and reader size distribution.
 * 
 * Key features:
 * 1. Reserve slots: keeps some idle slots as buffer (default 20% or min 2)
 * 2. Size-aware splitting: only split readers larger than recent average
 *    (small readers don't benefit much from splitting, waste slots)
 * 
 * Example flow:
 *   Task1 (100MB): sees 10 idle, reserves 2 -> can use 8 -> splits into 8
 *   Task2 (20MB): sees 2 idle, but 20MB < avgSize -> no split
 *   Task3 (200MB): sees 5 idle, 200MB > avgSize -> splits into 3
 * 
 * @param sampler The pool state sampler
 * @param reserveRatio Ratio of slots to keep as reserve (default 0.2)
 * @param minReserve Minimum slots to reserve (default 2)
 * @param sizeHistoryLength Number of recent sizes to track (default 50)
 */
class LoadPredictor(
    sampler: PoolStateSampler,
    reserveRatio: Double = 0.2,
    minReserve: Int = 2,
    sizeHistoryLength: Int = 50) extends AutoCloseable {
  
  private val numThreads = sampler.getNumThreads
  
  // Circular buffer to track recent reader sizes
  private val sizeHistory = new Array[Long](sizeHistoryLength)
  @volatile private var sizeHistoryIndex = 0
  @volatile private var sizeHistoryCount = 0
  private val sizeHistoryLock = new Object
  
  /**
   * Convenience constructor for use with a real pool.
   */
  def this(pool: PriorityAwareFileReaderThreadPool) = {
    this(new ThreadPoolSampler(pool))
  }
  
  /**
   * Record a reader size for tracking distribution.
   * Call this for every reader, whether split or not.
   */
  def recordReaderSize(size: Long): Unit = sizeHistoryLock.synchronized {
    sizeHistory(sizeHistoryIndex) = size
    sizeHistoryIndex = (sizeHistoryIndex + 1) % sizeHistoryLength
    if (sizeHistoryCount < sizeHistoryLength) {
      sizeHistoryCount += 1
    }
  }
  
  /**
   * Get average reader size from recent history.
   * Returns 0 if no history yet.
   */
  def getAverageReaderSize: Long = sizeHistoryLock.synchronized {
    if (sizeHistoryCount == 0) {
      0L
    } else {
      var sum = 0L
      for (i <- 0 until sizeHistoryCount) {
        sum += sizeHistory(i)
      }
      sum / sizeHistoryCount
    }
  }
  
  /**
   * Check if a reader is "large" enough to benefit from splitting.
   * A reader is considered large if it's >= 1.5x the average size,
   * or if we don't have enough history yet.
   */
  def isLargeReader(size: Long): Boolean = {
    val avgSize = getAverageReaderSize
    // If no history or very small average, consider all readers above MIN_PARALLEL_SIZE as large
    if (avgSize < ParallelFileIO.MIN_PARALLEL_SIZE) {
      size >= ParallelFileIO.MIN_PARALLEL_SIZE * 2
    } else {
      // Large = at least 1.5x average
      size >= avgSize * 3 / 2
    }
  }
  
  /**
   * Get available slots for splitting (after reserving some).
   * 
   * @return Number of slots available for new split tasks
   */
  def getAvailableSlots: Int = {
    val currentActiveCount = sampler.getActiveCount
    val currentQueueSize = sampler.getQueueSize
    val currentIdle = math.max(0, numThreads - currentActiveCount - currentQueueSize)
    
    // Reserve some slots as buffer
    val reserve = math.max(minReserve, (numThreads * reserveRatio).toInt)
    math.max(0, currentIdle - reserve)
  }
  
  /**
   * Get current idle slots (without reserve).
   */
  def getIdleSlots: Int = {
    val currentActiveCount = sampler.getActiveCount
    val currentQueueSize = sampler.getQueueSize
    math.max(0, numThreads - currentActiveCount - currentQueueSize)
  }
  
  /**
   * Alias for getAvailableSlots for the splitting decision.
   */
  def predictIdleSlots: Int = getAvailableSlots
  
  /**
   * Get the total number of threads in the pool.
   */
  def getNumThreads: Int = numThreads
  
  /**
   * Get current active thread count.
   */
  def getActiveCount: Int = sampler.getActiveCount
  
  /**
   * Get current queue size.
   */
  def getQueueSize: Int = sampler.getQueueSize
  
  /**
   * Reset state (for testing).
   */
  def reset(): Unit = sizeHistoryLock.synchronized {
    sizeHistoryIndex = 0
    sizeHistoryCount = 0
    java.util.Arrays.fill(sizeHistory, 0L)
  }
  
  /**
   * No-op (no background threads).
   */
  override def close(): Unit = {}
}

/**
 * Parallel file I/O utility for maximizing storage bandwidth utilization.
 * 
 * When reading large amounts of data from cloud storage (e.g., GCS), using multiple
 * parallel connections can significantly improve throughput. For example, GCS can
 * achieve ~2.2GB/s with 30+ parallel threads vs ~100MB/s single-threaded.
 * 
 * This utility provides methods to read multiple byte ranges in parallel, using
 * available idle threads from the file reader thread pool.
 * 
 * NOTE: Parallel I/O is only enabled when explicitly requested (enableParallel=true),
 * which is intended for cloud reader scenarios only.
 */
object ParallelFileIO extends Logging {
  
  /**
   * Minimum size for parallel I/O (10MB).
   * Smaller reads don't benefit much from parallelization.
   */
  val MIN_PARALLEL_SIZE: Long = 10L * 1024 * 1024
  
  /**
   * Default read buffer size for each thread (64KB).
   */
  val READ_BUFFER_SIZE: Int = 64 * 1024
  
  // Configuration key for ASIO enable/disable
  val ASIO_ENABLED_KEY = "spark.rapids.sql.multiThreadedRead.asio.enabled"
  
  // Global load predictor, lazily initialized when pool is available
  @volatile private var loadPredictor: LoadPredictor = _
  
  // ASIO statistics for metrics (thread-safe counters)
  private val parallelReadCount = new AtomicLong(0)
  private val sequentialReadCount = new AtomicLong(0)
  private val totalBytesParallel = new AtomicLong(0)
  private val totalSplitCount = new AtomicLong(0)
  private val lastPoolActiveThreads = new AtomicLong(0)
  private val lastPoolQueueSize = new AtomicLong(0)
  
  /**
   * Check if ASIO is enabled from configuration.
   * Tries to read from SQLConf, defaults to true if unavailable.
   */
  def isAsioEnabled: Boolean = {
    Try {
      val conf = SQLConf.get
      conf.getConfString(ASIO_ENABLED_KEY, "true").toBoolean
    }.getOrElse(true)  // Default to enabled if can't read config
  }
  
  /**
   * Get ASIO statistics for metrics reporting.
   */
  def getStatistics: AsioStatistics = AsioStatistics(
    parallelReads = parallelReadCount.get(),
    sequentialReads = sequentialReadCount.get(),
    totalBytesParallel = totalBytesParallel.get(),
    totalSplits = totalSplitCount.get(),
    poolActiveThreads = lastPoolActiveThreads.get(),
    poolQueueSize = lastPoolQueueSize.get()
  )
  
  /**
   * Reset statistics (mainly for testing).
   */
  def resetStatistics(): Unit = {
    parallelReadCount.set(0)
    sequentialReadCount.set(0)
    totalBytesParallel.set(0)
    totalSplitCount.set(0)
    lastPoolActiveThreads.set(0)
    lastPoolQueueSize.set(0)
  }
  
  /**
   * Get or create load predictor for the given pool.
   * The predictor automatically samples pool state periodically.
   */
  private def getLoadPredictor(pool: PriorityAwareFileReaderThreadPool): LoadPredictor = {
    if (loadPredictor == null) {
      synchronized {
        if (loadPredictor == null) {
          loadPredictor = new LoadPredictor(pool)
        }
      }
    }
    loadPredictor
  }
  
  /**
   * Shutdown the load predictor (should be called on application shutdown).
   */
  def shutdownPredictor(): Unit = {
    if (loadPredictor != null) {
      loadPredictor.close()
      loadPredictor = null
    }
  }
  
  /**
   * Read multiple byte ranges from a file in parallel.
   * 
   * If enableParallel is false, or there's insufficient idle capacity, 
   * or total size is too small, falls back to sequential reading.
   * 
   * @param filePath Path to the file
   * @param ranges Sequence of (fileOffset, bufferOffset, length) to read
   * @param targetBuffer Pre-allocated buffer to write data into
   * @param conf Hadoop configuration
   * @param enableParallel Whether to enable parallel I/O (default false)
   * @param pool Optional thread pool to use (uses global pool if not provided)
   * @return Total bytes read
   */
  def readRangesParallel(
      filePath: Path,
      ranges: Seq[(Long, Long, Long)],  // (fileOffset, bufferOffset, length)
      targetBuffer: HostMemoryBuffer,
      conf: Configuration,
      enableParallel: Boolean = false,
      pool: Option[PriorityAwareFileReaderThreadPool] = None): Long = {
    
    if (ranges.isEmpty) {
      return 0L
    }
    
    // Check ASIO global enable switch
    val asioEnabled = isAsioEnabled
    
    // If parallel not enabled (either by caller or global config), use sequential
    if (!enableParallel || !asioEnabled) {
      sequentialReadCount.incrementAndGet()
      return doSequentialRead(filePath, ranges, targetBuffer, conf)
    }
    
    val totalSize = ranges.map(_._3).sum
    val actualPool = pool.orElse(getGlobalPool)
    
    // Check if parallel I/O is beneficial
    actualPool match {
      case Some(p) if shouldUseParallel(p, totalSize) =>
        doParallelRead(filePath, ranges, targetBuffer, conf, p)
      case _ =>
        sequentialReadCount.incrementAndGet()
        doSequentialRead(filePath, ranges, targetBuffer, conf)
    }
  }
  
  /**
   * Get the global PriorityAwareFileReaderThreadPool if available.
   */
  private def getGlobalPool: Option[PriorityAwareFileReaderThreadPool] = {
    // Try to get from global pool
    // This is a bit hacky, but avoids passing pool through all layers
    try {
      val field = PriorityAwareFileReaderThreadPool.getClass
        .getDeclaredField("globalPool")
      field.setAccessible(true)
      field.get(PriorityAwareFileReaderThreadPool)
        .asInstanceOf[Option[PriorityAwareFileReaderThreadPool]]
    } catch {
      case _: Exception => None
    }
  }
  
  /**
   * Determine if parallel I/O should be used.
   * 
   * Conditions for parallel read:
   * 1. Size >= 2 * MIN_PARALLEL_SIZE (enough data to split)
   * 2. Available slots >= 2 (after reserving buffer)
   * 3. This reader is "large" relative to recent average (worth splitting)
   */
  private def shouldUseParallel(
      pool: PriorityAwareFileReaderThreadPool, 
      totalSize: Long): Boolean = {
    // Need enough data to make parallel worthwhile
    if (totalSize < MIN_PARALLEL_SIZE * 2) {
      return false
    }
    
    val predictor = getLoadPredictor(pool)
    
    // Record size for distribution tracking (always, even if not splitting)
    predictor.recordReaderSize(totalSize)
    
    // Check if we have available slots (after reserve)
    val availableSlots = predictor.getAvailableSlots
    if (availableSlots < 2) {
      return false
    }
    
    // Only split if this reader is large relative to average
    // Small readers don't benefit much from splitting
    predictor.isLargeReader(totalSize)
  }
  
  /**
   * Perform parallel read using multiple threads.
   * Uses LoadPredictor to determine optimal split count.
   * 
   * The current thread also executes one range group to avoid wasting it
   * while waiting for pool threads.
   */
  private def doParallelRead(
      filePath: Path,
      ranges: Seq[(Long, Long, Long)],
      targetBuffer: HostMemoryBuffer,
      conf: Configuration,
      pool: PriorityAwareFileReaderThreadPool): Long = {
    
    val tc = TaskContext.get()
    val totalSize = ranges.map(_._3).sum
    
    // Record pool state for metrics
    val activeCount = pool.getActiveCount
    val queueSize = pool.getQueue.size()
    lastPoolActiveThreads.set(activeCount)
    lastPoolQueueSize.set(queueSize)
    
    // Get available slots (after reserve buffer)
    val predictor = getLoadPredictor(pool)
    val availableSlots = math.max(2, predictor.getAvailableSlots)
    
    val maxSplitsBySize = (totalSize / MIN_PARALLEL_SIZE).toInt
    // +1 because current thread will handle one group itself
    val splitCount = math.min(availableSlots + 1, math.max(1, maxSplitsBySize))
    
    if (splitCount <= 1) {
      sequentialReadCount.incrementAndGet()
      return doSequentialRead(filePath, ranges, targetBuffer, conf)
    }
    
    // Record statistics for metrics
    parallelReadCount.incrementAndGet()
    totalBytesParallel.addAndGet(totalSize)
    totalSplitCount.addAndGet(splitCount)
    
    logDebug(s"ASIO parallel read for $filePath: ${ranges.size} ranges, " +
      s"total ${totalSize / (1024 * 1024)}MB, $splitCount splits (1 local + ${splitCount - 1} pool), " +
      s"available slots: $availableSlots, avg reader size: ${predictor.getAverageReaderSize / (1024 * 1024)}MB")
    
    // Group ranges into splits
    val rangeGroups = distributeRanges(ranges, splitCount)
    
    // First group is for current thread, rest go to pool
    val (localGroup, poolGroups) = (rangeGroups.head, rangeGroups.tail)
    
    // Submit pool tasks
    val futures = new ArrayBuffer[Future[AsyncResult[Long]]]()
    val errorRef = new AtomicReference[Throwable]()
    val bytesReadTotal = new AtomicLong(0)
    
    poolGroups.foreach { group =>
      val runner = new MultiRangeReadRunner(filePath, group, targetBuffer, conf, tc)
      val future = pool.submitRunner(runner)
      futures += future
    }
    
    // Current thread executes its own group - don't waste it waiting!
    try {
      val localBytes = doSequentialReadForRanges(filePath, localGroup, targetBuffer, conf)
      bytesReadTotal.addAndGet(localBytes)
    } catch {
      case e: Throwable =>
        errorRef.compareAndSet(null, e)
    }
    
    // Wait for pool tasks to complete
    futures.foreach { future =>
      try {
        val result = future.get()
        bytesReadTotal.addAndGet(result.data)
      } catch {
        case e: Throwable =>
          if (errorRef.compareAndSet(null, e)) {
            logError(s"Parallel read failed for $filePath: $e")
          }
      }
    }
    
    // Check for errors
    val error = errorRef.get()
    if (error != null) {
      throw error
    }
    
    bytesReadTotal.get()
  }
  
  /**
   * Read a specific set of ranges sequentially (used for current thread's work).
   */
  private def doSequentialReadForRanges(
      filePath: Path,
      ranges: Seq[(Long, Long, Long)],
      targetBuffer: HostMemoryBuffer,
      conf: Configuration): Long = {
    
    val fs = filePath.getFileSystem(conf)
    val readBuffer = new Array[Byte](READ_BUFFER_SIZE)
    var totalBytesRead = 0L
    
    withResource(fs.open(filePath)) { in =>
      ranges.foreach { case (fileOffset, bufferOffset, length) =>
        in.seek(fileOffset)
        var remaining = length
        var bufPos = bufferOffset
        
        while (remaining > 0) {
          val toRead = math.min(remaining, readBuffer.length).toInt
          val n = in.read(readBuffer, 0, toRead)
          if (n < 0) {
            throw new java.io.EOFException(
              s"Unexpected EOF reading $filePath at offset ${fileOffset + (length - remaining)}")
          }
          targetBuffer.setBytes(bufPos, readBuffer, 0, n)
          bufPos += n
          remaining -= n
          totalBytesRead += n
        }
      }
    }
    
    totalBytesRead
  }
  
  /**
   * Distribute ranges across splits, trying to balance total bytes.
   * 
   * If there's only one large range, it will be split at byte level.
   * Otherwise, ranges are grouped to achieve balanced distribution.
   */
  private def distributeRanges(
      ranges: Seq[(Long, Long, Long)],
      splitCount: Int): Seq[Seq[(Long, Long, Long)]] = {
    
    val totalSize = ranges.map(_._3).sum
    val targetPerSplit = math.max(MIN_PARALLEL_SIZE, totalSize / splitCount)
    
    // Special case: single large range - split at byte level
    if (ranges.size == 1) {
      val (fileOffset, bufferOffset, length) = ranges.head
      return splitSingleRange(fileOffset, bufferOffset, length, splitCount)
    }
    
    val groups = new ArrayBuffer[Seq[(Long, Long, Long)]]()
    var currentGroup = new ArrayBuffer[(Long, Long, Long)]()
    var currentSize = 0L
    
    ranges.foreach { range =>
      currentGroup += range
      currentSize += range._3
      
      if (currentSize >= targetPerSplit && groups.size < splitCount - 1) {
        groups += currentGroup.toSeq
        currentGroup = new ArrayBuffer[(Long, Long, Long)]()
        currentSize = 0L
      }
    }
    
    // Add remaining
    if (currentGroup.nonEmpty) {
      groups += currentGroup.toSeq
    }
    
    groups.toSeq
  }
  
  /**
   * Split a single large range into multiple sub-ranges at byte level.
   */
  private def splitSingleRange(
      fileOffset: Long,
      bufferOffset: Long,
      length: Long,
      splitCount: Int): Seq[Seq[(Long, Long, Long)]] = {
    
    val chunkSize = math.max(MIN_PARALLEL_SIZE, length / splitCount)
    val groups = new ArrayBuffer[Seq[(Long, Long, Long)]]()
    var remaining = length
    var currentFileOffset = fileOffset
    var currentBufferOffset = bufferOffset
    
    while (remaining > 0 && groups.size < splitCount) {
      val thisChunkSize = if (groups.size == splitCount - 1) {
        // Last chunk gets all remaining
        remaining
      } else {
        math.min(chunkSize, remaining)
      }
      
      groups += Seq((currentFileOffset, currentBufferOffset, thisChunkSize))
      currentFileOffset += thisChunkSize
      currentBufferOffset += thisChunkSize
      remaining -= thisChunkSize
    }
    
    groups.toSeq
  }
  
  /**
   * Perform sequential read (fallback). Delegates to doSequentialReadForRanges.
   */
  private def doSequentialRead(
      filePath: Path,
      ranges: Seq[(Long, Long, Long)],
      targetBuffer: HostMemoryBuffer,
      conf: Configuration): Long = {
    doSequentialReadForRanges(filePath, ranges, targetBuffer, conf)
  }
}

/**
 * AsyncRunner that reads multiple byte ranges from a file.
 * Used for parallel I/O when there's idle thread pool capacity.
 * 
 * @param filePath Path to the file
 * @param ranges Byte ranges to read: (fileOffset, bufferOffset, length)
 * @param targetBuffer Buffer to write data into
 * @param conf Hadoop configuration
 * @param tc Spark task context
 */
class MultiRangeReadRunner(
    filePath: Path,
    ranges: Seq[(Long, Long, Long)],  // (fileOffset, bufferOffset, length)
    targetBuffer: HostMemoryBuffer,
    conf: Configuration,
    tc: TaskContext) extends AsyncRunner[Long] with Logging {
  
  override def sparkTaskContext: Option[TaskContext] = Option(tc)
  
  override def resource: AsyncRunResource = AsyncRunResource.newCpuResource(0L)
  
  override def priority: Long = {
    sparkTaskContext.map(ctx =>
      com.nvidia.spark.rapids.jni.TaskPriority.getTaskPriority(ctx.taskAttemptId())
    ).getOrElse(0L)
  }
  
  override protected def buildResult(resultData: Long, metrics: AsyncMetrics): AsyncResult[Long] = {
    new FastReleaseResult[Long](resultData, metrics)
  }
  
  override protected def callImpl(): Long = {
    val fs = filePath.getFileSystem(conf)
    val readBuffer = new Array[Byte](ParallelFileIO.READ_BUFFER_SIZE)
    var totalBytesRead = 0L
    
    withResource(fs.open(filePath)) { in =>
      ranges.foreach { case (fileOffset, bufferOffset, length) =>
        in.seek(fileOffset)
        var remaining = length
        var bufPos = bufferOffset
        
        while (remaining > 0) {
          val toRead = math.min(remaining, readBuffer.length).toInt
          val n = in.read(readBuffer, 0, toRead)
          if (n < 0) {
            throw new java.io.EOFException(
              s"Unexpected EOF reading $filePath at offset ${fileOffset + (length - remaining)}")
          }
          targetBuffer.setBytes(bufPos, readBuffer, 0, n)
          bufPos += n
          remaining -= n
          totalBytesRead += n
        }
      }
    }
    
    logDebug(s"MultiRangeReadRunner completed: $filePath, " +
      s"${ranges.size} ranges, $totalBytesRead bytes")
    totalBytesRead
  }
}

/**
 * ASIO statistics for metrics reporting.
 * 
 * @param parallelReads Number of read operations that used parallel I/O
 * @param sequentialReads Number of read operations that fell back to sequential
 * @param totalBytesParallel Total bytes read via parallel I/O
 * @param totalSplits Total number of split tasks created
 * @param poolActiveThreads Last observed active thread count in pool
 * @param poolQueueSize Last observed queue size in pool
 */
case class AsioStatistics(
    parallelReads: Long,
    sequentialReads: Long,
    totalBytesParallel: Long,
    totalSplits: Long,
    poolActiveThreads: Long,
    poolQueueSize: Long) {
  
  /**
   * Get a human-readable summary string.
   */
  def summary: String = {
    val totalReads = parallelReads + sequentialReads
    val parallelRatio = if (totalReads > 0) parallelReads * 100.0 / totalReads else 0.0
    val avgSplitsPerParallel = if (parallelReads > 0) totalSplits.toDouble / parallelReads else 0.0
    val avgBytesPerParallel = if (parallelReads > 0) totalBytesParallel / parallelReads else 0L
    
    f"""ASIO Statistics:
       |  Parallel reads: $parallelReads ($parallelRatio%.1f%%)
       |  Sequential reads: $sequentialReads
       |  Total bytes (parallel): ${totalBytesParallel / (1024 * 1024)}MB
       |  Avg splits per parallel read: $avgSplitsPerParallel%.1f
       |  Avg bytes per parallel read: ${avgBytesPerParallel / (1024 * 1024)}MB
       |  Pool active threads (last): $poolActiveThreads
       |  Pool queue size (last): $poolQueueSize""".stripMargin
  }
}

