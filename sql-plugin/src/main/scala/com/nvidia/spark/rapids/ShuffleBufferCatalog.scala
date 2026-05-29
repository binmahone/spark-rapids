/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.function.{Consumer, IntUnaryOperator}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ContiguousTable, Cuda, DeviceMemoryBuffer, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.format.TableMeta
import com.nvidia.spark.rapids.spill.{SpillableDeviceBufferHandle, SpillableHandle}

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.ShuffleBlockId

/** Identifier for a shuffle buffer that holds the data for a table */
case class ShuffleBufferId(
    blockId: ShuffleBlockId,
    tableId: Int) {
  val shuffleId: Int = blockId.shuffleId
  val mapId: Long = blockId.mapId
}

/** Catalog for lookup of shuffle buffers by block ID */
class ShuffleBufferCatalog extends Logging {
  /**
   * Information stored for each active shuffle.
   * A shuffle block can be comprised of multiple batches. Each batch
   * is given a `ShuffleBufferId`.
   */
  private type ShuffleInfo =
    ConcurrentHashMap[ShuffleBlockId, ArrayBuffer[ShuffleBufferId]]

  private val bufferIdToHandle =
    new ConcurrentHashMap[
      ShuffleBufferId,
      (Option[SpillableDeviceBufferHandle], TableMeta)]()

  /** shuffle information for each active shuffle */
  private[this] val activeShuffles = new ConcurrentHashMap[Int, ShuffleInfo]

  /** Mapping of table ID to shuffle buffer ID */
  private[this] val tableMap = new ConcurrentHashMap[Int, ShuffleBufferId]

  /** Tracks the next table identifier */
  private[this] val tableIdCounter = new AtomicInteger(0)

  // ===========================================================================
  // INSTRUMENTATION for per-block release diagnostics. INFO-level logging adds
  // ~1 line per shuffle block add/remove; in steady state this is one log line
  // per ~MB of shuffle traffic, which is small relative to the work being done.
  // Counters are atomic so all paths (caching writer, UCX server send-complete,
  // stage-end cleanup) share a consistent view.
  // ===========================================================================
  private[this] val cumulativeAddBytes = new AtomicLong(0)
  private[this] val cumulativeAddCount = new AtomicLong(0)
  // Additional metrics to triangulate true catalog footprint vs reported
  // buff.getLength (the shared cuDF parent buffer length, which over-counts
  // when ContiguousTable is a slice of a contiguousSplit result).
  private[this] val cumulativeAddRows = new AtomicLong(0)
  private[this] val cumulativeUncompressedSize = new AtomicLong(0)
  private[this] val cumulativePerBlockRemoveBytes = new AtomicLong(0)
  private[this] val cumulativePerBlockRemoveCount = new AtomicLong(0)
  private[this] val cumulativeUnregisterBytes = new AtomicLong(0)
  private[this] val cumulativeUnregisterCount = new AtomicLong(0)
  // ShuffleBufferId -> recorded byte length, captured at add time so remove
  // path knows what to deduct from the running totals.
  private[this] val bufferIdToSize = new ConcurrentHashMap[ShuffleBufferId, Long]()
  // per-shuffleId live bytes (add - remove). Lets us see which shuffle's
  // output dominates at any point in time.
  private[this] val perShuffleLiveBytes =
    new ConcurrentHashMap[Int, AtomicLong]()
  private[this] val perShuffleBlockCount =
    new ConcurrentHashMap[Int, AtomicLong]()

  private def logBytes(b: Long): String = f"${b.toDouble / (1024 * 1024)}%.1f MB"

  private def perShuffleBreakdown(): String = {
    import scala.collection.JavaConverters._
    val pairs = perShuffleLiveBytes.entrySet().asScala
      .map(e => (e.getKey, e.getValue.get(),
        Option(perShuffleBlockCount.get(e.getKey)).map(_.get()).getOrElse(0L)))
      .toSeq.sortBy(-_._2)
    pairs.map { case (sid, b, c) =>
      s"sid=$sid:${logBytes(b)}/${c}blk"
    }.mkString(" ")
  }

  private def recordAdd(
      bufferId: ShuffleBufferId,
      bufLen: Long,
      rowCount: Long,
      uncompressedSize: Long): Unit = {
    bufferIdToSize.put(bufferId, bufLen)
    val addCount = cumulativeAddCount.incrementAndGet()
    val addBytes = cumulativeAddBytes.addAndGet(bufLen)
    val addRows = cumulativeAddRows.addAndGet(rowCount)
    val addUncomp = cumulativeUncompressedSize.addAndGet(uncompressedSize)
    perShuffleLiveBytes
      .computeIfAbsent(bufferId.shuffleId, _ => new AtomicLong(0L))
      .addAndGet(bufLen)
    perShuffleBlockCount
      .computeIfAbsent(bufferId.shuffleId, _ => new AtomicLong(0L))
      .incrementAndGet()
    val live = addBytes - cumulativePerBlockRemoveBytes.get() -
      cumulativeUnregisterBytes.get()
    // every 100 adds, log all three size metrics for triangulation:
    //   cumAddBytes      = sum of buff.getLength (the cuDF parent buffer length)
    //   cumAddUncompSize = sum of TableMeta.bufferMeta().uncompressedSize()
    //   cumAddRows       = sum of contigTable.getRowCount() (slice's actual rows)
    // If catalog metric is 8x inflated, cumAddBytes >> cumAddUncompSize and
    // also >> cumAddRows × bytes_per_row. The right metric tells us true
    // catalog footprint.
    if (addCount % 100L == 0L) {
      logInfo(s"GpuShuffleCatalog: add count=$addCount " +
        s"cumAddBytes=${logBytes(addBytes)} " +
        s"cumAddUncomp=${logBytes(addUncomp)} " +
        s"cumAddRows=$addRows " +
        s"cumPerBlockReleased=${logBytes(cumulativePerBlockRemoveBytes.get())} " +
        s"cumUnregisterReleased=${logBytes(cumulativeUnregisterBytes.get())} " +
        s"liveBytes=${logBytes(live)} blocks=${bufferIdToSize.size()} " +
        s"per-shuffle=[${perShuffleBreakdown()}]")
    }
  }

  private def recordPerBlockRemove(bufferId: ShuffleBufferId): Unit = {
    val size: java.lang.Long = bufferIdToSize.remove(bufferId)
    if (size != null) {
      val rmCount = cumulativePerBlockRemoveCount.incrementAndGet()
      val rmBytes = cumulativePerBlockRemoveBytes.addAndGet(size.longValue())
      val perShuf = perShuffleLiveBytes.get(bufferId.shuffleId)
      if (perShuf != null) perShuf.addAndGet(-size.longValue())
      val perShufCount = perShuffleBlockCount.get(bufferId.shuffleId)
      if (perShufCount != null) perShufCount.decrementAndGet()
      if (rmCount % 100L == 0L) {
        val live = cumulativeAddBytes.get() - rmBytes -
          cumulativeUnregisterBytes.get()
        logInfo(s"GpuShuffleCatalog: per-block-remove count=$rmCount " +
          s"cumRemoved=${logBytes(rmBytes)} liveBytes=${logBytes(live)}")
      }
    }
  }

  private def recordUnregister(bufferIds: Seq[ShuffleBufferId]): Unit = {
    var totalSize = 0L
    var n = 0
    var shuffleId = -1
    bufferIds.foreach { id =>
      shuffleId = id.shuffleId
      val size: java.lang.Long = bufferIdToSize.remove(id)
      if (size != null) {
        totalSize += size.longValue()
        n += 1
      }
    }
    // wipe out per-shuffle counters for this shuffleId.
    if (shuffleId >= 0) {
      perShuffleLiveBytes.remove(shuffleId)
      perShuffleBlockCount.remove(shuffleId)
    }
    val urCount = cumulativeUnregisterCount.incrementAndGet()
    val urBytes = cumulativeUnregisterBytes.addAndGet(totalSize)
    val live = cumulativeAddBytes.get() -
      cumulativePerBlockRemoveBytes.get() - urBytes
    logInfo(s"GpuShuffleCatalog: unregister #$urCount shuffleId=$shuffleId " +
      s"blocks=$n freedBytes=${logBytes(totalSize)} " +
      s"cumUnregisterReleased=${logBytes(urBytes)} liveBytes=${logBytes(live)} " +
      s"per-shuffle-remaining=[${perShuffleBreakdown()}]")
  }

  private def trackCachedHandle(
      bufferId: ShuffleBufferId,
      handle: SpillableDeviceBufferHandle,
      meta: TableMeta): Unit = {
    bufferIdToHandle.put(bufferId, (Some(handle), meta))
  }

  private def trackDegenerate(bufferId: ShuffleBufferId,
                              meta: TableMeta): Unit = {
    bufferIdToHandle.put(bufferId, (None, meta))
  }

  def removeCachedHandles(): Unit = {
    val bufferIt = bufferIdToHandle.keySet().iterator()
    while (bufferIt.hasNext) {
      val buffer = bufferIt.next()
      val (maybeHandle, _) = bufferIdToHandle.remove(buffer)
      tableMap.remove(buffer.tableId)
      maybeHandle.foreach(_.close())
    }
  }

  /**
   * Adds a contiguous table shuffle table to the device storage. This does NOT take ownership of
   * the contiguous table, so it is the responsibility of the caller to close it.
   * The refcount of the underlying device buffer will be incremented so the contiguous table
   * can be closed before this buffer is destroyed.
   *
   * @param blockId              Spark's `ShuffleBlockId` that identifies this buffer
   * @param contigTable          contiguous table to track in storage
   * @param initialSpillPriority starting spill priority value for the buffer
   * @return RapidsBufferHandle identifying this table
   */
  def addContiguousTable(blockId: ShuffleBlockId,
                         contigTable: ContiguousTable,
                         initialSpillPriority: Long): Unit = {
    withResource(contigTable) { _ =>
      val bufferId = nextShuffleBufferId(blockId)
      val tableMeta = MetaUtils.buildTableMeta(bufferId.tableId, contigTable)
      val buff = contigTable.getBuffer
      buff.incRefCount()
      val handle = SpillableDeviceBufferHandle(buff, initialSpillPriority)
      trackCachedHandle(bufferId, handle, tableMeta)
      val uncomp = Option(tableMeta.bufferMeta())
        .map(_.uncompressedSize()).getOrElse(0L)
      recordAdd(bufferId, buff.getLength, contigTable.getRowCount, uncomp)
    }
  }

  /**
   * Adds a buffer to the device storage, taking ownership of the buffer.
   *
   * @param blockId              Spark's `ShuffleBlockId` that identifies this buffer
   * @param compressedBatch      Compressed ColumnarBatch
   * @param initialSpillPriority starting spill priority value for the buffer
   * @return RapidsBufferHandle associated with this buffer
   */
  def addCompressedBatch(
    blockId: ShuffleBlockId,
    compressedBatch: ColumnarBatch,
    initialSpillPriority: Long): Unit = {
    withResource(compressedBatch) { _ =>
      val bufferId = nextShuffleBufferId(blockId)
      val compressed = compressedBatch.column(0).asInstanceOf[GpuCompressedColumnVector]
      val tableMeta = compressed.getTableMeta
      // update the table metadata for the buffer ID generated above
      tableMeta.bufferMeta().mutateId(bufferId.tableId)
      val buff = compressed.getTableBuffer
      buff.incRefCount()
      val handle = SpillableDeviceBufferHandle(buff, initialSpillPriority)
      trackCachedHandle(bufferId, handle, tableMeta)
      val uncomp = Option(tableMeta.bufferMeta())
        .map(_.uncompressedSize()).getOrElse(0L)
      val rows = Option(tableMeta).map(_.rowCount()).getOrElse(0L)
      recordAdd(bufferId, buff.getLength, rows, uncomp)
    }
  }

  /**
   * Register a new buffer with the catalog. An exception will be thrown if an
   * existing buffer was registered with the same block ID (extremely unlikely)
   */
  def addDegenerateRapidsBuffer(
      blockId: ShuffleBlockId,
      meta: TableMeta): Unit = {
    val bufferId = nextShuffleBufferId(blockId)
    trackDegenerate(bufferId, meta)
  }

  /**
   * Register a new shuffle.
   * This must be called before any buffer identifiers associated with this shuffle can be tracked.
   * @param shuffleId shuffle identifier
   */
  def registerShuffle(shuffleId: Int): Unit = {
    activeShuffles.computeIfAbsent(shuffleId, _ => new ShuffleInfo)
  }

  /** Frees all buffers that correspond to the specified shuffle. */
  def unregisterShuffle(shuffleId: Int): Unit = {
    // This might be called on a background thread that has not set the device yet.
    GpuDeviceManager.getDeviceId().foreach(Cuda.setDevice)

    noPerBlockReleaseShuffles.remove(shuffleId)
    val info = activeShuffles.remove(shuffleId)
    if (info != null) {
      val freedIds = new ArrayBuffer[ShuffleBufferId]()
      val bufferRemover: Consumer[ArrayBuffer[ShuffleBufferId]] = { bufferIds =>
        // NOTE: Not synchronizing array buffer because this shuffle should be inactive.
        bufferIds.foreach { id =>
          tableMap.remove(id.tableId)
          val handleAndMeta = bufferIdToHandle.remove(id)
          // handleAndMeta may be null if removeShuffleBlockByTableId already
          // released this block via per-block release path.
          if (handleAndMeta != null) {
            handleAndMeta._1.foreach(_.close())
            freedIds += id
          }
        }
      }
      info.forEachValue(Long.MaxValue, bufferRemover)
      recordUnregister(freedIds.toSeq)
    } else {
      // currently shuffle unregister can get called on the driver which never saw a register
      if (!TrampolineUtil.isDriver(SparkEnv.get)) {
        logWarning(s"Ignoring unregister of unknown shuffle $shuffleId")
      }
    }
  }

  def hasActiveShuffle(shuffleId: Int): Boolean = activeShuffles.containsKey(shuffleId)

  // Shuffles that have been observed to be consumed by more than one stage
  // (e.g. via ReusedExchangeExec). For these, per-block release at UCX
  // send-complete is unsafe because a second consumer stage still needs to
  // fetch each block. Listener marks these at onJobStart; we fall back to
  // stage-level cleanup for them.
  private val noPerBlockReleaseShuffles =
    ConcurrentHashMap.newKeySet[Int]()

  /**
   * Mark a shuffle as ineligible for per-block release. Called by the cleanup
   * listener when it detects a second consumer stage for the shuffle.
   */
  def markNoPerBlockRelease(shuffleId: Int): Unit = {
    noPerBlockReleaseShuffles.add(shuffleId)
  }

  /**
   * Whether per-block release is allowed for the given shuffle. Returns false
   * for shuffles consumed by more than one stage (ReusedExchange etc.).
   */
  def canReleasePerBlock(shuffleId: Int): Boolean = {
    !noPerBlockReleaseShuffles.contains(shuffleId)
  }

  /**
   * Remove a single shuffle block by its table identifier after it has been
   * consumed (e.g. UCX send-complete). This is more eager than
   * unregisterShuffle which clears the entire shuffleId at once.
   *
   * No-op when canReleasePerBlock returns false for the owning shuffle
   * (typically when a ReusedExchange has marked the shuffle ineligible).
   * Caller passes the tableId of the consumed block.
   */
  def removeShuffleBlockByTableId(tableId: Int): Unit = {
    val shuffleBufferId = tableMap.get(tableId)
    if (shuffleBufferId == null) {
      // already removed, or this is a degenerate/unknown table id.
      return
    }
    if (!canReleasePerBlock(shuffleBufferId.shuffleId)) {
      // ReusedExchange or similar — leave the block alone, stage-end cleanup
      // will handle it once all consumer stages are done.
      return
    }
    // commit the removal (CAS-style on tableMap to avoid concurrent double-close).
    if (tableMap.remove(tableId, shuffleBufferId)) {
      val handleAndMeta = bufferIdToHandle.remove(shuffleBufferId)
      if (handleAndMeta != null) {
        handleAndMeta._1.foreach(_.close())
      }
      // remove from the ShuffleInfo entry so subsequent unregisterShuffle is
      // a no-op for this id.
      val info = activeShuffles.get(shuffleBufferId.shuffleId)
      if (info != null) {
        val entries = info.get(shuffleBufferId.blockId)
        if (entries != null) {
          entries.synchronized {
            entries -= shuffleBufferId
          }
        }
      }
      recordPerBlockRemove(shuffleBufferId)
    }
  }

  /** Get all the buffer IDs that correspond to a shuffle block identifier. */
  private def blockIdToBuffersIds(blockId: ShuffleBlockId): Array[ShuffleBufferId] = {
    val info = activeShuffles.get(blockId.shuffleId)
    if (info == null) {
      throw new NoSuchElementException(s"unknown shuffle ${blockId.shuffleId}")
    }
    val entries = info.get(blockId)
    if (entries == null) {
      throw new NoSuchElementException(s"unknown shuffle block $blockId")
    }
    entries.synchronized {
      entries.toArray
    }
  }

  def getColumnarBatchIterator(
    blockId: ShuffleBlockId,
    sparkTypes: Array[DataType]): Iterator[ColumnarBatch] = {
    val bufferIDs = blockIdToBuffersIds(blockId)
    bufferIDs.iterator.map { bId =>
      GpuSemaphore.acquireIfNecessary(TaskContext.get)
      val (maybeHandle, meta) = bufferIdToHandle.get(bId)
      maybeHandle.map { handle =>
        withResource(handle.materialize()) { buff =>
          val bufferMeta = meta.bufferMeta()
          if (bufferMeta == null || bufferMeta.codecBufferDescrsLength == 0) {
            MetaUtils.getBatchFromMeta(buff, meta, sparkTypes)
          } else {
            GpuCompressedColumnVector.from(buff, meta)
          }
        }
      }.getOrElse {
        // degenerate table (handle is None)
        // make a batch out of denegerate meta
        val rowCount = meta.rowCount
        val packedMeta = meta.packedMetaAsByteBuffer()
        if (packedMeta != null) {
          withResource(DeviceMemoryBuffer.allocate(0)) { deviceBuffer =>
            withResource(Table.fromPackedTable(
              meta.packedMetaAsByteBuffer(), deviceBuffer)) { table =>
              GpuColumnVectorFromBuffer.from(table, deviceBuffer, meta, sparkTypes)
            }
          }
        } else {
          // no packed metadata, must be a table with zero columns
          new ColumnarBatch(Array.empty, rowCount.toInt)
        }
      }
    }
  }

  /** Get all the buffer metadata that correspond to a shuffle block identifier. */
  def blockIdToMetas(blockId: ShuffleBlockId): Seq[TableMeta] = {
    val info = activeShuffles.get(blockId.shuffleId)
    if (info == null) {
      throw new NoSuchElementException(s"unknown shuffle ${blockId.shuffleId}")
    }
    val entries = info.get(blockId)
    if (entries == null) {
      throw new NoSuchElementException(s"unknown shuffle block $blockId")
    }
    entries.synchronized { 
      entries.map(bufferIdToHandle.get).map { case (_, meta) =>
        meta
      }
    }.toSeq
  }

  /** Allocate a new shuffle buffer identifier and update the shuffle block mapping. */
  private def nextShuffleBufferId(blockId: ShuffleBlockId): ShuffleBufferId = {
    val info = activeShuffles.get(blockId.shuffleId)
    if (info == null) {
      throw new IllegalStateException(s"unknown shuffle ${blockId.shuffleId}")
    }

    val tableId = tableIdCounter.getAndUpdate(ShuffleBufferCatalog.TABLE_ID_UPDATER)
    val id = ShuffleBufferId(blockId, tableId)
    val prev = tableMap.put(tableId, id)
    if (prev != null) {
      throw new IllegalStateException(s"table ID $tableId is already in use")
    }

    // associate this new buffer with the shuffle block
    val blockBufferIds = info.computeIfAbsent(blockId, _ =>
      new ArrayBuffer[ShuffleBufferId])
    blockBufferIds.synchronized {
      blockBufferIds.append(id)
    }
    id
  }

  /** Lookup the shuffle buffer handle that corresponds to the specified table identifier. */
  def getShuffleBufferHandle(tableId: Int): RapidsShuffleHandle = {
    val shuffleBufferId = tableMap.get(tableId)
    if (shuffleBufferId == null) {
      throw new NoSuchElementException(s"unknown table ID $tableId")
    }
    val (maybeHandle, meta) = bufferIdToHandle.get(shuffleBufferId)
    maybeHandle match {
      case Some(spillable) =>
        RapidsShuffleHandle(spillable, meta)
      case None =>
        throw new IllegalStateException(
          "a buffer handle could not be obtained for a degenerate buffer")
    }
  }

  /**
   * Update the spill priority of a shuffle buffer that soon will be read locally.
   * @param handle shuffle buffer handle of buffer to update
   */
  // TODO: AB: priorities
  //def updateSpillPriorityForLocalRead(handle: RapidsBufferHandle): Unit = {
  //  handle.setSpillPriority(SpillPriorities.INPUT_FROM_SHUFFLE_PRIORITY)
  //}

  /**
   * Remove a buffer and table given a buffer handle
   * NOTE: This function is not thread safe! The caller should only invoke if
   * the handle being removed is not being utilized by another thread.
   * @param handle buffer handle
   */
  def removeBuffer(handle: SpillableHandle): Unit = {
    handle.close()
  }
}

object ShuffleBufferCatalog {
  private val MAX_TABLE_ID = Integer.MAX_VALUE
  private val TABLE_ID_UPDATER = new IntUnaryOperator {
    override def applyAsInt(i: Int): Int = if (i < MAX_TABLE_ID) i + 1 else 0
  }
}
