/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Executor-scoped cache of native-broadcast build batches. Each consumer
 * task on an executor would otherwise fetch the full build shuffle output
 * over UCX and materialise its own copy on the GPU; with K tasks per
 * executor this wastes K-1 build copies of GPU memory.
 *
 * The cache stores one SpillableColumnarBatch per build shuffleId. The
 * first task to call `getOrBuild` on a given shuffleId does the actual
 * shuffle fetch (via the supplied `build` thunk); subsequent tasks on
 * the same executor get a ColumnarBatch view of the cached spillable.
 *
 * Lifecycle: entries are dropped when the shuffleId is unregistered with
 * the shuffle manager (see RapidsShuffleInternalManagerBase
 * .unregisterGpuShuffle). The spillable's refCount transitions 1 -> 0 on
 * removal and its underlying device buffer is freed at that point.
 *
 * Caller contract: the returned ColumnarBatch is owned by the caller and
 * must be closed exactly once. The SpillableColumnarBatch held by the
 * cache is unaffected by the caller's close — only `remove(shuffleId)`
 * frees the underlying storage.
 */
package org.apache.spark.sql.rapids.execution

import java.util.concurrent.ConcurrentHashMap

import com.nvidia.spark.rapids.{SpillableColumnarBatch, SpillPriorities}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuShuffleBroadcastBuildCache extends Logging {

  // shuffleId -> spillable build batch (one per executor)
  private val cache = new ConcurrentHashMap[Int, SpillableColumnarBatch]()

  /**
   * Get a ColumnarBatch view of the build side for the given shuffleId,
   * computing it via `build` on first call. Caller owns the returned
   * ColumnarBatch and must close it; the cache holds its own reference to
   * the underlying SpillableColumnarBatch.
   *
   * @param shuffleId  build shuffleId (must be unique per native-broadcast)
   * @param build      thunk to fetch + assemble the full build ColumnarBatch
   *                   when this is the first call for `shuffleId`
   */
  def getOrBuild(shuffleId: Int, build: () => ColumnarBatch): ColumnarBatch = {
    var firstBuild = false
    val cached = cache.computeIfAbsent(shuffleId, _ => {
      firstBuild = true
      val builtBatch = build()
      val spillable = SpillableColumnarBatch(builtBatch,
        SpillPriorities.ACTIVE_BATCHING_PRIORITY)
      spillable
    })
    if (firstBuild) {
      logInfo(s"GpuShuffleBroadcastBuildCache: built cache entry for shuffle " +
        s"$shuffleId, rows=${cached.numRows()}")
    }
    cached.getColumnarBatch()
  }

  /**
   * Drop the cache entry for `shuffleId`. Called by the shuffle manager
   * when the shuffle is unregistered (job end or explicit cleanup). The
   * SpillableColumnarBatch's last refCount is released here, freeing the
   * underlying device buffer.
   */
  def remove(shuffleId: Int): Unit = {
    val removed = cache.remove(shuffleId)
    if (removed != null) {
      logInfo(s"GpuShuffleBroadcastBuildCache: removing cache entry for shuffle $shuffleId")
      removed.close()
    }
  }

  /** Visible for testing. */
  def size: Int = cache.size()
}
