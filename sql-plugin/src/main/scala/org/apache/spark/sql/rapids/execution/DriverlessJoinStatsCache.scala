/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
package org.apache.spark.sql.rapids.execution

import java.util.concurrent.ConcurrentHashMap

import com.nvidia.spark.rapids.{GpuMetric, SpillableColumnarBatch}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Immutable statistics belong to one cached source-output identity, not a shuffle ID alone. */
private[execution] final class DriverlessJoinStatsCache {
  private val values = new ConcurrentHashMap[HashBuildKey, JoinBuildSideStats]()

  def get(key: HashBuildKey): Option[JoinBuildSideStats] = Option(values.get(key))

  def getOrCompute(key: HashBuildKey, computations: GpuMetric, reuses: GpuMetric)(
      compute: => JoinBuildSideStats): JoinBuildSideStats = get(key) match {
    case Some(stats) =>
      reuses += 1
      stats
    case None =>
      // GPU work must not hold a cache lock or wait for another task's GPU semaphore.
      // Concurrent first users may compute the same immutable result independently.
      val stats = compute
      computations += 1
      Option(values.putIfAbsent(key, stats)).getOrElse(stats)
  }
}

/** Delegates GPU ownership and attaches a CPU-only statistics cache to each borrowed batch. */
private[execution] final class StatsCachedBroadcastBatch(underlying: SpillableColumnarBatch)
    extends SpillableColumnarBatch {
  private val stats = new DriverlessJoinStatsCache

  override def getColumnarBatch(): ColumnarBatch =
    closeOnExcept(underlying.getColumnarBatch()) { batch =>
      new BorrowedBroadcastWithStats(batch, stats)
    }

  override def numRows(): Int = underlying.numRows()
  override def sizeInBytes: Long = underlying.sizeInBytes
  override def dataTypes: Array[DataType] = underlying.dataTypes
  override def setSpillPriority(priority: Long): Unit = underlying.setSpillPriority(priority)
  override def incRefCount(): SpillableColumnarBatch = {
    underlying.incRefCount()
    this
  }
  override def close(): Unit = withResource(underlying)(_ => ())
}

/** The borrowed batch owns its columns; the shared statistics contain no GPU resources. */
private[execution] final class BorrowedBroadcastWithStats(
    underlying: ColumnarBatch,
    val joinStats: DriverlessJoinStatsCache)
    extends ColumnarBatch((0 until underlying.numCols()).map(underlying.column).toArray,
      underlying.numRows()) {
  override def close(): Unit = withResource(underlying)(_ => ())
}
