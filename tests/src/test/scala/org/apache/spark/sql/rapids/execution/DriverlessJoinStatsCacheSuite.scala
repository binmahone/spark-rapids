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

import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

import ai.rapids.cudf.{ColumnVector, Rmm}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class DriverlessJoinStatsCacheSuite extends RmmSparkRetrySuiteBase {
  private def keys(index: Int = 0): Seq[GpuExpression] =
    Seq(GpuBoundReference(index, IntegerType, nullable = true)(ExprId(index), "key"))

  private def key(index: Int = 0, filter: Boolean = false): HashBuildKey =
    HashBuildKey.fromExpressions(Seq.empty, keys(index),
      compareNullsEqual = true, filterOutNulls = filter)

  private def owner(values: Array[java.lang.Integer]): StatsCachedBroadcastBatch = {
    withResource(ColumnVector.fromBoxedInts(values: _*)) { column =>
      val batch = new ColumnarBatch(Array(
        GpuColumnVector.from(column.incRefCount(), IntegerType)), values.length)
      new StatsCachedBroadcastBatch(
        SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
    }
  }

  private def stats(batch: ColumnarBatch, index: Int = 0,
      filter: Boolean = false): JoinBuildSideStats = {
    if (filter) {
      val spillable = SpillableColumnarBatch(GpuColumnVector.incRefCounts(batch),
        SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
      withResource(GpuHashJoin.filterNullsWithRetryAndClose(spillable, keys(index))) { filtered =>
        JoinBuildSideStats.fromBatch(filtered, keys(index))
      }
    } else {
      JoinBuildSideStats.fromBatch(batch, keys(index))
    }
  }

  test("borrowers reuse actual GPU statistics and invalidation cannot reuse stale uniqueness") {
    val allocated = Rmm.getTotalBytesAllocated
    withResource(new ExecutorBroadcastCache[SpillableColumnarBatch](
        owned => withResource(owned)(_ => ()))) { cache =>
      def read(values: Array[java.lang.Integer]): BorrowedBroadcastWithStats =
        cache.getOrBuild[ColumnarBatch](42, () => owner(values), () => (), () => ())(
          GpuDriverlessBroadcastHelper.borrow, b => withResource(b)(_ => ()))
          .asInstanceOf[BorrowedBroadcastWithStats]
      val unique = Array[java.lang.Integer](1, 2, 3)
      withResource(read(unique)) { first =>
        val provider = new StatsOnlyHashBackendProvider(first.joinStats, key(),
          NoopMetric, NoopMetric)
        assert(provider.readyStats.isEmpty)
        assert(provider.cacheStats(stats(first)).isDistinct)
        withResource(read(unique)) { second =>
          val reused = new StatsOnlyHashBackendProvider(second.joinStats, key(),
            NoopMetric, NoopMetric)
          assert(reused.readyStats.contains(JoinBuildSideStats(1.0, isDistinct = true)))
          assert(reused.cacheStats(fail("Recomputed unchanged build")).isDistinct)
        }
        cache.invalidate(42)
        withResource(read(Array[java.lang.Integer](1, 1, 2))) { replacement =>
          assert(replacement.joinStats.get(key()).isEmpty)
          val computed = replacement.joinStats.getOrCompute(key(), NoopMetric, NoopMetric) {
            stats(replacement)
          }
          assert(!computed.isDistinct && computed.streamMagnificationFactor == 1.5)
          assert(first.joinStats.get(key()).exists(_.isDistinct))
          assert(stats(first).isDistinct)
        }
      }
    }
    assert(Rmm.getTotalBytesAllocated == allocated)
  }

  test("different bound keys and null filtering have independent actual statistics") {
    withResource(ColumnVector.fromBoxedInts(1, 2, null, null)) { first =>
      withResource(ColumnVector.fromInts(1, 1, 2, 2)) { second =>
        val batch = new ColumnarBatch(Array(
          GpuColumnVector.from(first.incRefCount(), IntegerType),
          GpuColumnVector.from(second.incRefCount(), IntegerType)), 4)
        withResource(new StatsCachedBroadcastBatch(
            SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))) { cached =>
          withResource(cached.getColumnarBatch()) { borrowed =>
            val cache = borrowed.asInstanceOf[BorrowedBroadcastWithStats].joinStats
            def get(index: Int, filter: Boolean): JoinBuildSideStats =
              cache.getOrCompute(key(index, filter), NoopMetric, NoopMetric) {
                stats(borrowed, index, filter)
              }
            assert(!get(0, false).isDistinct)
            assert(get(0, true).isDistinct)
            assert(!get(1, true).isDistinct)
            assert(get(1, true).streamMagnificationFactor == 2.0)
            assert(!get(0, false).isDistinct)
          }
        }
        assert(first.getRefCount == 1 && second.getRefCount == 1)
      }
    }
  }

  test("failed computation publishes no state and permits a later real computation") {
    withResource(owner(Array[java.lang.Integer](3, 4))) { cached =>
      withResource(cached.getColumnarBatch()) { borrowed =>
        val cache = borrowed.asInstanceOf[BorrowedBroadcastWithStats].joinStats
        intercept[IllegalArgumentException] {
          cache.getOrCompute(key(), NoopMetric, NoopMetric) {
            throw new IllegalArgumentException("injected statistics failure")
          }
        }
        assert(cache.get(key()).isEmpty)
        assert(cache.getOrCompute(key(), NoopMetric, NoopMetric)(stats(borrowed)).isDistinct)
      }
    }
  }

  test("concurrent cold computations do not hold a shared lock across GPU work") {
    val cache = new DriverlessJoinStatsCache
    val entered = new CountDownLatch(2)
    val pool = Executors.newFixedThreadPool(2)
    try {
      val futures = (0 until 2).map { _ =>
        pool.submit(new Callable[JoinBuildSideStats] {
          override def call(): JoinBuildSideStats = {
            cache.getOrCompute(key(), NoopMetric, NoopMetric) {
              entered.countDown()
              assert(entered.await(10, TimeUnit.SECONDS))
              JoinBuildSideStats(1.0, isDistinct = true)
            }
          }
        })
      }
      futures.foreach(f => assert(f.get(15, TimeUnit.SECONDS).isDistinct))
      assert(cache.getOrCompute(key(), NoopMetric, NoopMetric) {
        fail("Published result must be reused")
      }.isDistinct)
    } finally {
      pool.shutdownNow()
      assert(pool.awaitTermination(15, TimeUnit.SECONDS))
    }
  }
}
