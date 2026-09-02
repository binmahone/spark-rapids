/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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
package org.apache.spark.sql.rapids

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

/**
 * SparkListener that releases the RAPIDS GPU ShuffleBufferCatalog eagerly at
 * stage completion (ref count == 0) for single-job static-plan SQL executions,
 * instead of waiting for Spark's GC-triggered ContextCleaner.
 *
 * Background: when running RAPIDS UCX (or any GPU-resident) shuffle,
 * map-output device buffers are tracked in ShuffleBufferCatalog and only
 * freed when ShuffleManager.unregisterShuffle is invoked. The default path
 * relies on ContextCleaner.doCleanupShuffle which is triggered by GC of the
 * ShuffleDependency object and can be delayed until application shutdown.
 * Multi-stage queries (e.g. TPC-H Q2 5-way join) accumulate every stage's
 * map outputs on GPU until then, which can exhaust the RMM pool and OOM the
 * UCX shuffle server thread.
 *
 * Mechanism: track shuffle-to-consumer-stage refs at onJobStart, decrement
 * at onStageCompleted, and trigger cleanup (via BlockManagerMaster.removeShuffle
 * to broadcast to every executor) when ref hits zero -- but only for SQL
 * executions that are guaranteed to produce exactly one Spark job with a
 * static plan. Other executions defer cleanup to SparkListenerSQLExecutionEnd.
 *
 * Adapted from gluten branch 260414-mahone-ucx commit 1db5bd728. The
 * fine-grained per-shuffle classification mode from that branch is dropped
 * here because RAPIDS workloads we care about (TPC-H benchmark) are pure
 * SELECT with AQE disabled; coarse classification is sufficient.
 *
 * Driver-only. Cleanup is propagated to executors via
 * BlockManagerMaster.removeShuffle (which RPCs to every BlockManager;
 * BlockManagerStorageEndpoint.receive(RemoveShuffle) then invokes
 * SparkEnv.get.shuffleManager.unregisterShuffle, i.e.
 * RapidsShuffleInternalManagerBase.unregisterShuffle, which calls the
 * GpuShuffleEnv catalog's unregisterShuffle).
 *
 * Placed in org.apache.spark.sql.rapids package so it can access the
 * private[spark] StageInfo.shuffleDepId field.
 */
class GpuCatalogCleanupListener extends SparkListener with Logging {

  // executionId -> shuffleIds the execution has touched. Used by the
  // defer-to-SQL-end fallback.
  private val executionShuffles =
    new ConcurrentHashMap[Long, mutable.Set[Int]]()

  // shuffleId -> remaining consumer-stage count.
  private val shuffleRefCount =
    new ConcurrentHashMap[Int, AtomicInteger]()

  // stageId -> shuffleIds this stage consumes. Removed on the first
  // onStageCompleted for the stage (subsequent attempts no-op).
  private val stageInputShuffles =
    new ConcurrentHashMap[Int, Set[Int]]()

  // stageId -> the shuffleId this stage produces, accumulated across jobs.
  private val globalProducerMap =
    new ConcurrentHashMap[Int, Int]()

  // executionIds classified eager-eligible. Populated at onJobStart.
  private val eagerCleanupExecutions =
    ConcurrentHashMap.newKeySet[Long]()

  // executionIds requiring defer-to-SQL-end (AQE on, multi-job op, or no
  // QueryExecution).
  private val deferredExecutions =
    ConcurrentHashMap.newKeySet[Long]()

  // shuffleId -> owning executionId. Lets onStageCompleted look up
  // eligibility without scanning every execution.
  private val shuffleToExecution =
    new ConcurrentHashMap[Int, Long]()

  // shuffleIds that have ever had their ref incremented. Used to
  // distinguish a ref==0 transition that is "first consumer about to read"
  // (no warning) from "shuffle already cleaned, now revived" (warning -- a
  // missed multiJobOpPatterns entry).
  private val everIncrementedShuffles =
    ConcurrentHashMap.newKeySet[Int]()

  // Class simple-name substrings whose presence in the executedPlan forces
  // defer-to-SQL-end. Empirically these produce multiple Spark Jobs sharing
  // intermediate shuffle outputs (gluten 260414-mahone-ucx q7 instrumentation
  // showed VeloxColumnarWriteFiles reviving shuffle 6); eager cleanup of any
  // such shuffle between jobs would force re-execution of upstream stages.
  // TPC-H SELECT-only workload matches none of these; the list is kept as
  // defense-in-depth.
  private val multiJobOpPatterns: Seq[String] = Seq(
    "VeloxColumnarWriteFiles",
    "InsertIntoHadoopFsRelation",
    "InsertIntoDataSource",
    "CreateDataSourceTableAsSelect",
    "CreateHiveTableAsSelect",
    "InsertIntoHiveTable",
    // Native (worker-to-worker via UCX shuffle) broadcast splits its work
    // across two Spark jobs: an explicit submitMapStage job that runs the
    // build-side shuffle write, followed by the main consumer job that reads
    // the build via getShuffleRDD. The eager listener must NOT reclaim the
    // build shuffle between these two jobs.
    "GpuShuffleBroadcastHashJoinExec"
  )

  /**
   * Decide eager-eligibility for `executionId` on demand. Looks up the live
   * QueryExecution via SQLExecution.getQueryExecution and inspects
   * executedPlan. Result cached in eagerCleanupExecutions or
   * deferredExecutions.
   */
  private def classifyExecution(executionId: Long): Unit = {
    if (eagerCleanupExecutions.contains(executionId) ||
        deferredExecutions.contains(executionId)) {
      return
    }
    val qe = SQLExecution.getQueryExecution(executionId)
    if (qe == null) {
      deferredExecutions.add(executionId)
      logInfo(s"GpuCatalogCleanup: SQL execution $executionId has no " +
        "QueryExecution; deferring cleanup to SQL execution end")
      return
    }
    val plan = qe.executedPlan
    val aqe = planContainsAqe(plan)
    val multiJob = planContainsMultiJobOp(plan)
    if (!aqe && !multiJob) {
      eagerCleanupExecutions.add(executionId)
      logInfo(s"GpuCatalogCleanup: SQL execution $executionId" +
        " eager stage-level cleanup enabled")
    } else {
      deferredExecutions.add(executionId)
      logInfo(s"GpuCatalogCleanup: SQL execution $executionId deferred " +
        s"(aqe=$aqe multiJob=$multiJob)")
    }
  }

  private def planContainsAqe(plan: SparkPlan): Boolean =
    plan.find(_.isInstanceOf[AdaptiveSparkPlanExec]).isDefined

  private def planContainsMultiJobOp(plan: SparkPlan): Boolean =
    plan
      .find(n => multiJobOpPatterns.exists(pat =>
        n.getClass.getSimpleName.contains(pat)))
      .isDefined

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val executionIdOpt = Option(jobStart.properties)
      .flatMap(p => Option(p.getProperty(SQLExecution.EXECUTION_ID_KEY)))
      .flatMap(s => scala.util.Try(s.toLong).toOption)

    // Update global producer map first so the consumer-side ref counting
    // pass below can resolve any cross-job dependency.
    for (si <- jobStart.stageInfos) {
      si.shuffleDepId.foreach(shufId => globalProducerMap.put(si.stageId, shufId))
    }

    executionIdOpt.foreach { executionId =>
      classifyExecution(executionId)

      val shuffleIds = jobStart.stageInfos.flatMap(_.shuffleDepId).toSet
      if (shuffleIds.nonEmpty) {
        executionShuffles.compute(executionId, (_, existing) => {
          val set =
            if (existing == null) mutable.Set[Int]()
            else existing
          set ++= shuffleIds
          set
        })
        shuffleIds.foreach(sid => shuffleToExecution.putIfAbsent(sid, executionId))
      }
    }

    // Build consumer-side ref counts for stage-level cleanup.
    for (si <- jobStart.stageInfos) {
      val consumed = si.parentIds
        .flatMap(pid => Option(globalProducerMap.get(pid)))
        .toSet
      if (consumed.nonEmpty) {
        stageInputShuffles.put(si.stageId, consumed)
        consumed.foreach { shufId =>
          val ref = shuffleRefCount.computeIfAbsent(shufId, _ => new AtomicInteger(0))
          val before = ref.getAndIncrement()
          // before > 0 means a second consumer stage starts to fetch this
          // shuffle (ReusedExchange pattern). Per-block eager release on the
          // map-side is unsafe for such shuffles; warn loudly so the user
          // can disable perBlockEagerRelease for this workload. Note: we
          // cannot reliably propagate a "mark this shuffle no-per-block" to
          // executors from this listener thread — Spark local properties
          // set in a listener don't reach jobs submitted from other driver
          // threads. Plan-time detection via ReusedExchangeExec scan inside
          // GpuShuffleBroadcastHashJoinExec handles the cases visible
          // through a native-broadcast subtree.
          if (before > 0) {
            logWarning(s"GpuCatalogCleanup: shuffle $shufId now has multiple " +
              s"consumer stages (stage ${si.stageId} is consumer #${before + 1}). " +
              "If spark.rapids.shuffle.gpuCatalog.perBlockEagerRelease.enabled " +
              "is on, the shuffle may have been already released per-block by " +
              "the first consumer's fetch path. Disable perBlockEagerRelease " +
              "for this workload, or rely on the native-broadcast plan-time " +
              "detection in GpuShuffleBroadcastHashJoinExec.")
          }
          val newlySeen = everIncrementedShuffles.add(shufId)
          if (before == 0 && !newlySeen) {
            val execIdOpt = Option(shuffleToExecution.get(shufId)).map(_.longValue())
            if (execIdOpt.exists(eagerCleanupExecutions.contains)) {
              logWarning(s"GpuCatalogCleanup: shuffle $shufId revived from " +
                s"ref=0 by new consumer stage ${si.stageId} " +
                s"(execution=${execIdOpt.get}); eager cleanup may have been " +
                "premature. Add the responsible plan node to multiJobOpPatterns.")
            }
          }
        }
      }
    }
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val stageId = event.stageInfo.stageId
    val consumed = Option(stageInputShuffles.remove(stageId))
    consumed.foreach { shuffleIds =>
      shuffleIds.foreach { shufId =>
        val ref = shuffleRefCount.get(shufId)
        if (ref != null) {
          val remaining = ref.decrementAndGet()
          if (remaining <= 0) {
            val execIdOpt = Option(shuffleToExecution.get(shufId)).map(_.longValue())
            val canEager = execIdOpt.exists(eagerCleanupExecutions.contains)
            if (canEager) {
              logInfo(s"GpuCatalogCleanup: shuffle $shufId ref=0 " +
                s"(stage=$stageId execution=${execIdOpt.get}) - eager cleanup")
              shuffleRefCount.remove(shufId)
              shuffleToExecution.remove(shufId)
              removeFromExecution(shufId)
              try {
                onCleanup(shufId)
              } catch {
                case e: Exception =>
                  logWarning(s"GpuCatalogCleanup: failed eager cleanup for " +
                    s"shuffle $shufId", e)
              }
            } else {
              logInfo(s"GpuCatalogCleanup: shuffle $shufId ref=0 " +
                s"(stage=$stageId execution=${execIdOpt.orNull}) - deferring " +
                "to SQL execution end")
            }
          }
        }
      }
    }
  }

  private def removeFromExecution(shuffleId: Int): Unit = {
    val iter = executionShuffles.values().iterator()
    while (iter.hasNext) {
      val set = iter.next()
      set -= shuffleId
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionEnd => onSQLExecutionEnd(e)
    case _ =>
  }

  private def onSQLExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    eagerCleanupExecutions.remove(event.executionId)
    deferredExecutions.remove(event.executionId)
    Option(executionShuffles.remove(event.executionId)).foreach { ids =>
      if (ids.nonEmpty) {
        logInfo(s"GpuCatalogCleanup: SQL execution ${event.executionId} ended, " +
          s"cleaning ${ids.size} remaining shuffle(s): ${ids.mkString(", ")}")
        ids.foreach { shuffleId =>
          shuffleRefCount.remove(shuffleId)
          shuffleToExecution.remove(shuffleId)
          try {
            onCleanup(shuffleId)
          } catch {
            case e: Exception =>
              logWarning(s"GpuCatalogCleanup: failed to clean shuffle " +
                s"$shuffleId", e)
          }
        }
      }
    }
  }

  /**
   * Called at end-of-application to clean any executions that never received
   * SparkListenerSQLExecutionEnd (e.g. failed query, abrupt shutdown). Mirrors
   * the existing ShuffleCleanupListener.shutdown contract so RapidsDriverPlugin
   * can invoke it during plugin shutdown.
   */
  def shutdown(): Unit = {
    val remaining = executionShuffles.entrySet().asScala.toSeq
    if (remaining.nonEmpty) {
      logInfo(s"GpuCatalogCleanup shutdown: ${remaining.size} execution(s) " +
        "still have pending shuffle cleanup")
      remaining.foreach { entry =>
        entry.getValue.foreach { shuffleId =>
          try {
            onCleanup(shuffleId)
          } catch {
            case e: Exception =>
              logWarning("GpuCatalogCleanup shutdown: failed to clean " +
                s"shuffle $shuffleId", e)
          }
        }
      }
    }
    executionShuffles.clear()
    shuffleRefCount.clear()
    stageInputShuffles.clear()
    globalProducerMap.clear()
    eagerCleanupExecutions.clear()
    deferredExecutions.clear()
    shuffleToExecution.clear()
    everIncrementedShuffles.clear()
  }

  /**
   * Propagate cleanup to every executor by going through Spark's standard
   * BlockManager remove-shuffle RPC. BlockManagerStorageEndpoint on each
   * executor invokes SparkEnv.get.shuffleManager.unregisterShuffle, which
   * for RAPIDS-managed shuffle is RapidsShuffleInternalManagerBase: that
   * call closes the GPU device buffers in ShuffleBufferCatalog.
   *
   * Overridable for testing.
   */
  protected def onCleanup(shuffleId: Int): Unit = {
    val env = SparkEnv.get
    if (env == null) {
      logWarning(s"GpuCatalogCleanup: SparkEnv null, cannot clean shuffle " +
        s"$shuffleId")
      return
    }
    // Non-blocking: cleanup is best-effort. A subsequent fetch that races
    // against eager removal would trigger Spark's normal fetch-failure
    // recovery (rerun the parent stage), which is correctness-preserving.
    env.blockManager.master.removeShuffle(shuffleId, blocking = false)
  }
}
