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

import scala.collection.JavaConverters._

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

/**
 * Releases RAPIDS GPU shuffle buffers at the SQL execution boundary rather
 * than waiting for GC-triggered ContextCleaner cleanup.
 *
 * A stage-completion listener cannot know that every future consumer of a
 * shuffle has already been submitted. Range-partition sampling and explicit
 * submitMapStage calls can create a later Spark job in the same SQL execution.
 * Cleaning at stage completion can therefore remove a live shuffle. The SQL
 * execution end event is the earliest boundary at which all jobs belonging to
 * the query have completed.
 *
 * Driver-only. Cleanup is propagated to every executor through Spark's
 * BlockManagerMaster.removeShuffle RPC. The RAPIDS shuffle manager then
 * unregisters the shuffle from its ShuffleBufferCatalog.
 */
class GpuCatalogCleanupListener extends SparkListener with Logging {

  private val executionShuffles =
    new ConcurrentHashMap[Long, java.util.Set[Int]]()

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val executionId = Option(jobStart.properties)
      .flatMap(p => Option(p.getProperty(SQLExecution.EXECUTION_ID_KEY)))
      .flatMap(s => scala.util.Try(s.toLong).toOption)
    val shuffleIds = jobStart.stageInfos.flatMap(_.shuffleDepId).toSet

    executionId.filter(_ => shuffleIds.nonEmpty).foreach { id =>
      val tracked = executionShuffles.computeIfAbsent(
        id, _ => ConcurrentHashMap.newKeySet[Int]())
      tracked.addAll(shuffleIds.asJava)
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionEnd => cleanupExecution(e.executionId)
    case _ =>
  }

  private def cleanupExecution(executionId: Long): Unit = {
    Option(executionShuffles.remove(executionId)).foreach { ids =>
      logInfo(s"GpuCatalogCleanup: SQL execution $executionId ended, " +
        s"cleaning ${ids.size} shuffle(s): ${ids.asScala.mkString(", ")}")
      ids.asScala.foreach(cleanupShuffle)
    }
  }

  private def cleanupShuffle(shuffleId: Int): Unit = {
    try {
      onCleanup(shuffleId)
    } catch {
      case e: Exception =>
        logWarning(s"GpuCatalogCleanup: failed to clean shuffle $shuffleId", e)
    }
  }

  /** Clean executions that did not receive a SQL execution end event. */
  def shutdown(): Unit = {
    val remaining = executionShuffles.keySet().asScala.toSeq
    if (remaining.nonEmpty) {
      logInfo(s"GpuCatalogCleanup shutdown: cleaning ${remaining.size} execution(s)")
      remaining.foreach(cleanupExecution)
    }
  }

  /** Propagate cleanup to every executor and wait for acknowledgements. */
  protected def onCleanup(shuffleId: Int): Unit = {
    Option(SparkEnv.get) match {
      case Some(env) => env.blockManager.master.removeShuffle(shuffleId, blocking = true)
      case None =>
        logWarning(s"GpuCatalogCleanup: SparkEnv null, cannot clean shuffle $shuffleId")
    }
  }
}
