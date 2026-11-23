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

import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import ai.rapids.cudf.{Cuda, Rmm}
import com.nvidia.spark.rapids.{GpuDeviceManager, ShuffleCleanupManager}

import org.apache.spark.{SparkContext, SparkEnv, TaskContext}
import org.apache.spark.broadcast.ReferenceBroadcastSnapshot
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd,
  SparkListenerSQLExecutionStart}

/**
 * Explicit boundary for the reference benchmark's fresh, synchronous, uncached SQL actions.
 * This is not a general SQL-end cleanup policy: retained DataFrames, concurrent submissions,
 * cached RDDs and streaming iterators are outside its contract. Later use of a retired ordinary
 * broadcast fails instead of manufacturing a rows-only batch from cleared fields.
 */
object IndependentQueryBoundary {
  val EnabledKey = "spark.rapids.sql.benchmark.independentQueryCleanup.enabled"
  private val TokenKey = "rapids.benchmark.independentQueryBoundary"
  private var context: SparkContext = _
  private var listener: BoundaryListener = _
  private var token: Option[String] = None
  private var completedBefore = 0L
  private val executorReceipts = mutable.Map.empty[String, (String, String)]
  private val executorFailures = mutable.Map.empty[String, Throwable]

  private class BoundaryListener extends SparkListener {
    val active: mutable.Set[Long] = mutable.Set.empty
    var completed = 0L
    override def onOtherEvent(event: SparkListenerEvent): Unit = synchronized {
      event match {
        case e: SparkListenerSQLExecutionStart => active += e.executionId
        case e: SparkListenerSQLExecutionEnd =>
          active -= e.executionId
          completed += 1
        case _ =>
      }
    }
  }

  private def drain(sc: SparkContext): Unit = {
    sc.listenerBus.waitUntilEmpty(30000L)
    require(sc.statusTracker.getActiveJobIds().isEmpty, "A Spark job is still running")
    require(sc.getPersistentRDDs.isEmpty, "Cached RDDs are outside this benchmark contract")
    require(listener.synchronized(listener.active.isEmpty), "A SQL action is still running")
  }

  def begin(sc: SparkContext, next: String): Unit = synchronized {
    require(sc.getConf.getBoolean(EnabledKey, false), s"$EnabledKey must be explicitly enabled")
    require(next.matches("[A-Za-z0-9_-]+"), "Invalid benchmark boundary token")
    if (context ne sc) {
      require(token.isEmpty, "Another context owns the boundary")
      context = sc
      listener = new BoundaryListener
      sc.addSparkListener(listener)
    }
    drain(sc)
    require(token.isEmpty, "The previous independent SQL action has not been cleaned")
    token = Some(next)
    completedBefore = listener.synchronized(listener.completed)
  }

  /** Completes normal asynchronous UCX reclaim and retires ordinary broadcasts on each GPU. */
  def finish(sc: SparkContext, expected: String, executors: Int,
      slotsPerExecutor: Int): JList[String] = synchronized {
    require((context eq sc) && token.contains(expected), "Boundary context/token mismatch")
    drain(sc)
    require(listener.synchronized(listener.completed > completedBefore),
      "No complete SQL action was observed")
    val manager = ShuffleCleanupManager.get
    require(manager != null, "The reference profile requires UCX query-end reclaim")
    manager.awaitIndependentQueryReclaims(executors, 60L)
    sc.setLocalProperty(TokenKey, expected)
    try {
      val rows = sc.parallelize(0 until executors * slotsPerExecutor,
        executors * slotsPerExecutor).barrier().mapPartitions { _ =>
        Iterator(retireOnExecutor(expected))
      }.collect()
      require(rows.map(_._1).distinct.length == executors, "Cleanup missed an executor")
      drain(sc)
      token = None
      rows.groupBy(_._1).values.map(_.head._2).toList.asJava
    } finally {
      sc.setLocalProperty(TokenKey, null)
    }
  }

  private def retireOnExecutor(expected: String): (String, String) = synchronized {
    require(TaskContext.get().getLocalProperty(TokenKey) == expected,
      "No independent-query boundary was supplied")
    executorFailures.get(expected).foreach(throw _)
    try {
      executorReceipts.getOrElseUpdate(expected, retireOrdinaryBroadcasts())
    } catch {
      case NonFatal(error) =>
        executorFailures.update(expected, error)
        throw error
    }
  }

  private def retireOrdinaryBroadcasts(): (String, String) = {
    GpuDeviceManager.getDeviceId().foreach(Cuda.setDevice)
    Cuda.deviceSynchronize()
    val before = Rmm.getTotalBytesAllocated
    val broadcasts = ReferenceBroadcastSnapshot.takeOrdinaryGpuBroadcasts()
    val live = broadcasts.filterNot(_.isRetiredAfterIndependentQuery)
    val bytes = live.map(_.retireAfterIndependentQuery()).sum
    require(broadcasts.forall(_.isRetiredAfterIndependentQuery),
      "Ordinary broadcast retirement left owned resources")
    Cuda.deviceSynchronize()
    val after = Rmm.getTotalBytesAllocated
    val id = SparkEnv.get.executorId
    val result = s"""{"executor":"$id","ordinary_objects":${live.size},""" +
      s""""ordinary_gpu_bytes":$bytes,"rmm_before":$before,"rmm_after":$after}"""
    (id, result)
  }
}
