/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package org.apache.spark.sql.rapids

import java.lang.management.ManagementFactory
import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable

import ai.rapids.cudf.{Cuda, Rmm}
import com.nvidia.spark.rapids.{GpuDeviceManager, HostAlloc, ShuffleCleanupManager}
import com.nvidia.spark.rapids.benchmark.gc.BroadcastGcAgent

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.broadcast.{BroadcastGcSnapshot, ReferenceBroadcastSnapshot}
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

/** Quiescent independent-query diagnostic only; all arms retain normal UCX cleanup. */
object BroadcastGcBoundary {
  private val actions = mutable.Map.empty[String, String]
  private var context: SparkContext = _
  private val activeSql = mutable.Set.empty[Long]
  private val listener = new SparkListener {
    override def onOtherEvent(event: SparkListenerEvent): Unit = activeSql.synchronized {
      event match {
        case start: SparkListenerSQLExecutionStart => activeSql += start.executionId
        case end: SparkListenerSQLExecutionEnd => activeSql -= end.executionId
        case _ =>
      }
    }
  }

  private def oldCollections: Long = ManagementFactory.getGarbageCollectorMXBeans.asScala
    .filter(_.getName.contains("Old")).map(_.getCollectionCount).sum

  private def requestGc(phase: String): Unit = {
    BroadcastGcAgent.requestGc(phase)
  }

  def driverGc(phase: String): Unit = requestGc(phase)

  def driverGcCompleted(): Boolean = BroadcastGcAgent.gcCompleted()

  def mark(phase: String): Unit = BroadcastGcAgent.mark("phase", phase)

  def drain(sc: SparkContext): Unit = {
    synchronized {
      if (context == null) {
        context = sc
        sc.addSparkListener(listener)
      }
      require(context eq sc, "The diagnostic supports one SparkContext")
    }
    sc.listenerBus.waitUntilEmpty(30000L)
    require(sc.statusTracker.getActiveJobIds().isEmpty, "A Spark job is active")
    require(sc.getPersistentRDDs.isEmpty, "Cached RDDs are outside this diagnostic")
    require(activeSql.synchronized(activeSql.isEmpty), "A SQL action is active")
  }

  def finish(sc: SparkContext, phase: String, executors: Int,
      slots: Int, manual: Boolean): JList[String] = {
    drain(sc)
    require(sc.getConf.getBoolean(IndependentQueryBoundary.EnabledKey, false) == manual,
      "Ordinary cleanup flag differs from the requested diagnostic branch")
    val manager = ShuffleCleanupManager.get
    require(manager != null, "UCX query-end cleanup is required")
    manager.awaitIndependentQueryReclaims(executors, 60L)
    probe(sc, phase, executors, slots, if (manual) "manual" else "observe")
  }

  def probe(sc: SparkContext, phase: String, executors: Int,
      slots: Int, action: String): JList[String] = {
    drain(sc)
    val rows = sc.parallelize(0 until executors * slots, executors * slots).barrier()
      .mapPartitions { _ => Iterator(onExecutor(phase, action)) }.collect().distinct
    require(rows.length == executors, s"Expected $executors distinct executor records")
    drain(sc)
    rows.toList.asJava
  }

  private def hostAllocated: Long = {
    val getter = HostAlloc.getClass.getDeclaredMethod("getSingleton")
    getter.setAccessible(true)
    val allocator = getter.invoke(HostAlloc)
    val current = allocator.getClass.getMethod("getCurrentAllocated")
    current.setAccessible(true)
    current.invoke(allocator).asInstanceOf[java.lang.Long].longValue()
  }

  private def onExecutor(phase: String, action: String): String = synchronized {
    actions.getOrElseUpdate(phase, {
      GpuDeviceManager.getDeviceId().foreach(Cuda.setDevice)
      if (action == "manual") {
        Cuda.deviceSynchronize()
      }
      val beforeRmm = Rmm.getTotalBytesAllocated
      val observe = action == "observe" || action == "manual"
      val beforeObjects = if (observe) BroadcastGcSnapshot.records() else "[]"
      if (action == "gc") {
        requestGc(phase)
      } else if (action == "manual") {
        val broadcasts = ReferenceBroadcastSnapshot.takeOrdinaryGpuBroadcasts()
        broadcasts.foreach(_.retireAfterIndependentQuery())
        Cuda.deviceSynchronize()
      }
      // GC wait probes deliberately avoid acquiring references to broadcast objects.
      val objects = if (observe) BroadcastGcSnapshot.records() else "[]"
      BroadcastGcAgent.checkHealthy()
      val finalized = BroadcastGcAgent.finalizedIds().asScala.mkString("[", ",", "]")
      val result = s"""{"executor":"${SparkEnv.get.executorId}","time_ms":${System.currentTimeMillis()},""" +
        s""""phase":"$phase","action":"$action","rmm_bytes":${Rmm.getTotalBytesAllocated},""" +
        s""""host_allocated_bytes":$hostAllocated,"old_gc_count":$oldCollections,""" +
        s""""gc_cycle_completed":${BroadcastGcAgent.gcCompleted()},"objects":$objects,""" +
        s""""rmm_before_action":$beforeRmm,"objects_before_action":$beforeObjects,""" +
        s""""finalized_object_ids":$finalized}"""
      BroadcastGcAgent.mark("executor_phase", phase)
      result
    })
  }
}
