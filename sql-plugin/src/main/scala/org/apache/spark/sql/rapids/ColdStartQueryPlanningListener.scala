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

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex,
  InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.util.QueryExecutionListener

/** Emits query-planning and file-index timing for the cold-start diagnostic. */
class ColdStartQueryPlanningListener extends QueryExecutionListener with Logging {
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    safelyLogQuery("success", funcName, qe, durationNs, None)
  }

  override def onFailure(
      funcName: String,
      qe: QueryExecution,
      exception: Exception): Unit = {
    safelyLogQuery("failure", funcName, qe, -1L, Some(exception.getClass.getName))
  }

  private def safelyLogQuery(
      outcome: String,
      funcName: String,
      qe: QueryExecution,
      durationNs: Long,
      errorClass: Option[String]): Unit = {
    try {
      logQuery(outcome, funcName, qe, durationNs, errorClass)
    } catch {
      case NonFatal(error) =>
        emitMetric(s"RAPIDS_QUERY_PLANNING_METRIC outcome=instrumentation_failure " +
          s"query_execution_id=${qe.id} func=${sanitize(funcName)} " +
          s"error_class=${sanitize(error.getClass.getName)}")
    }
  }

  private def logQuery(
      outcome: String,
      funcName: String,
      qe: QueryExecution,
      durationNs: Long,
      errorClass: Option[String]): Unit = {
    val writeCommand = qe.logical.collectFirst {
      case command: InsertIntoHadoopFsRelationCommand => command
    }
    val outputPath = writeCommand.map(_.outputPath.toString).getOrElse("none")
    val inputPlanIdentity = writeCommand
      .map(command => System.identityHashCode(command.query))
      .getOrElse(-1)
    val phaseMetrics = qe.tracker.phases.toSeq.sortBy(_._1).map { case (phase, summary) =>
      s"phase_${sanitize(phase)}_ms=${summary.durationMs}"
    }.mkString(" ")
    val topRules = qe.tracker.topRulesByTime(10).map { case (rule, summary) =>
      s"${sanitize(rule)}:${summary.totalTimeNs / 1000000L}"
    }.mkString(",")
    val durationMs = if (durationNs < 0) -1L else durationNs / 1000000L
    val failureMetric = errorClass.map(name => s" error_class=${sanitize(name)}").getOrElse("")

    emitMetric(s"RAPIDS_QUERY_PLANNING_METRIC outcome=$outcome query_execution_id=${qe.id} " +
      s"func=${sanitize(funcName)} duration_ms=$durationMs output_path=$outputPath " +
      s"input_plan_identity_hash=$inputPlanIdentity " +
      s"$phaseMetrics top_rules=$topRules$failureMetric")

    logFileIndexes(qe, outputPath)
  }

  private def logFileIndexes(qe: QueryExecution, outputPath: String): Unit = {
    val indexes = qe.analyzed.collect {
      case relation: LogicalRelation => relation.relation match {
        case fsRelation: HadoopFsRelation => fsRelation.location match {
          case index: InMemoryFileIndex => Some(index)
          case _ => None
        }
        case _ => None
      }
    }.flatten

    indexes.zipWithIndex.foreach { case (index, ordinal) =>
      val roots = index.rootPaths.map(_.toString).mkString(",")
      emitMetric(s"RAPIDS_FILE_INDEX_METRIC query_execution_id=${qe.id} ordinal=$ordinal " +
        s"identity_hash=${System.identityHashCode(index)} root_paths=$roots " +
        s"input_files=${index.inputFiles.length} " +
        s"metadata_ops_time_ns=${metadataOpsTimeNs(index)} output_path=$outputPath")
    }
  }

  private def metadataOpsTimeNs(index: InMemoryFileIndex): Long = {
    try {
      index.getClass.getMethod("metadataOpsTimeNs").invoke(index) match {
        case value: Option[_] => value.collect { case timeNs: Long => timeNs }.getOrElse(-1L)
        case _ => -1L
      }
    } catch {
      case NonFatal(_) => -1L
    }
  }

  private def emitMetric(metric: String): Unit = {
    logWarning(metric)
    ColdStartQueryPlanningListener.observeMetric(metric)
  }

  private def sanitize(value: String): String = value.replaceAll("[^A-Za-z0-9_.$-]", "_")
}

object ColdStartQueryPlanningListener {
  @volatile private var metricObserver: (String => Unit) = (_: String) => ()

  private[rapids] def observeMetric(metric: String): Unit = metricObserver(metric)

  private[rapids] def setMetricObserver(observer: String => Unit): Unit = {
    metricObserver = observer
  }

  private[rapids] def resetMetricObserver(): Unit = {
    metricObserver = (_: String) => ()
  }
}
