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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession

private[rapids] final class ColdStartWriteInstrumentation private[rapids] (
    val enabled: Boolean,
    val queryExecutionId: String,
    val outputPath: String) {

  def event(name: String): Unit = {
    if (enabled) {
      val epochMs = System.currentTimeMillis()
      ColdStartWriteInstrumentation.emit(ColdStartWriteInstrumentation.Metric(
        event = name,
        phase = "none",
        outcome = "none",
        queryExecutionId = queryExecutionId,
        outputPath = outputPath,
        durationNs = -1L,
        startEpochMs = epochMs,
        endEpochMs = epochMs,
        errorClass = "none"))
    }
  }

  def phase[T](name: String)(body: => T): T = {
    if (!enabled) {
      body
    } else {
      val startEpochMs = System.currentTimeMillis()
      val startNs = System.nanoTime()
      try {
        val result = body
        val endEpochMs = System.currentTimeMillis()
        ColdStartWriteInstrumentation.emit(ColdStartWriteInstrumentation.Metric(
          event = "phase",
          phase = name,
          outcome = "success",
          queryExecutionId = queryExecutionId,
          outputPath = outputPath,
          durationNs = System.nanoTime() - startNs,
          startEpochMs = startEpochMs,
          endEpochMs = endEpochMs,
          errorClass = "none"))
        result
      } catch {
        case t: Throwable =>
          val endEpochMs = System.currentTimeMillis()
          ColdStartWriteInstrumentation.emit(ColdStartWriteInstrumentation.Metric(
            event = "phase",
            phase = name,
            outcome = "failure",
            queryExecutionId = queryExecutionId,
            outputPath = outputPath,
            durationNs = System.nanoTime() - startNs,
            startEpochMs = startEpochMs,
            endEpochMs = endEpochMs,
            errorClass = t.getClass.getName))
          throw t
      }
    }
  }
}

private[rapids] object ColdStartWriteInstrumentation extends Logging {
  val ENABLED_KEY = "spark.rapids.sql.write.driverInstrumentation.enabled"
  val Disabled = new ColdStartWriteInstrumentation(false, "unknown", "unknown")

  case class Metric(
      event: String,
      phase: String,
      outcome: String,
      queryExecutionId: String,
      outputPath: String,
      durationNs: Long,
      startEpochMs: Long,
      endEpochMs: Long,
      errorClass: String)

  @volatile private var metricObserver: Metric => Unit = _ => ()

  def apply(sparkSession: SparkSession, outputPath: String): ColdStartWriteInstrumentation = {
    val sparkConfValue = sparkSession.sparkContext.getConf.getOption(ENABLED_KEY)
    val sessionConfValue = sparkSession.conf.getOption(ENABLED_KEY)
    val enabled = sparkConfValue.orElse(sessionConfValue).exists(_.toBoolean)
    val queryExecutionId = Option(sparkSession.sparkContext.getLocalProperty(
      SQLExecution.EXECUTION_ID_KEY)).getOrElse("unknown")
    if (sparkConfValue.isDefined || sessionConfValue.isDefined) {
      logWarning(s"RAPIDS_DRIVER_WRITE_INSTRUMENTATION_ACTIVATION enabled=$enabled " +
        s"spark_conf_value=${sparkConfValue.getOrElse("missing")} " +
        s"session_conf_value=${sessionConfValue.getOrElse("missing")} " +
        s"query_execution_id=$queryExecutionId output_path=$outputPath " +
        s"instrumentation_code_source=${codeSource(classOf[ColdStartWriteInstrumentation])} " +
        s"command_code_source=${codeSource(classOf[GpuInsertIntoHadoopFsRelationCommand])} " +
        s"writer_code_source=${codeSource(GpuFileFormatWriter.getClass)}")
    }
    new ColdStartWriteInstrumentation(enabled, queryExecutionId, outputPath)
  }

  private def codeSource(clazz: Class[_]): String = {
    Option(clazz.getProtectionDomain)
      .flatMap(domain => Option(domain.getCodeSource))
      .flatMap(source => Option(source.getLocation))
      .map(_.toString)
      .getOrElse("unknown")
  }

  private[rapids] def emit(metric: Metric): Unit = {
    logWarning(s"RAPIDS_DRIVER_WRITE_PHASE_METRIC event=${metric.event} " +
      s"phase=${metric.phase} outcome=${metric.outcome} " +
      s"query_execution_id=${metric.queryExecutionId} output_path=${metric.outputPath} " +
      s"duration_ns=${metric.durationNs} start_epoch_ms=${metric.startEpochMs} " +
      s"end_epoch_ms=${metric.endEpochMs} error_class=${metric.errorClass}")
    metricObserver(metric)
  }

  private[rapids] def setMetricObserver(observer: Metric => Unit): Unit = {
    metricObserver = observer
  }

  private[rapids] def resetMetricObserver(): Unit = {
    metricObserver = _ => ()
  }
}
