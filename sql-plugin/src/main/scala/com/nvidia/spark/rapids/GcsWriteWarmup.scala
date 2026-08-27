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

package com.nvidia.spark.rapids

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
 * Warms an executor-local cloud output stream with one bounded create/write/close/delete cycle.
 *
 * The operation runs on a daemon thread and is disabled by default. Callers must provide a
 * dedicated writable root. Each executor writes a distinct object below that root and deletes it
 * after close, so this path does not create a Spark job or a workload output file.
 */
private[rapids] object GcsWriteWarmup extends Logging {
  val ENABLED_KEY = "spark.rapids.executor.gcsWriteWarmup.enabled"
  val ROOT_URI_KEY = "spark.rapids.executor.gcsWriteWarmup.rootUri"
  val BYTES_KEY = "spark.rapids.executor.gcsWriteWarmup.bytes"
  val TIMEOUT_MS_KEY = "spark.rapids.executor.gcsWriteWarmup.timeoutMs"
  val CANCEL_ON_TASK_START_KEY = "spark.rapids.executor.gcsWriteWarmup.cancelOnTaskStart"
  val EXPECTED_FS_IMPL_KEY = "spark.rapids.executor.gcsWriteWarmup.expectedFsImpl"

  private val DefaultBytes = 1
  private val MaxBytes = 64 * 1024
  private val DefaultTimeoutMs = 15000L
  private val MaxTimeoutMs = 60000L
  private val DefaultExpectedFsImpl =
    "com.nvidia.v017.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"

  private[rapids] case class Settings(
      rootUri: String,
      byteCount: Int,
      timeoutMs: Long,
      cancelOnTaskStart: Boolean,
      expectedFsImpl: String)

  private[rapids] case class Result(
      executorId: String,
      uri: String,
      bytesWritten: Int,
      fsImpl: String,
      getFileSystemMs: Long,
      createMs: Long,
      writeMs: Long,
      closeMs: Long,
      deleteMs: Long,
      deleted: Boolean,
      totalMs: Long,
      startEpochMs: Long,
      endEpochMs: Long)

  private[rapids] final class AsyncHandle(
      val cancelOnTaskStart: Boolean,
      private val done: CountDownLatch,
      private val cancelled: AtomicBoolean,
      private val worker: Thread) {
    def cancel(reason: String): Boolean = {
      if (done.getCount > 0 && cancelled.compareAndSet(false, true)) {
        worker.interrupt()
        logInfo(s"RAPIDS_EXECUTOR_GCS_WRITE_WARMUP_METRIC event=cancel_requested " +
          s"reason=${metricValue(reason)} epoch_ms=${System.currentTimeMillis()}")
        true
      } else {
        false
      }
    }

    private[rapids] def await(timeoutMs: Long): Boolean =
      done.await(timeoutMs, TimeUnit.MILLISECONDS)
  }

  def startAsync(
      sparkConf: SparkConf,
      hadoopConf: () => Configuration,
      executorId: String): Option[AsyncHandle] = {
    if (!sparkConf.getBoolean(ENABLED_KEY, false)) {
      None
    } else {
      val confSnapshot = new SparkConf(false).setAll(sparkConf.getAll)
      val settings = parseSettings(confSnapshot)
      val done = new CountDownLatch(1)
      val cancelled = new AtomicBoolean(false)
      val safeExecutorId = metricValue(executorId)
      val worker = new Thread(() => {
        val startNanos = System.nanoTime()
        val startEpochMs = System.currentTimeMillis()
        try {
          val result = run(
            confSnapshot, hadoopConf(), executorId, settings, cancelled, startNanos, startEpochMs)
          logResult(result)
        } catch {
          case e: InterruptedException =>
            Thread.currentThread().interrupt()
            logFailure(executorId, "cancelled", startNanos, startEpochMs, e)
          case NonFatal(e) =>
            val status = if (cancelled.get()) "cancelled" else "failed"
            logFailure(executorId, status, startNanos, startEpochMs, e)
        } finally {
          done.countDown()
        }
      }, s"rapids-gcs-write-warmup-$safeExecutorId")
      worker.setDaemon(true)
      val handle = new AsyncHandle(
        settings.cancelOnTaskStart, done, cancelled, worker)
      worker.start()
      startDeadlineThread(handle, settings.timeoutMs, safeExecutorId)
      logInfo(s"RAPIDS_EXECUTOR_GCS_WRITE_WARMUP_METRIC event=submitted status=running " +
        s"executor_id=$safeExecutorId timeout_ms=${settings.timeoutMs} " +
        s"cancel_on_task_start=${settings.cancelOnTaskStart} " +
        s"epoch_ms=${System.currentTimeMillis()}")
      Some(handle)
    }
  }

  private[rapids] def run(
      sparkConf: SparkConf,
      baseHadoopConf: Configuration,
      executorId: String): Result = {
    val startNanos = System.nanoTime()
    val startEpochMs = System.currentTimeMillis()
    run(sparkConf, baseHadoopConf, executorId, parseSettings(sparkConf),
      new AtomicBoolean(false), startNanos, startEpochMs)
  }

  private def run(
      sparkConf: SparkConf,
      baseHadoopConf: Configuration,
      executorId: String,
      settings: Settings,
      cancelled: AtomicBoolean,
      startNanos: Long,
      startEpochMs: Long): Result = {
    val path = outputPath(settings.rootUri, executorId)
    val effectiveConf = GcsReadWarmup.buildEffectiveHadoopConf(sparkConf, baseHadoopConf)
    checkCancelled(cancelled)

    val getFileSystemStart = System.nanoTime()
    val fs = path.getFileSystem(effectiveConf)
    val getFileSystemMs = elapsedMs(getFileSystemStart)
    val fsImpl = fs.getClass.getName
    require(fsImpl == settings.expectedFsImpl,
      s"$EXPECTED_FS_IMPL_KEY expected ${settings.expectedFsImpl}, observed $fsImpl")
    checkCancelled(cancelled)

    val createStart = System.nanoTime()
    val out = fs.create(path, true)
    val createMs = elapsedMs(createStart)
    var writeMs = -1L
    var closeMs = -1L
    var deleteMs = -1L
    var deleted = false
    try {
      checkCancelled(cancelled)
      val writeStart = System.nanoTime()
      out.write(new Array[Byte](settings.byteCount))
      writeMs = elapsedMs(writeStart)
    } finally {
      val closeStart = System.nanoTime()
      try {
        out.close()
      } finally {
        closeMs = elapsedMs(closeStart)
        val deleteStart = System.nanoTime()
        deleted = fs.delete(path, false)
        deleteMs = elapsedMs(deleteStart)
      }
    }
    require(deleted, s"GCS write warm-up object was not deleted: $path")

    Result(
      executorId,
      path.toString,
      settings.byteCount,
      fsImpl,
      getFileSystemMs,
      createMs,
      writeMs,
      closeMs,
      deleteMs,
      deleted,
      elapsedMs(startNanos),
      startEpochMs,
      System.currentTimeMillis())
  }

  private[rapids] def parseSettings(conf: SparkConf): Settings = {
    val rootUri = conf.getOption(ROOT_URI_KEY).map(_.trim).filter(_.nonEmpty)
      .getOrElse(throw new IllegalArgumentException(
        s"$ROOT_URI_KEY is required when $ENABLED_KEY=true"))
    val rootPath = new Path(rootUri)
    require(Option(rootPath.toUri.getScheme).exists(_.equalsIgnoreCase("gs")),
      s"$ROOT_URI_KEY must use the gs scheme, observed $rootUri")
    val byteCount = conf.getInt(BYTES_KEY, DefaultBytes)
    require(byteCount > 0 && byteCount <= MaxBytes,
      s"$BYTES_KEY must be within [1, $MaxBytes], observed $byteCount")
    val timeoutMs = conf.getLong(TIMEOUT_MS_KEY, DefaultTimeoutMs)
    require(timeoutMs > 0 && timeoutMs <= MaxTimeoutMs,
      s"$TIMEOUT_MS_KEY must be within [1, $MaxTimeoutMs], observed $timeoutMs")
    val expectedFsImpl = conf.get(EXPECTED_FS_IMPL_KEY, DefaultExpectedFsImpl).trim
    require(expectedFsImpl.nonEmpty, s"$EXPECTED_FS_IMPL_KEY must not be empty")
    Settings(
      rootUri,
      byteCount,
      timeoutMs,
      conf.getBoolean(CANCEL_ON_TASK_START_KEY, false),
      expectedFsImpl)
  }

  private[rapids] def outputPath(rootUri: String, executorId: String): Path = {
    val safeExecutorId = metricValue(executorId)
    new Path(new Path(rootUri), s"executor-$safeExecutorId.bin")
  }

  private def startDeadlineThread(
      handle: AsyncHandle,
      timeoutMs: Long,
      safeExecutorId: String): Unit = {
    val deadline = new Thread(() => {
      try {
        if (!handle.await(timeoutMs)) {
          handle.cancel("timeout")
        }
      } catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
      }
    }, s"rapids-gcs-write-warmup-deadline-$safeExecutorId")
    deadline.setDaemon(true)
    deadline.start()
  }

  private def checkCancelled(cancelled: AtomicBoolean): Unit = {
    if (cancelled.get() || Thread.currentThread().isInterrupted) {
      throw new InterruptedException("GCS write warm-up was cancelled")
    }
  }

  private def logResult(result: Result): Unit = {
    logInfo(s"RAPIDS_EXECUTOR_GCS_WRITE_WARMUP_METRIC event=completed status=success " +
      s"executor_id=${metricValue(result.executorId)} uri=${metricValue(result.uri)} " +
      s"bytes=${result.bytesWritten} fs_impl=${metricValue(result.fsImpl)} " +
      s"get_file_system_ms=${result.getFileSystemMs} create_ms=${result.createMs} " +
      s"write_ms=${result.writeMs} close_ms=${result.closeMs} delete_ms=${result.deleteMs} " +
      s"deleted=${result.deleted} total_ms=${result.totalMs} " +
      s"start_epoch_ms=${result.startEpochMs} end_epoch_ms=${result.endEpochMs}")
  }

  private def logFailure(
      executorId: String,
      status: String,
      startNanos: Long,
      startEpochMs: Long,
      error: Throwable): Unit = {
    logInfo(s"RAPIDS_EXECUTOR_GCS_WRITE_WARMUP_METRIC event=completed status=$status " +
      s"executor_id=${metricValue(executorId)} total_ms=${elapsedMs(startNanos)} " +
      s"start_epoch_ms=$startEpochMs end_epoch_ms=${System.currentTimeMillis()} " +
      s"detail=${metricValue(errorDetail(error))}")
  }

  private def elapsedMs(startNanos: Long): Long =
    (System.nanoTime() - startNanos) / 1000000L

  private def errorDetail(error: Throwable): String =
    s"${error.getClass.getSimpleName}:${Option(error.getMessage).getOrElse("no_message")}"

  private def metricValue(value: String): String =
    value.replaceAll("[^A-Za-z0-9_./:@?=&%+,-]", "_")
}
