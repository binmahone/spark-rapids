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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
 * Performs one best-effort executor-local GCS read before workload tasks start.
 *
 * The warm-up runs on a daemon thread and never blocks executor registration. A real task or the
 * configured deadline can cancel it. The URI list is expected to contain dedicated immutable
 * objects so the diagnostic does not depend on workload file selection or Spark query planning.
 */
private[rapids] object GcsReadWarmup extends Logging {
  val ENABLED_KEY = "spark.rapids.executor.gcsReadWarmup.enabled"
  val URIS_KEY = "spark.rapids.executor.gcsReadWarmup.uris"
  val BYTES_KEY = "spark.rapids.executor.gcsReadWarmup.bytes"
  val OFFSET_KEY = "spark.rapids.executor.gcsReadWarmup.offset"
  val TIMEOUT_MS_KEY = "spark.rapids.executor.gcsReadWarmup.timeoutMs"
  val CANCEL_ON_TASK_START_KEY = "spark.rapids.executor.gcsReadWarmup.cancelOnTaskStart"
  val EXPECTED_FS_IMPL_KEY = "spark.rapids.executor.gcsReadWarmup.expectedFsImpl"

  private val SparkHadoopPrefix = "spark.hadoop."
  private val DefaultBytes = 256 * 1024
  private val MaxBytes = 1024 * 1024
  private val DefaultTimeoutMs = 5000L
  private val MaxTimeoutMs = 30000L
  private val DefaultExpectedFsImpl =
    "com.nvidia.v017.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"

  private[rapids] case class Settings(
      uris: Seq[String],
      byteCount: Int,
      offset: Long,
      timeoutMs: Long,
      cancelOnTaskStart: Boolean,
      expectedFsImpl: String)

  private[rapids] case class Result(
      status: String,
      executorId: String,
      uri: String,
      uriIndex: Int,
      bytesRead: Int,
      fsImpl: String,
      configurationMs: Long,
      getFileSystemMs: Long,
      openMs: Long,
      seekMs: Long,
      firstByteMs: Long,
      readRemainingMs: Long,
      closeMs: Long,
      totalMs: Long,
      detail: String)

  private[rapids] final class AsyncHandle(
      val cancelOnTaskStart: Boolean,
      private val done: CountDownLatch,
      private val cancelled: AtomicBoolean,
      private val cancelReason: AtomicReference[String],
      private val input: AtomicReference[FSDataInputStream],
      private val worker: Thread) {

    def cancel(reason: String): Boolean = {
      val requested = cancelReason.compareAndSet(null, reason)
      if (requested && done.getCount > 0) {
        cancelled.set(true)
        worker.interrupt()
        Option(input.get()).foreach(closeAsync)
        logInfo(s"RAPIDS_EXECUTOR_GCS_READ_WARMUP_METRIC event=cancel_requested " +
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
      val done = new CountDownLatch(1)
      val cancelled = new AtomicBoolean(false)
      val cancelReason = new AtomicReference[String]()
      val input = new AtomicReference[FSDataInputStream]()
      val safeExecutorId = metricValue(executorId)

      val worker = new Thread(() => {
        val totalStart = System.nanoTime()
        val startEpochMs = System.currentTimeMillis()
        try {
          val configurationStart = System.nanoTime()
          val settings = parseSettings(confSnapshot)
          logPhase(executorId, "configuration", elapsedMs(configurationStart))
          val hadoopConfStart = System.nanoTime()
          val baseHadoopConf = hadoopConf()
          logPhase(executorId, "hadoop_conf_supplier", elapsedMs(hadoopConfStart))
          val result = run(confSnapshot, baseHadoopConf, executorId, settings, cancelled, input,
            totalStart)
          logResult(result, startEpochMs)
        } catch {
          case e: InterruptedException =>
            logInfo(s"RAPIDS_EXECUTOR_GCS_READ_WARMUP_METRIC event=completed status=cancelled " +
              s"executor_id=$safeExecutorId total_ms=${elapsedMs(totalStart)} " +
              s"start_epoch_ms=$startEpochMs end_epoch_ms=${System.currentTimeMillis()} " +
              s"detail=${metricValue(errorDetail(e))}")
          case NonFatal(e) =>
            val status = if (cancelled.get()) "cancelled" else "failed"
            logInfo(s"RAPIDS_EXECUTOR_GCS_READ_WARMUP_METRIC event=completed status=$status " +
              s"executor_id=$safeExecutorId total_ms=${elapsedMs(totalStart)} " +
              s"start_epoch_ms=$startEpochMs end_epoch_ms=${System.currentTimeMillis()} " +
              s"detail=${metricValue(errorDetail(e))}")
        } finally {
          done.countDown()
        }
      }, s"rapids-gcs-read-warmup-$safeExecutorId")
      worker.setDaemon(true)

      val cancelOnTaskStart = Try(
        confSnapshot.getBoolean(CANCEL_ON_TASK_START_KEY, true)).getOrElse(true)
      val timeoutMs = Try(
        confSnapshot.getLong(TIMEOUT_MS_KEY, DefaultTimeoutMs))
        .filter(value => value > 0 && value <= MaxTimeoutMs)
        .getOrElse(DefaultTimeoutMs)
      val handle = new AsyncHandle(
        cancelOnTaskStart, done, cancelled, cancelReason, input, worker)
      worker.start()
      startDeadlineThread(handle, done, timeoutMs, safeExecutorId)
      logInfo(s"RAPIDS_EXECUTOR_GCS_READ_WARMUP_METRIC event=submitted status=running " +
        s"executor_id=$safeExecutorId timeout_ms=$timeoutMs " +
        s"cancel_on_task_start=$cancelOnTaskStart epoch_ms=${System.currentTimeMillis()}")
      Some(handle)
    }
  }

  private[rapids] def run(
      sparkConf: SparkConf,
      baseHadoopConf: Configuration,
      executorId: String): Result = {
    val settings = parseSettings(sparkConf)
    run(sparkConf, baseHadoopConf, executorId, settings,
      new AtomicBoolean(false), new AtomicReference[FSDataInputStream](), System.nanoTime())
  }

  private def run(
      sparkConf: SparkConf,
      baseHadoopConf: Configuration,
      executorId: String,
      settings: Settings,
      cancelled: AtomicBoolean,
      inputRef: AtomicReference[FSDataInputStream],
      totalStart: Long): Result = {
    val configurationStart = System.nanoTime()
    val effectiveHadoopConf = buildEffectiveHadoopConf(sparkConf, baseHadoopConf)
    val uriIndex = Math.floorMod(executorId.hashCode, settings.uris.size)
    val uri = settings.uris(uriIndex)
    val path = new Path(uri)
    require(Option(path.toUri.getScheme).exists(_.equalsIgnoreCase("gs")),
      s"$URIS_KEY entries must use the gs scheme, observed $uri")
    val configurationMs = elapsedMs(configurationStart)
    logPhase(executorId, "hadoop_configuration", configurationMs)
    checkCancelled(cancelled)

    val getFileSystemStart = System.nanoTime()
    val fs = path.getFileSystem(effectiveHadoopConf)
    val getFileSystemMs = elapsedMs(getFileSystemStart)
    logPhase(executorId, "get_file_system", getFileSystemMs)
    val fsImpl = fs.getClass.getName
    require(fsImpl == settings.expectedFsImpl,
      s"$EXPECTED_FS_IMPL_KEY expected ${settings.expectedFsImpl}, observed $fsImpl")
    checkCancelled(cancelled)

    val openStart = System.nanoTime()
    val in = fs.open(path)
    val openMs = elapsedMs(openStart)
    logPhase(executorId, "open", openMs)
    inputRef.set(in)
    var seekMs = -1L
    var firstByteMs = -1L
    var readRemainingMs = -1L
    var closeMs = -1L
    var bytesRead = 0
    try {
      checkCancelled(cancelled)
      val seekStart = System.nanoTime()
      in.seek(settings.offset)
      seekMs = elapsedMs(seekStart)
      logPhase(executorId, "seek", seekMs)

      checkCancelled(cancelled)
      val firstByteStart = System.nanoTime()
      val firstByte = in.read()
      firstByteMs = elapsedMs(firstByteStart)
      logPhase(executorId, "first_byte", firstByteMs)
      require(firstByte >= 0, s"Unexpected EOF at offset ${settings.offset} for $uri")
      bytesRead = 1

      val buffer = new Array[Byte](math.min(64 * 1024, settings.byteCount - bytesRead))
      val readRemainingStart = System.nanoTime()
      while (bytesRead < settings.byteCount) {
        checkCancelled(cancelled)
        val requested = math.min(buffer.length, settings.byteCount - bytesRead)
        val count = in.read(buffer, 0, requested)
        require(count >= 0,
          s"Unexpected EOF after $bytesRead of ${settings.byteCount} bytes for $uri")
        bytesRead += count
      }
      readRemainingMs = elapsedMs(readRemainingStart)
      logPhase(executorId, "read_remaining", readRemainingMs)
    } finally {
      inputRef.compareAndSet(in, null)
      val closeStart = System.nanoTime()
      closeQuietly(in)
      closeMs = elapsedMs(closeStart)
      logPhase(executorId, "close", closeMs)
    }

    Result(
      status = if (cancelled.get()) "cancelled" else "success",
      executorId = executorId,
      uri = uri,
      uriIndex = uriIndex,
      bytesRead = bytesRead,
      fsImpl = fsImpl,
      configurationMs = configurationMs,
      getFileSystemMs = getFileSystemMs,
      openMs = openMs,
      seekMs = seekMs,
      firstByteMs = firstByteMs,
      readRemainingMs = readRemainingMs,
      closeMs = closeMs,
      totalMs = elapsedMs(totalStart),
      detail = "none")
  }

  private[rapids] def parseSettings(sparkConf: SparkConf): Settings = {
    val uris = sparkConf.getOption(URIS_KEY).toSeq
      .flatMap(_.split(",")).map(_.trim).filter(_.nonEmpty)
    require(uris.nonEmpty, s"$URIS_KEY must contain at least one URI when $ENABLED_KEY=true")
    val byteCount = sparkConf.getInt(BYTES_KEY, DefaultBytes)
    require(byteCount > 0 && byteCount <= MaxBytes,
      s"$BYTES_KEY must be within [1, $MaxBytes], observed $byteCount")
    val offset = sparkConf.getLong(OFFSET_KEY, 0L)
    require(offset >= 0, s"$OFFSET_KEY must be non-negative, observed $offset")
    val timeoutMs = sparkConf.getLong(TIMEOUT_MS_KEY, DefaultTimeoutMs)
    require(timeoutMs > 0 && timeoutMs <= MaxTimeoutMs,
      s"$TIMEOUT_MS_KEY must be within [1, $MaxTimeoutMs], observed $timeoutMs")
    val expectedFsImpl = sparkConf.get(EXPECTED_FS_IMPL_KEY, DefaultExpectedFsImpl).trim
    require(expectedFsImpl.nonEmpty, s"$EXPECTED_FS_IMPL_KEY must not be empty")
    Settings(
      uris,
      byteCount,
      offset,
      timeoutMs,
      sparkConf.getBoolean(CANCEL_ON_TASK_START_KEY, true),
      expectedFsImpl)
  }

  private[rapids] def buildEffectiveHadoopConf(
      sparkConf: SparkConf,
      baseHadoopConf: Configuration): Configuration = {
    val effective = new Configuration(baseHadoopConf)
    sparkConf.getAllWithPrefix(SparkHadoopPrefix).foreach { case (key, value) =>
      effective.set(key, value)
    }
    effective
  }

  private def startDeadlineThread(
      handle: AsyncHandle,
      done: CountDownLatch,
      timeoutMs: Long,
      safeExecutorId: String): Unit = {
    val deadline = new Thread(() => {
      try {
        if (!done.await(timeoutMs, TimeUnit.MILLISECONDS)) {
          handle.cancel("timeout")
        }
      } catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
      }
    }, s"rapids-gcs-read-warmup-deadline-$safeExecutorId")
    deadline.setDaemon(true)
    deadline.start()
  }

  private def checkCancelled(cancelled: AtomicBoolean): Unit = {
    if (cancelled.get() || Thread.currentThread().isInterrupted) {
      throw new InterruptedException("GCS read warm-up was cancelled")
    }
  }

  private def logResult(result: Result, startEpochMs: Long): Unit = {
    logInfo(s"RAPIDS_EXECUTOR_GCS_READ_WARMUP_METRIC event=completed " +
      s"status=${result.status} executor_id=${metricValue(result.executorId)} " +
      s"uri_index=${result.uriIndex} uri=${metricValue(result.uri)} bytes=${result.bytesRead} " +
      s"fs_impl=${metricValue(result.fsImpl)} configuration_ms=${result.configurationMs} " +
      s"get_file_system_ms=${result.getFileSystemMs} open_ms=${result.openMs} " +
      s"seek_ms=${result.seekMs} first_byte_ms=${result.firstByteMs} " +
      s"read_remaining_ms=${result.readRemainingMs} close_ms=${result.closeMs} " +
      s"total_ms=${result.totalMs} start_epoch_ms=$startEpochMs " +
      s"end_epoch_ms=${System.currentTimeMillis()} detail=${metricValue(result.detail)}")
  }

  private def logPhase(executorId: String, phase: String, durationMs: Long): Unit = {
    logInfo(s"RAPIDS_EXECUTOR_GCS_READ_WARMUP_METRIC event=phase " +
      s"executor_id=${metricValue(executorId)} phase=$phase duration_ms=$durationMs")
  }

  private def closeQuietly(in: FSDataInputStream): Unit = {
    try {
      in.close()
    } catch {
      case NonFatal(_) =>
    }
  }

  private def closeAsync(in: FSDataInputStream): Unit = {
    val closer = new Thread(() => closeQuietly(in), "rapids-gcs-read-warmup-cancel-close")
    closer.setDaemon(true)
    closer.start()
  }

  private def elapsedMs(startNanos: Long): Long =
    (System.nanoTime() - startNanos) / 1000000L

  private def errorDetail(e: Throwable): String =
    s"${e.getClass.getSimpleName}:${Option(e.getMessage).getOrElse("no_message")}"

  private def metricValue(value: String): String =
    value.replaceAll("[^A-Za-z0-9_./:@?=&%+,-]", "_")
}
