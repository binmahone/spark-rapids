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

import java.util.concurrent.{CountDownLatch, Future, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.io.async.UnboundedAsyncRunner
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
 * Warms executor-local cloud-reader and cuDF Parquet decode paths without creating a Spark job.
 *
 * File reads are submitted to the singleton pool used by the multi-file readers. The coordinator
 * then decodes one complete Parquet object directly with cuDF. This does not execute Spark scan
 * admission, TaskContext, GPU semaphore, projection, or filtering paths.
 */
private[rapids] object ExecutorReaderDecodeWarmup extends Logging {
  val ENABLED_KEY = "spark.rapids.executor.readerDecodeWarmup.enabled"
  val URIS_KEY = "spark.rapids.executor.readerDecodeWarmup.uris"
  val WORKER_COUNT_KEY = "spark.rapids.executor.readerDecodeWarmup.workerCount"
  val MAX_FILE_BYTES_KEY = "spark.rapids.executor.readerDecodeWarmup.maxFileBytes"
  val TIMEOUT_MS_KEY = "spark.rapids.executor.readerDecodeWarmup.timeoutMs"
  val CANCEL_ON_TASK_START_KEY =
    "spark.rapids.executor.readerDecodeWarmup.cancelOnTaskStart"
  val EXPECTED_FS_IMPL_KEY = "spark.rapids.executor.readerDecodeWarmup.expectedFsImpl"
  val WAIT_FOR_GCS_WARMUP_KEY =
    "spark.rapids.executor.readerDecodeWarmup.waitForGcsReadWarmup"

  private val DefaultWorkerCount = 1
  private val MaxWorkerCount = 64
  private val DefaultMaxFileBytes = 1024 * 1024
  private val AbsoluteMaxFileBytes = 8 * 1024 * 1024
  private val DefaultTimeoutMs = 15000L
  private val MaxTimeoutMs = 60000L
  private[rapids] val TaskStartCancelAwaitMs = 2000L
  private val DefaultExpectedFsImpl =
    "com.nvidia.v017.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"

  private[rapids] case class Settings(
      uris: Seq[String],
      workerCount: Int,
      maxFileBytes: Int,
      timeoutMs: Long,
      cancelOnTaskStart: Boolean,
      expectedFsImpl: String,
      waitForGcsReadWarmup: Boolean)

  private[rapids] case class ReadResult(
      uri: String,
      uriIndex: Int,
      bytes: Array[Byte],
      fsImpl: String,
      workerThread: String,
      readMs: Long)

  private[rapids] final class AsyncHandle(
      val cancelOnTaskStart: Boolean,
      private val done: CountDownLatch,
      private val cancelled: AtomicBoolean,
      private val coordinator: Thread,
      private val futures: ArrayBuffer[Future[_]]) {
    private val taskStartHandled = new AtomicBoolean(false)

    def cancel(reason: String): Boolean = synchronized {
      if (done.getCount > 0 && cancelled.compareAndSet(false, true)) {
        futures.synchronized(futures.foreach(_.cancel(true)))
        coordinator.interrupt()
        logInfo(s"RAPIDS_EXECUTOR_READER_DECODE_WARMUP_METRIC event=cancel_requested " +
          s"reason=${metricValue(reason)}")
        true
      } else {
        false
      }
    }

    private[rapids] def await(timeoutMs: Long): Boolean =
      done.await(timeoutMs, TimeUnit.MILLISECONDS)

    private[rapids] def cancelOnFirstTaskAndAwait(reason: String, timeoutMs: Long): Boolean = {
      if (taskStartHandled.compareAndSet(false, true)) {
        val start = System.nanoTime()
        cancel(reason)
        val completed = await(timeoutMs)
        logInfo(s"RAPIDS_EXECUTOR_READER_DECODE_WARMUP_METRIC event=cancel_wait " +
          s"reason=${metricValue(reason)} completed=$completed duration_ms=${elapsedMs(start)}")
        completed
      } else {
        true
      }
    }
  }

  def startAsync(
      sparkConf: SparkConf,
      rapidsConf: RapidsConf,
      hadoopConf: () => Configuration,
      executorId: String,
      predecessor: Option[GcsReadWarmup.AsyncHandle]): Option[AsyncHandle] = {
    if (!sparkConf.getBoolean(ENABLED_KEY, false)) {
      None
    } else {
      val confSnapshot = new SparkConf(false).setAll(sparkConf.getAll)
      val settings = parseSettings(confSnapshot)
      val poolConf = ThreadPoolConfBuilder(rapidsConf).build()
      require(!poolConf.stageLevelPool,
        s"$ENABLED_KEY cannot be used with a stage-level multi-thread reader pool")
      val done = new CountDownLatch(1)
      val cancelled = new AtomicBoolean(false)
      val futures = ArrayBuffer.empty[Future[_]]
      val safeExecutorId = metricValue(executorId)

      val coordinator = new Thread(() => {
        val start = System.nanoTime()
        try {
          if (settings.waitForGcsReadWarmup) {
            predecessor.foreach(_.await(settings.timeoutMs))
          }
          checkCancelled(cancelled)
          val effectiveConf = GcsReadWarmup.buildEffectiveHadoopConf(confSnapshot, hadoopConf())
          val pool = MultiFileReaderThreadPool.getOrCreateThreadPool(poolConf)
          MultiFileReaderThreadPool.recordWarmupPool(pool)
          val selected = selectUris(settings.uris, settings.workerCount, executorId)
          logInfo(s"RAPIDS_EXECUTOR_READER_DECODE_WARMUP_METRIC event=pool_ready " +
            s"executor_id=$safeExecutorId pool_identity=${System.identityHashCode(pool)} " +
            s"pool_class=${metricValue(pool.getClass.getName)} " +
            s"maximum_pool_size=${pool.getMaximumPoolSize} selected_workers=${selected.size}")

          val submitted = selected.map { case (uri, uriIndex) =>
            val runner = new ReadRunner(
              uri, uriIndex, effectiveConf, settings.maxFileBytes, settings.expectedFsImpl)
            val future = pool.submit(runner)
            futures.synchronized(futures += future)
            future
          }
          val results = submitted.map { future =>
            checkCancelled(cancelled)
            val result = future.get(settings.timeoutMs, TimeUnit.MILLISECONDS)
            try {
              result.data
            } finally {
              result.close()
            }
          }
          results.foreach(logReadResult(executorId, _))
          checkCancelled(cancelled)

          val decodeStart = System.nanoTime()
          val (rows, columns) = withResource(Table.readParquet(results.head.bytes)) { table =>
            (table.getRowCount, table.getNumberOfColumns)
          }
          checkCancelled(cancelled)
          logInfo(s"RAPIDS_EXECUTOR_READER_DECODE_WARMUP_METRIC event=completed status=success " +
            s"executor_id=$safeExecutorId workers=${results.size} " +
            s"unique_worker_threads=${results.map(_.workerThread).distinct.size} " +
            s"bytes=${results.map(_.bytes.length.toLong).sum} decoded_rows=$rows " +
            s"decoded_columns=$columns decode_ms=${elapsedMs(decodeStart)} " +
            s"total_ms=${elapsedMs(start)}")
        } catch {
          case e: InterruptedException =>
            Thread.currentThread().interrupt()
            logCompletionFailure(executorId, "cancelled", start, e)
          case NonFatal(e) =>
            val status = if (cancelled.get()) "cancelled" else "failed"
            logCompletionFailure(executorId, status, start, e)
        } finally {
          done.countDown()
        }
      }, s"rapids-reader-decode-warmup-$safeExecutorId")
      coordinator.setDaemon(true)
      val handle = new AsyncHandle(
        settings.cancelOnTaskStart, done, cancelled, coordinator, futures)
      coordinator.start()
      startDeadlineThread(handle, settings.timeoutMs, safeExecutorId)
      logInfo(s"RAPIDS_EXECUTOR_READER_DECODE_WARMUP_METRIC event=submitted status=running " +
        s"executor_id=$safeExecutorId timeout_ms=${settings.timeoutMs} " +
        s"cancel_on_task_start=${settings.cancelOnTaskStart}")
      Some(handle)
    }
  }

  private class ReadRunner(
      uri: String,
      uriIndex: Int,
      conf: Configuration,
      maxFileBytes: Int,
      expectedFsImpl: String) extends UnboundedAsyncRunner[ReadResult] {

    override protected def callImpl(): ReadResult = {
      val start = System.nanoTime()
      val path = new Path(uri)
      require(Option(path.toUri.getScheme).exists(_.equalsIgnoreCase("gs")),
        s"$URIS_KEY entries must use the gs scheme, observed $uri")
      val fs = path.getFileSystem(conf)
      val fsImpl = fs.getClass.getName
      require(fsImpl == expectedFsImpl,
        s"$EXPECTED_FS_IMPL_KEY expected $expectedFsImpl, observed $fsImpl")
      val length = fs.getFileStatus(path).getLen
      require(length > 0 && length <= maxFileBytes,
        s"Parquet warm-up object $uri has $length bytes; expected (0, $maxFileBytes]")
      val bytes = new Array[Byte](length.toInt)
      withResource(fs.open(path)) { in =>
        in.readFully(0L, bytes)
      }
      ReadResult(uri, uriIndex, bytes, fsImpl, Thread.currentThread().getName, elapsedMs(start))
    }
  }

  private[rapids] def parseSettings(conf: SparkConf): Settings = {
    val uris = conf.getOption(URIS_KEY).toSeq
      .flatMap(_.split(",")).map(_.trim).filter(_.nonEmpty)
    require(uris.nonEmpty, s"$URIS_KEY must contain at least one URI when $ENABLED_KEY=true")
    val workerCount = conf.getInt(WORKER_COUNT_KEY, DefaultWorkerCount)
    require(workerCount > 0 && workerCount <= MaxWorkerCount,
      s"$WORKER_COUNT_KEY must be within [1, $MaxWorkerCount], observed $workerCount")
    require(workerCount <= uris.size,
      s"$WORKER_COUNT_KEY=$workerCount exceeds the ${uris.size} configured URIs")
    val maxFileBytes = conf.getInt(MAX_FILE_BYTES_KEY, DefaultMaxFileBytes)
    require(maxFileBytes > 0 && maxFileBytes <= AbsoluteMaxFileBytes,
      s"$MAX_FILE_BYTES_KEY must be within [1, $AbsoluteMaxFileBytes], observed $maxFileBytes")
    val timeoutMs = conf.getLong(TIMEOUT_MS_KEY, DefaultTimeoutMs)
    require(timeoutMs > 0 && timeoutMs <= MaxTimeoutMs,
      s"$TIMEOUT_MS_KEY must be within [1, $MaxTimeoutMs], observed $timeoutMs")
    val expectedFsImpl = conf.get(EXPECTED_FS_IMPL_KEY, DefaultExpectedFsImpl).trim
    require(expectedFsImpl.nonEmpty, s"$EXPECTED_FS_IMPL_KEY must not be empty")
    Settings(
      uris,
      workerCount,
      maxFileBytes,
      timeoutMs,
      conf.getBoolean(CANCEL_ON_TASK_START_KEY, true),
      expectedFsImpl,
      conf.getBoolean(WAIT_FOR_GCS_WARMUP_KEY, true))
  }

  private[rapids] def selectUris(
      uris: Seq[String], workerCount: Int, executorId: String): Seq[(String, Int)] = {
    val start = Math.floorMod(executorId.hashCode, uris.size)
    (0 until workerCount).map { offset =>
      val index = (start + offset) % uris.size
      (uris(index), index)
    }
  }

  private def startDeadlineThread(
      handle: AsyncHandle, timeoutMs: Long, safeExecutorId: String): Unit = {
    val deadline = new Thread(() => {
      try {
        if (!handle.await(timeoutMs)) {
          handle.cancel("timeout")
        }
      } catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
      }
    }, s"rapids-reader-decode-warmup-deadline-$safeExecutorId")
    deadline.setDaemon(true)
    deadline.start()
  }

  private def checkCancelled(cancelled: AtomicBoolean): Unit = {
    if (cancelled.get() || Thread.currentThread().isInterrupted) {
      throw new InterruptedException("Executor reader/decode warm-up was cancelled")
    }
  }

  private def logReadResult(executorId: String, result: ReadResult): Unit = {
    logInfo(s"RAPIDS_EXECUTOR_READER_DECODE_WARMUP_METRIC event=worker_read status=success " +
      s"executor_id=${metricValue(executorId)} uri_index=${result.uriIndex} " +
      s"uri=${metricValue(result.uri)} bytes=${result.bytes.length} " +
      s"fs_impl=${metricValue(result.fsImpl)} " +
      s"worker_thread=${metricValue(result.workerThread)} read_ms=${result.readMs}")
  }

  private def logCompletionFailure(
      executorId: String, status: String, start: Long, error: Throwable): Unit = {
    logInfo(s"RAPIDS_EXECUTOR_READER_DECODE_WARMUP_METRIC event=completed status=$status " +
      s"executor_id=${metricValue(executorId)} total_ms=${elapsedMs(start)} " +
      s"detail=${metricValue(errorDetail(error))}")
  }

  private def elapsedMs(startNanos: Long): Long =
    (System.nanoTime() - startNanos) / 1000000L

  private def errorDetail(error: Throwable): String =
    s"${error.getClass.getSimpleName}:${Option(error.getMessage).getOrElse("no_message")}"

  private def metricValue(value: String): String =
    value.replaceAll("[^A-Za-z0-9_./:@?=&%+,-]", "_")
}
