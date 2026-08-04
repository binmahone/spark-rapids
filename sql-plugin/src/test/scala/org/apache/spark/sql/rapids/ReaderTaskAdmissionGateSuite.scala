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

import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit, TimeoutException}
import java.util.concurrent.atomic.AtomicInteger

import com.nvidia.spark.rapids.ScalableTaskCompletion
import org.mockito.Mockito.{mock, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.TaskContext

class ReaderTaskAdmissionGateSuite extends AnyFunSuite with BeforeAndAfterEach {

  private def taskContext(taskAttemptId: Long): TaskContext = {
    val context = mock(classOf[TaskContext])
    when(context.taskAttemptId()).thenReturn(taskAttemptId)
    context
  }

  override def afterEach(): Unit = {
    ScalableTaskCompletion.reset()
  }

  test("reader admission is task-reentrant and blocks excess tasks") {
    val releaseCount = new AtomicInteger()
    val releasedGpu = new CountDownLatch(2)
    val gate = new ReaderTaskAdmissionGate(1, _ => {
      releaseCount.incrementAndGet()
      releasedGpu.countDown()
    })
    val firstContext = taskContext(1L)
    val secondContext = taskContext(2L)
    val first = gate.acquire(firstContext)
    assert(first.acquired)
    assert(gate.availablePermits === 0)
    assert(!gate.acquire(firstContext).acquired)
    assert(releaseCount.get() === 1)

    val executor = Executors.newSingleThreadExecutor()
    val started = new CountDownLatch(1)
    try {
      val second = executor.submit(new Callable[ReaderTaskAdmissionResult] {
        override def call(): ReaderTaskAdmissionResult = {
          started.countDown()
          gate.acquire(secondContext)
        }
      })
      assert(started.await(5, TimeUnit.SECONDS))
      assert(releasedGpu.await(5, TimeUnit.SECONDS))
      assert(releaseCount.get() === 2)
      intercept[TimeoutException] {
        second.get(100, TimeUnit.MILLISECONDS)
      }

      gate.releaseReference(1L)
      assert(gate.availablePermits === 0)
      gate.releaseReference(1L)
      assert(second.get(5, TimeUnit.SECONDS).acquired)
      assert(gate.availablePermits === 0)
      gate.releaseReference(2L)
      assert(gate.availablePermits === 1)
    } finally {
      executor.shutdownNow()
    }
  }

  test("interrupted admission does not leak task identity or permits") {
    val gate = new ReaderTaskAdmissionGate(1, _ => ())
    val firstContext = taskContext(1L)
    val secondContext = taskContext(2L)
    assert(gate.acquire(firstContext).acquired)

    val executor = Executors.newSingleThreadExecutor()
    val started = new CountDownLatch(1)
    try {
      val blocked = executor.submit(new Callable[ReaderTaskAdmissionResult] {
        override def call(): ReaderTaskAdmissionResult = {
          started.countDown()
          gate.acquire(secondContext)
        }
      })
      assert(started.await(5, TimeUnit.SECONDS))
      assert(blocked.cancel(true))
      executor.submit(new Runnable {
        override def run(): Unit = {}
      }).get(5, TimeUnit.SECONDS)
      gate.releaseReference(1L)

      val retry = gate.acquire(secondContext)
      assert(retry.acquired)
      gate.releaseReference(2L)
      assert(gate.availablePermits === 1)
    } finally {
      executor.shutdownNow()
    }
  }
}
