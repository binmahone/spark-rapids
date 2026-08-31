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

import java.io.{ByteArrayOutputStream, IOException}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.scalatest.funsuite.AnyFunSuite

class SharedBytesInFlightLimiterSuite extends AnyFunSuite {

  test("shuffle compression buffer writes all accumulated bytes") {
    val buffer = RapidsShuffleInternalManagerBase.newShuffleCompressionBuffer(32)
    val expected = Array.tabulate[Byte](4097)(index => (index % 251).toByte)
    buffer.write(expected)

    val destination = new ByteArrayOutputStream()
    buffer.writeTo(destination)

    assert(buffer.size() == expected.length)
    assert(destination.toByteArray.sameElements(expected))
  }

  test("shared limiter bounds aggregate quota across writers") {
    val limiter = new SharedBytesInFlightLimiter(1024)
    limiter.acquireOrBlock(800, None)

    val started = new CountDownLatch(1)
    @volatile var acquired = false
    val blocked = new Thread(() => {
      started.countDown()
      limiter.acquireOrBlock(800, None)
      acquired = true
      limiter.release(800)
    })

    blocked.start()
    assert(started.await(5, TimeUnit.SECONDS))
    awaitWaiting(blocked)
    assert(limiter.getBytesInFlight == 800)

    limiter.release(800)
    blocked.join(5000)
    assert(!blocked.isAlive)
    assert(acquired)
    assert(limiter.getBytesInFlight == 0)
  }

  test("shared limiter propagates writer-local abort") {
    val limiter = new SharedBytesInFlightLimiter(1024)
    limiter.acquireOrBlock(800, None)
    @volatile var cause: Option[Throwable] = None
    @volatile var failure: Throwable = null
    val started = new CountDownLatch(1)
    val blocked = new Thread(() => {
      started.countDown()
      try {
        limiter.acquireOrBlock(800, cause)
      } catch {
        case t: Throwable => failure = t
      }
    })

    blocked.start()
    assert(started.await(5, TimeUnit.SECONDS))
    awaitWaiting(blocked)

    val expected = new IOException("writer aborted")
    cause = Some(expected)
    limiter.notifyWaiters()
    blocked.join(5000)

    assert(!blocked.isAlive)
    assert(failure eq expected)
    assert(limiter.getBytesInFlight == 800)
    limiter.release(800)
  }

  private def awaitWaiting(thread: Thread): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
    while (thread.getState != Thread.State.WAITING && System.nanoTime() < deadline) {
      Thread.sleep(10)
    }
    assert(thread.getState == Thread.State.WAITING)
  }
}
