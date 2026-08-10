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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "351"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids

import java.io.IOException
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.scalatest.funsuite.AnyFunSuite

class BytesInFlightLimiterSuite extends AnyFunSuite {

  test("abort wakes a blocked acquirer and preserves existing quota") {
    val limiter = new BytesInFlightLimiter(1024)
    assert(limiter.acquire(800))

    val started = new CountDownLatch(1)
    @volatile var failure: Throwable = null
    val blocked = new Thread(() => {
      started.countDown()
      try {
        limiter.acquireOrBlock(800)
      } catch {
        case t: Throwable => failure = t
      }
    })

    blocked.start()
    assert(started.await(5, TimeUnit.SECONDS))
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
    while (blocked.getState != Thread.State.WAITING && System.nanoTime() < deadline) {
      Thread.sleep(10)
    }
    assert(blocked.getState == Thread.State.WAITING)

    val cause = new IOException("injected compression failure")
    limiter.abort(cause)
    blocked.join(5000)

    assert(!blocked.isAlive)
    assert(failure eq cause)
    assert(limiter.getBytesInFlight == 800)
    limiter.release(800)
    assert(limiter.getBytesInFlight == 0)
  }

  test("acquire after abort rolls back quota before propagating the cause") {
    val limiter = new BytesInFlightLimiter(1024)
    val cause = new IOException("injected compression failure")
    limiter.abort(cause)

    val thrown = intercept[IOException] {
      limiter.acquireOrBlock(800)
    }

    assert(thrown eq cause)
    assert(limiter.getBytesInFlight == 0)
  }
}
