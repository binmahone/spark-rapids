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

import org.scalatest.funsuite.AnyFunSuite

class RapidsShuffleReaderSchedulingSuite extends AnyFunSuite {
  test("a ready result bypasses an older background future") {
    assert(!RapidsShuffleInternalManagerBase.shouldWaitForBackgroundFuture(
      hasPendingFuture = true,
      hasReadyResult = true))
  }

  test("an empty result queue waits for pending background work") {
    assert(RapidsShuffleInternalManagerBase.shouldWaitForBackgroundFuture(
      hasPendingFuture = true,
      hasReadyResult = false))
  }

  test("no pending future never triggers a future wait") {
    assert(!RapidsShuffleInternalManagerBase.shouldWaitForBackgroundFuture(
      hasPendingFuture = false,
      hasReadyResult = false))
    assert(!RapidsShuffleInternalManagerBase.shouldWaitForBackgroundFuture(
      hasPendingFuture = false,
      hasReadyResult = true))
  }
}
