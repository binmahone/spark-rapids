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

import org.scalatest.funsuite.AnyFunSuite

class ExecutorInitInstrumentationSuite extends AnyFunSuite {
  test("executor initialization phase records elapsed time") {
    val clockValues = Iterator(1000000L, 8500000L)
    var recorded: Option[(String, Long)] = None

    val result = RapidsExecutorPlugin.timeExecutorInitPhase(
      "test_phase",
      () => clockValues.next(),
      (phase, durationMs) => recorded = Some((phase, durationMs))) {
      "result"
    }

    assert(result === "result")
    assert(recorded.contains(("test_phase", 7L)))
  }

  test("executor initialization phase records failures") {
    val clockValues = Iterator(0L, 3000000L)
    var recorded: Option[(String, Long)] = None

    intercept[IllegalStateException] {
      RapidsExecutorPlugin.timeExecutorInitPhase(
        "failed_phase",
        () => clockValues.next(),
        (phase, durationMs) => recorded = Some((phase, durationMs))) {
        throw new IllegalStateException("expected")
      }
    }

    assert(recorded.contains(("failed_phase", 3L)))
  }

  test("memory initialization phase records elapsed time") {
    val clockValues = Iterator(2000000L, 11200000L)
    var recorded: Option[(String, Long)] = None

    val result = GpuDeviceManager.timeMemoryInitPhase(
      "test_memory_phase",
      () => clockValues.next(),
      (phase, durationMs) => recorded = Some((phase, durationMs))) {
      "result"
    }

    assert(result === "result")
    assert(recorded.contains(("test_memory_phase", 9L)))
  }

  test("memory initialization phase records failures") {
    val clockValues = Iterator(0L, 4000000L)
    var recorded: Option[(String, Long)] = None

    intercept[IllegalArgumentException] {
      GpuDeviceManager.timeMemoryInitPhase(
        "failed_memory_phase",
        () => clockValues.next(),
        (phase, durationMs) => recorded = Some((phase, durationMs))) {
        throw new IllegalArgumentException("expected")
      }
    }

    assert(recorded.contains(("failed_memory_phase", 4L)))
  }
}
