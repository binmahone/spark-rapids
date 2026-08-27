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

import scala.collection.mutable.ArrayBuffer

import org.scalatest.funsuite.AnyFunSuite

class ColdStartWriteInstrumentationSuite extends AnyFunSuite {
  test("disabled instrumentation evaluates the phase without emitting a metric") {
    val metrics = ArrayBuffer.empty[ColdStartWriteInstrumentation.Metric]
    ColdStartWriteInstrumentation.setMetricObserver(metrics += _)
    try {
      val instrumentation = new ColdStartWriteInstrumentation(false, "17", "gs://bucket/output")
      assert(instrumentation.phase("test") { 41 + 1 } === 42)
      instrumentation.event("entered")
      assert(metrics.isEmpty)
    } finally {
      ColdStartWriteInstrumentation.resetMetricObserver()
    }
  }

  test("enabled instrumentation emits events and successful phases") {
    val metrics = ArrayBuffer.empty[ColdStartWriteInstrumentation.Metric]
    ColdStartWriteInstrumentation.setMetricObserver(metrics += _)
    try {
      val instrumentation = new ColdStartWriteInstrumentation(true, "23", "gs://bucket/output")
      instrumentation.event("entered")
      assert(instrumentation.phase("prepare") { "result" } === "result")

      assert(metrics.map(_.event) === Seq("entered", "phase"))
      assert(metrics.last.phase === "prepare")
      assert(metrics.last.outcome === "success")
      assert(metrics.last.queryExecutionId === "23")
      assert(metrics.last.durationNs >= 0)
    } finally {
      ColdStartWriteInstrumentation.resetMetricObserver()
    }
  }

  test("failed phases emit the error class and preserve the exception") {
    val metrics = ArrayBuffer.empty[ColdStartWriteInstrumentation.Metric]
    ColdStartWriteInstrumentation.setMetricObserver(metrics += _)
    try {
      val instrumentation = new ColdStartWriteInstrumentation(true, "29", "gs://bucket/output")
      val error = intercept[IllegalStateException] {
        instrumentation.phase("failure") {
          throw new IllegalStateException("expected")
        }
      }

      assert(error.getMessage === "expected")
      assert(metrics.length === 1)
      assert(metrics.head.outcome === "failure")
      assert(metrics.head.errorClass === classOf[IllegalStateException].getName)
    } finally {
      ColdStartWriteInstrumentation.resetMetricObserver()
    }
  }
}
