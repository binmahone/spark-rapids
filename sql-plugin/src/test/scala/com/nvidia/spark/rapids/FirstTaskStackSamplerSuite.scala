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
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkConf, TaskContext}

import org.mockito.Mockito.when

class FirstTaskStackSamplerSuite extends AnyFunSuite with MockitoSugar {
  test("diagnostic sampler settings are bounded and disabled by default") {
    val disabled = FirstTaskStackSampler.create(new SparkConf(false), "1")
    assert(disabled.isEmpty)

    val conf = new SparkConf(false)
      .set(FirstTaskStackSampler.ENABLED_KEY, "true")
      .set(FirstTaskStackSampler.DURATION_MS_KEY, "4000")
      .set(FirstTaskStackSampler.INTERVAL_MS_KEY, "10")
      .set(FirstTaskStackSampler.REGISTRATION_WINDOW_MS_KEY, "250")
      .set(FirstTaskStackSampler.MAX_DEPTH_KEY, "48")
      .set(FirstTaskStackSampler.TOP_STACK_COUNT_KEY, "12")
      .set(FirstTaskStackSampler.MAX_TASKS_KEY, "32")

    val settings = FirstTaskStackSampler.parseSettings(conf)
    assert(settings.durationMs === 4000L)
    assert(settings.intervalMs === 10L)
    assert(settings.registrationWindowMs === 250L)
    assert(settings.maxDepth === 48)
    assert(settings.topStackCount === 12)
    assert(settings.maxTasks === 32)
    assert(FirstTaskStackSampler.create(conf, "1").nonEmpty)
  }

  test("invalid sampling interval fails closed") {
    val conf = new SparkConf(false)
      .set(FirstTaskStackSampler.INTERVAL_MS_KEY, "1")
    val error = intercept[IllegalArgumentException] {
      FirstTaskStackSampler.parseSettings(conf)
    }
    assert(error.getMessage.contains(FirstTaskStackSampler.INTERVAL_MS_KEY))
  }

  test("stack classification identifies production-path waits") {
    def frame(className: String, methodName: String): StackTraceElement =
      new StackTraceElement(className, methodName, "Test.scala", 1)

    assert(FirstTaskStackSampler.classify(Array(
      frame("com.nvidia.spark.rapids.GpuSemaphore", "acquireIfNecessary"))) ===
      "gpu_semaphore")
    assert(FirstTaskStackSampler.classify(Array(
      frame("org.apache.spark.broadcast.TorrentBroadcast", "readBroadcastBlock"))) ===
      "broadcast_block_manager")
    assert(FirstTaskStackSampler.classify(Array(
      frame("java.lang.ClassLoader", "loadClass"))) === "class_loading")
    assert(FirstTaskStackSampler.classify(Array(
      frame("com.esotericsoftware.kryo.Kryo", "readClassAndObject"))) === "serializer")
    assert(FirstTaskStackSampler.classify(Array(
      frame("java.util.concurrent.FutureTask", "get"))) === "async_wait")
    assert(FirstTaskStackSampler.classify(Array(
      frame("com.nvidia.spark.rapids.GpuFileScanRDD", "compute"))) === "production_scan")
  }

  test("collapsed stacks contain metric-safe method identities") {
    val stack = Array(
      new StackTraceElement("example.Outer$Inner", "run task", "Test.scala", 1),
      new StackTraceElement("example.Reader", "next", "Test.scala", 2))
    assert(FirstTaskStackSampler.collapseStack(stack) ===
      "example.Outer_Inner.run_task>example.Reader.next")
  }

  test("sampler observes a registered task thread and reaches completion") {
    val context = mock[TaskContext]
    when(context.stageId()).thenReturn(3)
    when(context.taskAttemptId()).thenReturn(7L)
    val settings = FirstTaskStackSampler.Settings(
      durationMs = 100L,
      intervalMs = 5L,
      registrationWindowMs = 50L,
      maxDepth = 8,
      topStackCount = 2,
      maxTasks = 1)
    val sampler = new FirstTaskStackSampler.Sampler(settings, "test-executor")

    sampler.onTaskStart(context)
    assert(sampler.await(2000L))
    sampler.onTaskEnd()
  }
}
