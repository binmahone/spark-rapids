/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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
{"spark": "321"}
{"spark": "321cdh"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import scala.collection.mutable

import org.apache.spark.sql.util.{SQLOptTraceReporter, TraceEvent}

object SparkShimImpl extends Spark321PlusShims
    with Spark320PlusNonDBShims
    with Spark31Xuntil33XShims
    with AnsiCastRuleShims {

  val bdEventSet: mutable.Set[String] = mutable.Set.empty

  override def reproduceEmptyStringBug: Boolean = true

  override def postFallbackMetrics(
      operationName: String,
      className: String,
      message: String): Unit = {
    if (message.contains("cannot run on GPU") && !bdEventSet.contains(operationName)) {
      bdEventSet.add(operationName)
      val data = Map (
        "type" -> "RapidsFallback",
        "operation" -> operationName,
        "class" -> className,
        "message" -> message
      )
      logInfo(s"send metrics event = $data")
      SQLOptTraceReporter.postImmediately(TraceEvent(data))
    }
  }
}
