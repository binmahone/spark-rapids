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

import com.nvidia.spark.rapids._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids._
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
    val shouldNotSupportOp = Set(
      "com.bytedance.tqs.datasource.CustomCSVFileFormat",
      "org.apache.spark.sql.execution.LocalTableScanExec",
      "cannot run on GPU because not all data writing commands can be replaced"
    )
    if (message.contains("cannot run on GPU") && !bdEventSet.contains(operationName) &&
        !shouldNotSupportOp.exists(msg => message.contains(msg))) {
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

  /**
   * Get Spark 321 specific expressions
   */
  private def exprsFor321: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[ReorderMapKey](
      "Sort map column according to keys in each map",
      ExprChecks.unaryProject(
        TypeSig.MAP.nested((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT).nested()),
        TypeSig.MAP.nested(TypeSig.all),
        TypeSig.MAP.nested((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT).nested()),
        TypeSig.MAP.nested(TypeSig.all)),
      (a, conf, p, r) => new UnaryExprMeta[ReorderMapKey](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = {
          GpuReorderMapKey(child)
        }
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap

  /**
   * Get expressions from base class and append Spark 321 specific expressions
   */
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    super.getExprs ++ exprsFor321
  }

  private def execsFor321: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
    GpuOverrides.exec[HashAggregateExec](
      "The backend for hash based aggregations",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
          TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
        TypeSig.all),
      (agg, conf, p, r) => new GpuHashAggregateMeta(agg, conf, p, r)),

    GpuOverrides.exec[SortAggregateExec](
      "The backend for sort based aggregations",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
          TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.BINARY).nested(),
        TypeSig.all),
      (agg, conf, p, r) => new GpuSortAggregateExecMeta(agg, conf, p, r))
  ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    super.getExecs ++ execsFor321
  }
}
