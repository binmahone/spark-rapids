/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
{"spark": "320"}
{"spark": "321"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims.bd

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.HybridFileSourceScanExecMeta

import org.apache.spark.rapids.velox.VeloxFileSourceScanExec
import org.apache.spark.sql.catalyst.expressions.{Attribute, DynamicPruningExpression}
import org.apache.spark.sql.execution._

class VeloxFileSourceScanExecMeta(plan: FileSourceScanExec,
                                  conf: RapidsConf,
                                  parent: Option[RapidsMeta[_, _, _]],
                                  rule: DataFromReplacementRule,
                                  pushedFilterSchema: Option[Seq[Attribute]] = None)
  extends HybridFileSourceScanExecMeta(plan, conf, parent, rule) {

  // Replaces SubqueryBroadcastExec inside dynamic pruning filters with native counterpart
  // if possible. Instead regarding filters as childExprs of current Meta, we create
  // a new meta for SubqueryBroadcastExec. The reason is that the native replacement of
  // FileSourceScan is independent from the replacement of the partitionFilters.
  private lazy val partitionFilters = {
    val convertBroadcast = (bc: SubqueryBroadcastExec) => {
      val meta = GpuOverrides.wrapAndTagPlan(bc, conf)
      meta.tagForExplain()
      meta.convertIfNeeded().asInstanceOf[BaseSubqueryExec]
    }
    wrapped.partitionFilters.map { filter =>
      filter.transformDown {
        case dpe@DynamicPruningExpression(inSub: InSubqueryExec) =>
          inSub.plan match {
            case bc: SubqueryBroadcastExec =>
              dpe.copy(inSub.copy(plan = convertBroadcast(bc)))
            case reuse@ReusedSubqueryExec(bc: SubqueryBroadcastExec) =>
              dpe.copy(inSub.copy(plan = reuse.copy(convertBroadcast(bc))))
            case _ =>
              dpe
          }
      }
    }
  }

  override def convertToGpu(): GpuExec = {
    // Modifies the original plan to support DPP
    val fixedExec = wrapped.copy(
      // fix the inconsistent schema due to Filter Elimination on AQE
      output = pushedFilterSchema.getOrElse(wrapped.output),
      partitionFilters = partitionFilters)

    VeloxFileSourceScanExec(fixedExec)(conf)
  }
}
