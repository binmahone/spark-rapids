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
package org.apache.spark.rapids.shims.bd

import com.nvidia.spark.rapids._

import org.apache.spark.rdd.{RDD, RoundRobinPartitionCoalescer}
import org.apache.spark.sql.execution.{RoundRobingCoalesceExec, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuRoundRobingCoalesceMeta(plan: RoundRobingCoalesceExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends SparkPlanMeta[RoundRobingCoalesceExec](plan, conf, parent, rule) {
  override def convertToGpu(): GpuExec = {
    new GpuRoundRobingCoalesceExec(plan.numPartitions, childPlans.head.convertIfNeeded())
  }
}

class GpuRoundRobingCoalesceExec(
    override val numPartitions: Int,
    override val child: SparkPlan) extends GpuCoalesceExec(numPartitions, child) {
  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val rdd = child.executeColumnar()
    if (numPartitions == 1 && rdd.getNumPartitions < 1) {
      // Make sure we don't output an RDD with 0 partitions, when claiming that we have a
      // `SinglePartition`.
      new GpuCoalesceExec.EmptyRDDWithPartitions(sparkContext, numPartitions)
    } else {
      rdd.coalesce(numPartitions, shuffle = false,
        Some(new RoundRobinPartitionCoalescer()))
    }
  }
}
