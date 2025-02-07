/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.velox

import io.glutenproject.execution.{FileSourceScanExecTransformer, WholeStageTransformer}
import io.glutenproject.rapids.GlutenJniWrapper

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch

object VeloxBackendApis {
  // Mark if VeloxBackend is enabled
  private var isEnabled: Boolean = false

  private[velox] def init(conf: SparkConf): Unit = synchronized {
    isEnabled = true
  }

  def getRuntime: Option[GlutenJniWrapper] = {
    if (isEnabled) {
      require(Option(TaskContext.get()).nonEmpty,
        "VeloxBackendApis should only run inside Spark Executors")
      Some(GlutenJniWrapper.create())
    } else {
      None
    }
  }

  def overrideFileSourceScanExec(scanExec: FileSourceScanExec): SparkPlan = {
    new FileSourceScanExecTransformer(
      scanExec.relation,
      scanExec.output,
      scanExec.requiredSchema,
      scanExec.partitionFilters,
      scanExec.optionalBucketSet,
      scanExec.optionalNumCoalescedBuckets,
      scanExec.dataFilters,
      scanExec.tableIdentifier,
      scanExec.disableBucketedScan
    )
  }

  def executeNativePlan(nativePlan: SparkPlan): RDD[ColumnarBatch] = {
    val pipeline = WholeStageTransformer(nativePlan, materializeInput = false)(1)
    pipeline.doExecuteColumnar()
  }

  def getNativeScanMetadata(nativePlan: SparkPlan): Map[String, String] = nativePlan match {
    case scan: FileSourceScanExecTransformer => scan.metadata
    case _ => Map.empty
  }
}
