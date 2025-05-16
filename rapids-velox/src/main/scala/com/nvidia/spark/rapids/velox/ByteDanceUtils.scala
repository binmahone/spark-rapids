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

package com.nvidia.spark.rapids.velox

import io.glutenproject.execution._
import io.substrait.proto.Plan

import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.FilePartition

object ByteDanceUtils extends Logging {
  def logGlutenPartition(split: Partition, context: TaskContext): Unit = {
    split match {
      case FirstZippedPartitionsPartition(_, inputPartition, _) =>
        inputPartition match {
          case GlutenPartition(_, plan, _, _) =>
            try {
              val planObj = Plan.parseFrom(plan)
              logInfo("Velox Parquet Scan Plan object: \n" + planObj)
            } catch {
              case _: Throwable => ()
            }
          case GlutenRawPartition(_, _, splitInfos, _) =>
            try {
              splitInfos.foreach { splitInfo =>
                val filePartition = splitInfo.getFilePartition()
                filePartition match {
                  case FilePartition(_, files) =>
                    files.foreach { file =>
                      logWarning("Read parquet file with Velox: " + file +
                        ", task id: " + context.taskAttemptId())
                    }
                  case _ =>
                }
              }
            } catch {
              case _: Throwable => ()
            }
          case _ =>
        }
      case _ =>
    }
  }
}
