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

import scala.collection.mutable

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.RapidsMeta.gpuSupportedTag
import com.nvidia.spark.rapids.shims.HybridFileSourceScanExecMeta

import org.apache.spark.sql.catalyst.expressions.{And, ConcatWs, Expression, ExpressionSet, Factorial, LengthOfJsonArray, MapFromArrays, PredicateHelper, Sequence, TruncDate}
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, LeafExecNode, SparkPlan}

case class VeloxGpuFilterExecMeta(
  filter: FilterExec,
  override val conf: RapidsConf,
  parentMetaOpt: Option[RapidsMeta[_, _, _]],
  rule: DataFromReplacementRule
) extends SparkPlanMeta[FilterExec](filter, conf, parentMetaOpt, rule) with PredicateHelper {

  private lazy val notSupportedByVeloxFilters = Seq(
    classOf[Factorial],
    classOf[ConcatWs],
    classOf[LengthOfJsonArray],
    classOf[TruncDate],
    classOf[Sequence],
    classOf[MapFromArrays]
  )

  lazy val filters: Seq[Expression] = splitConjunctivePredicates(filter.condition)

  // if the child is a FileSourceScanExec and config is open
  private lazy val canBePushedToVelox: String = {
    filter.child match {
      case fs: FileSourceScanExec if HybridFileSourceScanExecMeta.useHybridScan(conf, fs) =>
        conf.hybridParquetFilterPushDown
      case _ =>
        "UNCHANGED"
    }
  }

  private lazy val containsNotSupportedCondition = {
    filters.exists {filter => notSupportedByVeloxFilters.exists(_.isInstance(filter))}
  }

  override def tagPlanForGpu(): Unit = {
    if (canBePushedToVelox == "ALL_SUPPORTED" && !containsNotSupportedCondition) {
      // if all filters are supported by velox, we can skip the filter, but we need to
      // keep the convertToGpu to do the filter push down
      cannotBeReplacedReasons = Some(mutable.Set.empty)
      wrapped match {
        case p: SparkPlan =>
          p.setTagValue(gpuSupportedTag, Set.empty[String])
        case _ =>
      }
    } else {
      super.tagPlanForGpu()
    }
  }

  private def getRemainingFilters(scanFilters: Seq[Expression],
                                  filters: Seq[Expression]): Seq[Expression] = {
    (ExpressionSet(filters) -- ExpressionSet(scanFilters)).toSeq
  }

  private def postProcessPushDownFilter(
    extraFilters: Seq[Expression],
    sparkExecNode: LeafExecNode): Seq[Expression] = {
    sparkExecNode match {
      case fileSourceScan: FileSourceScanExec =>
        fileSourceScan.dataFilters ++ getRemainingFilters(
          fileSourceScan.dataFilters,
          extraFilters)
      case _ =>
        throw new IllegalStateException("Unexpected plan type")
    }
  }

  private def applyFilterPushdownToScan(filter: FilterExec): Seq[Expression] =
    filter.child match {
      case fileSourceScan: FileSourceScanExec =>
        val filterConditions = splitConjunctivePredicates(filter.condition)
        val pushDownFilters =
          postProcessPushDownFilter(
            filterConditions,
            fileSourceScan)
        pushDownFilters
      case _ =>
        throw new IllegalStateException("Unexpected plan type")
    }

  override def convertToGpu(): GpuExec = {
    (filter.child, canBePushedToVelox) match {
      case (fsse: FileSourceScanExec, "NONE") =>
        val updatedFsseChild = fsse.copy(dataFilters = Seq.empty)
        val updatedFilter = FilterExec(filters.reduceLeft(And), updatedFsseChild)
        val newMeta = VeloxGpuFilterExecMeta(updatedFilter, conf, parentMetaOpt, rule)
        GpuFilterExec(newMeta.childExprs.head.convertToGpu(),
          (new VeloxFileSourceScanExecMeta(updatedFsseChild, conf, parentMetaOpt, rule))
            .convertToGpu())()
      case (fsse: FileSourceScanExec, "ALL_SUPPORTED") =>
        if (containsNotSupportedCondition) {
          // we need to extract the unsupported conditions and push down the rest
          val (notSupportedConditions, supportedConditions) = filters.partition {
            case filter if notSupportedByVeloxFilters.exists(_.isInstance(filter)) =>
              true
            case _ => false
          }
          val updatedFsseChild = fsse.copy(dataFilters = supportedConditions)
          val updatedFilter = FilterExec(notSupportedConditions.reduceLeft(And), updatedFsseChild)
          val newMeta = VeloxGpuFilterExecMeta(updatedFilter, conf, parentMetaOpt, rule)
          GpuFilterExec(newMeta.childExprs.head.convertToGpu(),
            (new VeloxFileSourceScanExecMeta(updatedFsseChild, conf, parentMetaOpt, rule))
              .convertToGpu())()
        } else {
          // the filterExec can be removed and the filter can be pushed down to the scan
          val newCondition = applyFilterPushdownToScan(filter)
          val newScan = fsse.copy(dataFilters = newCondition)
          // The FilterExec might change the outputAttr of the childPlan. Therefore, it is
          // essential to use the outputAttr of FilterExec as the outputAttr of VeloxScanExec if
          // the FilterExec is supposed to be eliminated. Otherwise, schema mismatch will occur
          // when AQE is enabled.
          val meta = new VeloxFileSourceScanExecMeta(newScan,
            conf, parentMetaOpt, rule, Some(filter.output))
          meta.convertToGpu()
        }
      case (_, _) =>
        GpuFilterExec(childExprs.head.convertToGpu(),
          childPlans.head.convertIfNeeded())()
    }
  }
}
