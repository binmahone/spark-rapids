/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{DType, NvtxColor, NvtxRange, PartitionedTable}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.expressions.{Expression, HiveHash, Murmur3Hash}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.rapids.{GpuHashExpression, GpuHiveHash, GpuMurmur3Hash, GpuPmod}
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class GpuHashPartitioningBase(expressions: Seq[Expression], numPartitions: Int,
    hashMode: HashMode.Value)
  extends GpuExpression with ShimExpression with GpuPartitioning with Serializable {

  override def children: Seq[Expression] = expressions
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  def partitionInternalAndClose(batch: ColumnarBatch): (Array[Int], Array[GpuColumnVector]) = {
    val types = GpuColumnVector.extractTypes(batch)
    val partedTable = GpuHashPartitioningBase.hashPartitionAndClose(batch, expressions,
      numPartitions, "Calculate part", hashMode)
    withResource(partedTable) { partedTable =>
      val parts = partedTable.getPartitions
      val tp = partedTable.getTable
      val columns = (0 until partedTable.getNumberOfColumns.toInt).zip(types).map {
        case (index, sparkType) =>
          GpuColumnVector.from(tp.getColumn(index).incRefCount(), sparkType)
      }
      (parts, columns.toArray)
    }
  }

  override def columnarEvalAny(batch: ColumnarBatch): Any = {
    //  We are doing this here because the cudf partition command is at this level
    withResource(new NvtxRange("Hash partition", NvtxColor.PURPLE)) { _ =>
      val numRows = batch.numRows
      val (partitionIndexes, partitionColumns) = {
        withResource(new NvtxRange("partition", NvtxColor.BLUE)) { _ =>
          partitionInternalAndClose(batch)
        }
      }
      sliceInternalGpuOrCpuAndClose(numRows, partitionIndexes, partitionColumns)
    }
  }

  def partitionIdExpression: GpuExpression = GpuPmod(
    GpuHashPartitioningBase.toHashExpr(hashMode, expressions),
    GpuLiteral(numPartitions))
}

object GpuHashPartitioningBase {

  val DEFAULT_HASH_SEED: Int = 42

  private[rapids] def toHashExpr(hashMode: HashMode.Value, keys: Seq[Expression],
      seed: Int = DEFAULT_HASH_SEED): GpuHashExpression = hashMode match {
    case HashMode.MURMUR3 => GpuMurmur3Hash(keys, seed)
    case HashMode.HIVE => GpuHiveHash(keys)
    case _ => throw new Exception(s"Unsupported hash mode: $hashMode")
  }

  def hashPartitionAndClose(batch: ColumnarBatch, keys: Seq[Expression], numPartitions: Int,
      nvtxName: String, hashMode: HashMode.Value,
      seed: Int = DEFAULT_HASH_SEED): PartitionedTable = {
    val hashExpr = toHashExpr(hashMode, keys, seed)
    val sb = SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    RmmRapidsRetryIterator.withRetryNoSplit(sb) { sb =>
      withResource(sb.getColumnarBatch()) { cb =>
        val parts = withResource(new NvtxRange(nvtxName, NvtxColor.CYAN)) { _ =>
          withResource(hashExpr.columnarEval(cb)) { hash =>
            withResource(GpuScalar.from(numPartitions, IntegerType)) { partsLit =>
              hash.getBase.pmod(partsLit, DType.INT32)
            }
          }
        }
        withResource(parts) { parts =>
          withResource(GpuColumnVector.from(cb)) { table =>
            table.partition(parts, numPartitions)
          }
        }
      }
    }
  }

  private[rapids] def getHashModeFromCpu(cpuHp: HashPartitioning,
      conf: RapidsConf): Either[HashMode.Value, String] = {
    if (!conf.isHashModePartitioningEnabled) {
      return Left(HashMode.MURMUR3)
    }
    // One customized Spark introduces a new field to define the hash algorithm
    // used by HashPartitioning. Since there is no shim for it, so here leverages
    // Java reflection to access it.
    try {
      val hashModeMethod = cpuHp.getClass.getMethod("hashingFunctionClass")
      hashModeMethod.invoke(cpuHp) match {
        case m if m == classOf[Murmur3Hash] => Left(HashMode.MURMUR3)
        case h if h == classOf[HiveHash] => Left(HashMode.HIVE)
        case o => Right(o.asInstanceOf[Class[_]].getSimpleName) // unsupported hash algorithm
      }
    } catch {
      // default to murmur3
      case _: NoSuchMethodException => Left(HashMode.MURMUR3)
    }
  }

}

object HashMode extends Enumeration with Serializable {
  type HashMode = Value
  val MURMUR3, HIVE = Value
}
