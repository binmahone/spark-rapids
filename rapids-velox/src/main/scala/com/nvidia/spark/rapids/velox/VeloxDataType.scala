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

import org.apache.spark.sql.types._

/**
 * Checks and conversions between Velox native types and Spark types
 *
 * @param sparkTypes the types that Velox type may map to
 */
sealed abstract class VeloxDataType(val sparkTypes: Seq[DataType]) {
  /**
   * Can this Velox Native type be mapped to the specified Spark Type
   *
   * @param dst Spark type
   * @return true if Velox Native type can be mapped to, false otherwise.
   */
  def canConvert(dst: DataType): Boolean = sparkTypes.contains(dst)
}

object VeloxDataType {

  private case object BOOLEAN extends VeloxDataType(Seq(BooleanType))

  private case object TINYINT extends VeloxDataType(Seq(ByteType))

  private case object SMALLINT extends VeloxDataType(Seq(ShortType))

  private case object INTEGER extends VeloxDataType(Seq(IntegerType, DateType))

  private case object BIGINT extends VeloxDataType(Seq(LongType)) {
    override def canConvert(dst: DataType): Boolean = dst match {
      case d: DecimalType =>
        // velox stores 32/64 bits decimal as long
        DecimalType.is32BitDecimalType(d) || DecimalType.is64BitDecimalType(d)
      case _ => super.canConvert(dst)
    }
  }

  private case object REAL extends VeloxDataType(Seq(FloatType))

  private case object DOUBLE extends VeloxDataType(Seq(DoubleType))

  private case object VARCHAR extends VeloxDataType(Seq(StringType))

  private case object VARBINARY extends VeloxDataType(Seq(StringType))

  private case object TIMESTAMP extends VeloxDataType(Seq(TimestampType))

  private case object HUGEINT extends VeloxDataType(Nil) {
    override def canConvert(dst: DataType): Boolean = dst match {
      case d: DecimalType =>
        // velox stores 128 bits decimal as huge int
        !DecimalType.is32BitDecimalType(d) && !DecimalType.is64BitDecimalType(d)
      case _ => false
    }
  }

  // not support, return false; Velox always map date to INTEGER, so will never get this
  private case object DATE extends VeloxDataType(Nil)

  private case object ARRAY extends VeloxDataType(Nil) {
    override def canConvert(dst: DataType): Boolean = dst.isInstanceOf[ArrayType]
  }

  private case object MAP extends VeloxDataType(Nil) {
    override def canConvert(dst: DataType): Boolean = dst.isInstanceOf[MapType]
  }

  private case object ROW extends VeloxDataType(Nil) {
    override def canConvert(dst: DataType): Boolean = dst.isInstanceOf[StructType]
  }

  // not support, return false
  private case object UNKNOWN extends VeloxDataType(Nil)

  // not support, return false
  private case object FUNCTION extends VeloxDataType(Nil)

  // not support, return false
  private case object OPAQUE extends VeloxDataType(Nil)

  // not support, return false
  private case object INVALID extends VeloxDataType(Nil)

  /**
   * Convert Velox native type id to VeloxDataType
   *
   * @param veloxNativeTypeId Velox native type, this is from Velox code:
   *                          https://github.com/facebookincubator/velox/blob/main/velox/type/Type.h
   * @return VeloxDataType which contains Spark types that can be mapped to
   */
  def decodeVeloxType(veloxNativeTypeId: Int): VeloxDataType = veloxNativeTypeId match {
    case 0 => BOOLEAN
    case 1 => TINYINT
    case 2 => SMALLINT
    case 3 => INTEGER
    case 4 => BIGINT
    case 5 => REAL
    case 6 => DOUBLE
    case 7 => VARCHAR
    case 8 => VARBINARY
    case 9 => TIMESTAMP
    case 10 => HUGEINT
    case 11 => DATE
    case 30 => ARRAY
    case 31 => MAP
    case 32 => ROW
    case 33 => UNKNOWN
    case 34 => FUNCTION
    case 35 => OPAQUE
    case 36 => INVALID
    case _ =>
      throw new IllegalArgumentException(s"Invalid $veloxNativeTypeId for VeloxDataType")
  }

  /**
   * Used by Hybrid JNI data transfer, should be consistent with types in: `ConversionBasics.h`,
   *
   * @param dataType Spark data byte
   * @return Type used in Hybrid JNI
   */
  def encodeSparkType(dataType: DataType): Int = dataType match {
    case BooleanType => 1
    case ByteType => 2
    case ShortType => 3
    case IntegerType => 4
    case LongType => 5
    case FloatType => 6
    case DoubleType => 7
    case d: DecimalType if DecimalType.is32BitDecimalType(d) => 8
    case d: DecimalType if DecimalType.is64BitDecimalType(d) => 9
    case _: DecimalType => 10
    case StringType => 11
    case DateType => 12
    case TimestampType => 13
    case _: ArrayType => 101
    case _: MapType => 102
    case _: StructType => 103
    case _ =>
      throw new IllegalArgumentException(s"Unsupported SparkType($dataType)")
  }
}
