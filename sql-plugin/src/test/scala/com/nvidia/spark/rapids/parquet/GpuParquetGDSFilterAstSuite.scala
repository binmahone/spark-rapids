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

package com.nvidia.spark.rapids.parquet

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.sources.{Filter, GreaterThanOrEqual, IsNotNull,
  LessThan}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

class GpuParquetGDSFilterAstSuite extends AnyFunSuite {
  private val schema = StructType(Seq(StructField("value", DoubleType, nullable = true)))
  private val cudfColumnNames = Array("value")

  private def compile(filters: Array[Filter]): GpuParquetGDSFilterAst.CompiledFilter = {
    GpuParquetGDSFilterAst.compile(filters, schema, cudfColumnNames, isCaseSensitive = true) match {
      case Right(compiled) => compiled
      case Left(reason) => fail(reason)
    }
  }

  test("elide IsNotNull when a pushed range predicate rejects nulls") {
    val notNull = IsNotNull("value")
    val lower = GreaterThanOrEqual("value", 0.05d)
    val upper = LessThan("value", 0.08d)
    val compiled = compile(Array(notNull, lower, upper))
    try {
      assert(compiled.pushedFilters == Seq(lower, upper))
      assert(compiled.skippedFilters == Seq(
        notNull -> "redundant with another null-rejecting pushed predicate"))
    } finally {
      compiled.expression.close()
    }
  }

  test("push IsNotNull when it is the only predicate for a column") {
    val notNull = IsNotNull("value")
    val compiled = compile(Array(notNull))
    try {
      assert(compiled.pushedFilters == Seq(notNull))
      assert(compiled.skippedFilters.isEmpty)
    } finally {
      compiled.expression.close()
    }
  }
}
