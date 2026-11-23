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

package com.nvidia.spark.rapids.bench;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import scala.None$;
import scala.Option;
import scala.Some;
import scala.collection.immutable.Map$;
import scala.math.BigInt;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat;
import org.apache.spark.sql.catalyst.catalog.CatalogStatistics;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/** Benchmark bridge for installing externally computed statistics in a Spark session catalog. */
public final class CatalogStatsBridge {
  private CatalogStatsBridge() {}

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static void install(
      SparkSession spark,
      String table,
      String rowCount,
      Map<String, String> distinctCounts) throws Exception {
    install(
        spark,
        table,
        rowCount,
        distinctCounts,
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static void install(
      SparkSession spark,
      String table,
      String rowCount,
      Map<String, String> distinctCounts,
      Map<String, String> minimumValues,
      Map<String, String> maximumValues,
      Map<String, String> nullCounts) throws Exception {
    LogicalPlan plan = spark.table(table).queryExecution().optimizedPlan();
    BigInt sizeInBytes = plan.stats().sizeInBytes();
    BigInt rows = new BigInt(new BigInteger(rowCount));
    scala.collection.immutable.Map<String, CatalogColumnStat> columnStats =
        Map$.MODULE$.empty();
    Option none = None$.MODULE$;
    Set<String> columns = new TreeSet<>();
    columns.addAll(distinctCounts.keySet());
    columns.addAll(minimumValues.keySet());
    columns.addAll(maximumValues.keySet());
    columns.addAll(nullCounts.keySet());
    for (String column : columns) {
      Option ndv = distinctCounts.containsKey(column)
          ? new Some<>(new BigInt(new BigInteger(distinctCounts.get(column))))
          : none;
      Option minimum = minimumValues.containsKey(column)
          ? new Some<>(minimumValues.get(column))
          : none;
      Option maximum = maximumValues.containsKey(column)
          ? new Some<>(maximumValues.get(column))
          : none;
      Option nullCount = nullCounts.containsKey(column)
          ? new Some<>(new BigInt(new BigInteger(nullCounts.get(column))))
          : none;
      CatalogColumnStat columnStat = new CatalogColumnStat(
          ndv, minimum, maximum, nullCount, none, none, none, 2);
      columnStats = columnStats.updated(column, columnStat);
    }
    CatalogStatistics statistics = new CatalogStatistics(
        sizeInBytes, new Some<>(rows), columnStats);
    TableIdentifier identifier = spark.sessionState().sqlParser().parseTableIdentifier(table);
    spark.sessionState().catalog().alterTableStats(identifier, new Some<>(statistics));
    spark.catalog().refreshTable(table);
  }
}
