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

package org.apache.spark.sql.rapids

import java.nio.file.Files

import com.nvidia.spark.rapids.GpuRequiredKeyBloomFilter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.BloomFilterMightContain
import org.apache.spark.sql.execution.FilterExec

/** CPU semantic and reachability probe; run through the experiment's saved build script. */
object RequiredKeyBloomFilterProbe extends Logging {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]")
      .appName("required-key-bloom-cpu-probe")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.optimizer.runtime.bloomFilter.enabled", "false")
      .config("spark.sql.extensions", "com.nvidia.spark.rapids.GpuRequiredKeyBloomExtension")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val prefix = "spark.rapids.sql.optimizer.requiredKeyBloomFilter."
    val root = Files.createTempDirectory("required-key-bloom-")
    try {
      val target = spark.sql("""SELECT * FROM VALUES
        (1L, 1, 'one-a'), (1L, 2, 'one-b'), (2L, 1, 'two'),
        (3L, 1, 'three'), (4L, 1, 'four'), (NULL, 1, 'null') AS t(k, z, v)""")
      val source = spark.sql("""SELECT * FROM VALUES
        (1L, 1, 1, 4), (1L, 2, 1, 5), (2L, 1, 0, 2),
        (3L, 1, 1, 1), (NULL, 1, 1, 8) AS s(k, z, flag, amount)""")
      target.write.parquet(root.resolve("target").toString)
      source.write.parquet(root.resolve("source").toString)
      spark.read.parquet(root.resolve("target").toString).createOrReplaceTempView("target")
      spark.read.parquet(root.resolve("source").toString).createOrReplaceTempView("source")

      val filtered = "(SELECT * FROM source WHERE flag = 1)"
      val grouped = "(SELECT k, sum(amount) AS total FROM source GROUP BY k HAVING sum(amount) > 3)"
      val cases = Seq(
        ("inner-duplicates", s"SELECT t.* FROM target t JOIN $filtered s ON t.k=s.k", true),
        ("grouped-having", s"SELECT t.* FROM target t JOIN $grouped s ON t.k=s.k", true),
        ("grouped-in-subquery", "SELECT t.* FROM target t WHERE t.k IN " +
          "(SELECT k FROM source GROUP BY k HAVING sum(amount)>3)", true),
        ("empty-source", "SELECT t.* FROM target t JOIN " +
          "(SELECT * FROM source WHERE flag=99) s ON t.k=s.k", true),
        ("composite-keys", s"SELECT t.* FROM target t JOIN $filtered s " +
          "ON t.k=s.k AND t.z=s.z", true),
        ("residual", s"SELECT t.* FROM target t JOIN $filtered s " +
          "ON t.k=s.k AND t.z < s.amount", true),
        ("semi", s"SELECT t.* FROM target t LEFT SEMI JOIN $filtered s ON t.k=s.k", true),
        ("left-outer", s"SELECT t.* FROM target t LEFT JOIN $filtered s ON t.k=s.k", false),
        ("full-outer", s"SELECT t.* FROM target t FULL JOIN $filtered s ON t.k=s.k", false),
        ("anti", s"SELECT t.* FROM target t LEFT ANTI JOIN $filtered s ON t.k=s.k", false),
        ("null-safe", s"SELECT t.* FROM target t JOIN $filtered s ON t.k <=> s.k", false),
        ("nondeterministic-source", "SELECT t.* FROM target t JOIN " +
          "(SELECT * FROM source WHERE flag=1 AND rand(42)<0.5) s ON t.k=s.k", false))

      cases.foreach { case (name, sql, shouldFilter) =>
        spark.conf.set(prefix + "enabled", "false")
        val reference = spark.sql(sql).collect().map(_.toString).sorted.toSeq
        val original = spark.sql(sql).queryExecution.optimizedPlan
        assert(!original.exists(_.expressions.exists(_.exists(
          _.isInstanceOf[BloomFilterMightContain]))), name + ": disabled path added a filter")
        spark.conf.set(prefix + "enabled", "true")
        val rewritten = spark.sql(sql)
        val plan = rewritten.queryExecution.optimizedPlan
        val hasFilter = plan.exists(_.expressions.exists(_.exists(
          _.isInstanceOf[BloomFilterMightContain])))
        assert(hasFilter == shouldFilter, name + ": unexpected eligibility\n" + plan)
        val actual = rewritten.collect().map(_.toString).sorted.toSeq
        assert(reference == actual, name + ": result mismatch " + reference + " vs " + actual)
        if (shouldFilter) {
          val twice = GpuRequiredKeyBloomFilter(spark)(plan)
          assert(twice.fastEquals(plan), name + ": repeated application changed the plan")
          assert(rewritten.queryExecution.executedPlan.toString.contains("might_contain"),
            name + ": logical filter did not reach physical execution")
          if (name == "inner-duplicates") {
            val pruned = rewritten.queryExecution.executedPlan.collect {
              case filter: FilterExec if filter.child.output.exists(_.name == "v") &&
                  filter.condition.exists(_.isInstanceOf[BloomFilterMightContain]) =>
                filter.metrics("numOutputRows").value
            }
            assert(pruned.nonEmpty && pruned.forall(_ == 3),
              name + ": the filter did not reduce the six-row target: " + pruned)
          }
        }
        logWarning("PROBE_PASS " + name + " rows=" + actual.size + " filter=" + hasFilter)
      }
      logWarning("PROBE_ALL_PASS cases=" + cases.size)
    } finally {
      spark.stop()
      val paths = Files.walk(root)
      try {
        import scala.collection.JavaConverters._
        paths.iterator().asScala.toSeq.sortBy(_.getNameCount).reverse.foreach(Files.delete)
      } finally {
        paths.close()
      }
    }
  }
}
