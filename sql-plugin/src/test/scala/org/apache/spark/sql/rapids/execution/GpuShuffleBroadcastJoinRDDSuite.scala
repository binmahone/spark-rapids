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
package org.apache.spark.sql.rapids.execution

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class GpuShuffleBroadcastJoinRDDSuite extends AnyFunSuite with BeforeAndAfterAll {
  private var sparkContext: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext = new SparkContext(new SparkConf()
      .setMaster("local[2]")
      .setAppName(getClass.getSimpleName)
      .set("spark.ui.enabled", "false"))
  }

  override def afterAll(): Unit = {
    try {
      if (sparkContext != null) {
        sparkContext.stop()
      }
    } finally {
      super.afterAll()
    }
  }

  test("build shuffle is scheduled through the result RDD dependency graph") {
    val streamRdd = sparkContext.parallelize(Seq(1, 2, 3, 4), 2)
    val buildRdd = sparkContext.parallelize(Seq(10, 20), 2).repartition(1)
    val joinRdd = new GpuShuffleBroadcastJoinRDD(
      streamRdd,
      buildRdd,
      (stream: Iterator[Int], build: Iterator[Int]) => {
        val buildValues = build.toArray
        stream.flatMap(streamValue => buildValues.iterator.map(streamValue + _))
      })

    assert(joinRdd.getNumPartitions == streamRdd.getNumPartitions)
    assert(joinRdd.dependencies.length == 2)
    assert(joinRdd.dependencies.head.getParents(1) == Seq(1))
    assert(joinRdd.dependencies(1).getParents(1) == Seq(0))
    assert(joinRdd.collect().sorted.sameElements(Array(11, 12, 13, 14, 21, 22, 23, 24)))
  }
}
