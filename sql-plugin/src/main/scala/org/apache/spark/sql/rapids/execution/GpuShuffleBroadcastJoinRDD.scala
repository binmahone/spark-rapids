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

import java.io.{IOException, ObjectOutputStream}

import scala.reflect.ClassTag

import org.apache.spark.{Dependency, NarrowDependency, OneToOneDependency, Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

private class GpuShuffleBroadcastJoinPartition(
    override val index: Int,
    @transient private val streamRdd: RDD[_],
    @transient private val buildRdd: RDD[_]) extends Partition {
  var streamPartition: Partition = streamRdd.partitions(index)
  var buildPartition: Partition = buildRdd.partitions(0)

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    streamPartition = streamRdd.partitions(index)
    buildPartition = buildRdd.partitions(0)
    oos.defaultWriteObject()
  }
}

/**
 * Preserves the streamed RDD's partitioning while making a single-partition build RDD part of
 * the same Spark job. Every streamed partition consumes build partition zero.
 */
private[execution] class GpuShuffleBroadcastJoinRDD[A: ClassTag, B: ClassTag, C: ClassTag](
    private var streamRdd: RDD[A],
    private var buildRdd: RDD[B],
    join: (Iterator[A], Iterator[B]) => Iterator[C])
    extends RDD[C](streamRdd.context, Nil) {

  require(buildRdd.getNumPartitions == 1,
    s"shuffle broadcast build RDD must have one partition, found ${buildRdd.getNumPartitions}")

  override protected def getPartitions: Array[Partition] = streamRdd.partitions.map { partition =>
    new GpuShuffleBroadcastJoinPartition(partition.index, streamRdd, buildRdd)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val joinPartition = split.asInstanceOf[GpuShuffleBroadcastJoinPartition]
    streamRdd.preferredLocations(joinPartition.streamPartition)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[C] = {
    val joinPartition = split.asInstanceOf[GpuShuffleBroadcastJoinPartition]
    val streamIter = streamRdd.iterator(joinPartition.streamPartition, context)
    val buildIter = buildRdd.iterator(joinPartition.buildPartition, context)
    join(streamIter, buildIter)
  }

  override def getDependencies: Seq[Dependency[_]] = Seq(
    new OneToOneDependency(streamRdd),
    new NarrowDependency[B](buildRdd) {
      override def getParents(partitionId: Int): Seq[Int] = Seq(0)
    })

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    streamRdd = null
    buildRdd = null
  }
}
