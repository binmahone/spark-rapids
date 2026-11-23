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

package org.apache.spark.broadcast

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.sql.rapids.execution.SerializeConcatHostBuffersDeserializeBatch
import org.apache.spark.storage.{BroadcastBlockId, StorageLevel}

class ReferenceBroadcastSnapshotSuite extends AnyFunSuite {
  test("taking GPU broadcasts removes their cache and host blocks and preserves other broadcasts") {
    val conf = new SparkConf(false).setMaster("local[1]").setAppName("broadcast-retirement")
      .set("spark.ui.enabled", "false")
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.driver.bindAddress", "127.0.0.1")
    val sc = new SparkContext(conf)
    val value = new SerializeConcatHostBuffersDeserializeBatch(null, Seq.empty, 10, 0L)
    try {
      val env = SparkEnv.get
      val gpuId = BroadcastBlockId(1000001L)
      val otherId = BroadcastBlockId(1000002L)
      env.broadcastManager.cachedValues.put(gpuId, value)
      env.broadcastManager.cachedValues.put(otherId, "keep")
      assert(env.blockManager.putSingle(gpuId, value, StorageLevel.MEMORY_ONLY,
        tellMaster = false))
      assert(env.blockManager.putSingle(otherId, "keep", StorageLevel.MEMORY_ONLY,
        tellMaster = false))
      assert(env.blockManager.getStatus(gpuId).isDefined)
      val taken = ReferenceBroadcastSnapshot.takeOrdinaryGpuBroadcasts()
      assert(taken.size == 1 && (taken.head eq value))
      assert(env.broadcastManager.cachedValues.get(gpuId) == null)
      assert(env.blockManager.getStatus(gpuId).isEmpty)
      assert(env.broadcastManager.cachedValues.get(otherId) == "keep")
      assert(env.blockManager.getStatus(otherId).isDefined)
      assert(ReferenceBroadcastSnapshot.takeOrdinaryGpuBroadcasts().isEmpty)
    } finally {
      value.closeInternal()
      sc.stop()
    }
  }
}
