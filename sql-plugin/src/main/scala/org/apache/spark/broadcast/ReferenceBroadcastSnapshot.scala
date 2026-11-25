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

import java.lang.{Boolean => JBoolean}
import java.util.{Collections, IdentityHashMap}

import scala.collection.JavaConverters._

import org.apache.spark.SparkEnv
import org.apache.spark.sql.rapids.execution.SerializeConcatHostBuffersDeserializeBatch
import org.apache.spark.storage.BroadcastBlockId

/** Typed Spark 3.5.3 bridge; cachedValues is private to Spark's broadcast package. */
private[spark] object ReferenceBroadcastSnapshot {
  def takeOrdinaryGpuBroadcasts(): Seq[SerializeConcatHostBuffersDeserializeBatch] = {
    val unique = Collections.newSetFromMap(
      new IdentityHashMap[SerializeConcatHostBuffersDeserializeBatch, JBoolean]())
    val values = SparkEnv.get.broadcastManager.cachedValues
    val entries = values.synchronized {
      values.entrySet().asScala.toList.collect {
        case entry if entry.getKey.isInstanceOf[BroadcastBlockId] &&
            entry.getValue.isInstanceOf[SerializeConcatHostBuffersDeserializeBatch] =>
          (entry.getKey.asInstanceOf[BroadcastBlockId],
            entry.getValue.asInstanceOf[SerializeConcatHostBuffersDeserializeBatch])
      }
    }
    entries.foreach { case (id, value) =>
      // Remove host blocks before retirement so a later eviction cannot serialize cleared data.
      SparkEnv.get.blockManager.removeBroadcast(id.broadcastId, tellMaster = true)
      values.synchronized {
        if (values.get(id).asInstanceOf[AnyRef] eq value) {
          values.remove(id)
        }
      }
      unique.add(value)
    }
    unique.asScala.toSeq
  }
}
