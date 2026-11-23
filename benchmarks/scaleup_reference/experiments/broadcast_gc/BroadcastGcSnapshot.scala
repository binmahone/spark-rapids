/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package org.apache.spark.broadcast

import scala.collection.JavaConverters._

import org.apache.spark.SparkEnv
import org.apache.spark.sql.rapids.execution.SerializeConcatHostBuffersDeserializeBatch
import org.apache.spark.storage.BroadcastBlockId

/** Spark 3.5.3 diagnostic observer. No cache removals or retained broadcast references. */
object BroadcastGcSnapshot {
  def records(): String = {
    val env = SparkEnv.get
    val values = env.broadcastManager.cachedValues
    val identities = values.synchronized {
      values.entrySet().asScala.iterator.flatMap { entry =>
        entry.getKey match {
          case id: BroadcastBlockId =>
            entry.getValue match {
              case value: SerializeConcatHostBuffersDeserializeBatch =>
                Some((id.broadcastId, System.identityHashCode(value), value.dataSize))
              case _ => None
            }
          case _ => None
        }
      }.toList
    }
    val rows = identities.map { case (id, identity, bytes) =>
      val present = env.blockManager.getStatus(BroadcastBlockId(id)).isDefined
      s"""{"broadcast_id":$id,"object_id":$identity,""" +
        s""""host_block_present":$present,"serialized_bytes":$bytes}"""
    }
    rows.mkString("[", ",", "]")
  }
}
