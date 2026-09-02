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

import java.util.Properties

import scala.collection.mutable.ArrayBuffer

import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.scheduler.{SparkListenerJobStart, StageInfo}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

class GpuCatalogCleanupListenerSuite extends AnyFunSuite {

  test("cleanup waits for SQL execution end and includes shuffles from every job") {
    val cleaned = ArrayBuffer.empty[Int]
    val listener = new GpuCatalogCleanupListener {
      override protected def onCleanup(shuffleId: Int): Unit = cleaned += shuffleId
    }
    val properties = new Properties()
    properties.setProperty(SQLExecution.EXECUTION_ID_KEY, "42")
    val firstStage = mock[StageInfo]
    val secondStage = mock[StageInfo]
    when(firstStage.shuffleDepId).thenReturn(Some(7))
    when(secondStage.shuffleDepId).thenReturn(Some(9))

    listener.onJobStart(SparkListenerJobStart(1, 100, Seq(firstStage), properties))
    listener.onJobStart(SparkListenerJobStart(2, 200, Seq(secondStage), properties))
    assert(cleaned.isEmpty)

    listener.onOtherEvent(SparkListenerSQLExecutionEnd(42, 300))
    assert(cleaned.sorted == Seq(7, 9))
  }
}
