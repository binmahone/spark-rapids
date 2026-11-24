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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.scalatest.funsuite.AnyFunSuite

class IndependentBroadcastRetirementSuite extends AnyFunSuite {
  test("retirement is idempotent and rejects every reuse entry point") {
    val value = new SerializeConcatHostBuffersDeserializeBatch(null, Seq.empty, 10, 0L)
    assert(!value.isRetiredAfterIndependentQuery)
    assertResult(0L)(value.retireAfterIndependentQuery())
    assert(value.isRetiredAfterIndependentQuery)
    assertResult(0L)(value.retireAfterIndependentQuery())
    intercept[IllegalArgumentException](value.batch)
    intercept[IllegalArgumentException](value.hostBatch)
    val output = new ByteArrayOutputStream()
    val stream = new ObjectOutputStream(output)
    try {
      intercept[IllegalArgumentException](value.doWriteObject(stream))
      stream.flush()
      val input = new ObjectInputStream(new ByteArrayInputStream(output.toByteArray))
      try {
        intercept[IllegalArgumentException](value.doReadObject(input))
      } finally {
        input.close()
      }
    } finally {
      stream.close()
      value.closeInternal()
    }
  }
}
