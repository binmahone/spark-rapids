/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.shims

import java.io.ByteArrayInputStream

import org.apache.hadoop.fs.{FSDataInputStream, FSInputStream}
import org.scalatest.funsuite.AnyFunSuite

class GpuOrcDataReaderBaseSuite extends AnyFunSuite {
  test("readFullyInChunks bounds allocations and preserves data") {
    val source = Array.tabulate[Byte](23)(_.toByte)
    val input = new FSDataInputStream(new ByteArraySeekableInputStream(source))
    val chunks = collection.mutable.ArrayBuffer.empty[Array[Byte]]

    GpuOrcDataReaderBase.readFullyInChunks(input, 3, 17, 5) { data =>
      chunks += data
    }

    assert(chunks.map(_.length) == Seq(5, 5, 5, 2))
    assert(chunks.flatten.toArray.sameElements(source.slice(3, 20)))
  }

  test("readFullyInChunks handles an empty range without allocating") {
    val input = new FSDataInputStream(new ByteArraySeekableInputStream(Array.emptyByteArray))
    var calls = 0

    GpuOrcDataReaderBase.readFullyInChunks(input, 0, 0, 5) { _ =>
      calls += 1
    }

    assert(calls == 0)
  }

  test("readFullyInChunks exposes each production read separately from consumption") {
    val source = Array.tabulate[Byte](23)(_.toByte)
    val input = new FSDataInputStream(new ByteArraySeekableInputStream(source))
    val reads = collection.mutable.ArrayBuffer.empty[(Long, Int)]
    val chunks = collection.mutable.ArrayBuffer.empty[Array[Byte]]

    GpuOrcDataReaderBase.readFullyInChunksWithReader(3, 17, 5) { (offset, data) =>
      reads += offset -> data.length
      input.readFully(offset, data, 0, data.length)
    } { data =>
      chunks += data
    }

    assert(reads == Seq(3L -> 5, 8L -> 5, 13L -> 5, 18L -> 2))
    assert(chunks.flatten.toArray.sameElements(source.slice(3, 20)))
  }

  private class ByteArraySeekableInputStream(data: Array[Byte]) extends FSInputStream {
    private val stream = new ByteArrayInputStream(data)
    private var position = 0

    override def read(): Int = {
      val value = stream.read()
      if (value >= 0) {
        position += 1
      }
      value
    }

    override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
      val count = stream.read(buffer, offset, length)
      if (count > 0) {
        position += count
      }
      count
    }

    override def seek(target: Long): Unit = {
      stream.reset()
      val skipped = stream.skip(target)
      if (skipped != target) {
        throw new IllegalArgumentException(s"invalid seek target: $target")
      }
      position = target.toInt
    }

    override def getPos: Long = position

    override def seekToNewSource(targetPos: Long): Boolean = false
  }
}
