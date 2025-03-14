/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.velox

import ai.rapids.cudf.{HostMemoryBuffer, PinnedMemoryPool}
import com.nvidia.spark.rapids.velox.DefaultHostMemoryAllocator.AutoCloseableBufferInfo

trait HostMemoryAllocator[R] {

  /**
   * Allocate multiple buffers, the returned result should have the same
   * size as the input.
   *
   * @param bytesSizes multiple sizes in bytes to allocate
   * @return the allocated result as a Seq of R
   */
  def allocate(bytesSizes: Seq[Long]): Seq[R]

  /**
   * Allocate a single buffer
   *
   * @param bytesSize size in bytes to allocate
   * @return the allocated result as a R
   */
  def allocate(bytesSize: Long): R = {
    val bufs = allocate(Seq(bytesSize))
    require(bufs.length == 1)
    bufs.head
  }
}

/**
 * The host memory allocator leveraging "HostMemoryBuffer" to apply for a new buffer.
 * And it will try pinned memory first by default. Specify "tryPinned" to false to
 * disable this behavior, meaning it will always allocate new buffers from pageable
 * memory.
 */
class DefaultHostMemoryAllocator(val tryPinned: Boolean = true)
  extends HostMemoryAllocator[HostBufferInfo] {

  /** Allocate multiple buffers, it will blow up if any allocation fails. */
  override def allocate(bytesSizes: Seq[Long]): Seq[HostBufferInfo] = {
    val bufs = new Array[HostBufferInfo](bytesSizes.length)
    var pos = 0
    try {
      bytesSizes.foreach { size =>
        var buf: HostMemoryBuffer = null
        if (tryPinned) {
          buf = PinnedMemoryPool.tryAllocate(size)
        }
        bufs(pos) = if (buf != null) { // pinned
          HostBufferInfo(buf, isPinned = true)
        } else {
          HostBufferInfo(HostMemoryBuffer.allocate(size, false), isPinned = false)
        }
        pos += 1
      }
      bufs.toSeq
    } catch {
      case t: Throwable =>
        // Try to close already allocated buffers
        bufs.slice(0, pos).foreach(_.safeClose(t))
        throw t
    }
  }
}

object DefaultHostMemoryAllocator {
  implicit class AutoCloseableBufferInfo(autoCloseable: AutoCloseable) {
    /**
     * safeClose: Is an implicit on AutoCloseable class that tries to close the resource, if an
     * Exception was thrown prior to this close, it adds the new exception to the suppressed
     * exceptions, otherwise just throws
     *
     * @param e Exception which we don't want to suppress
     */
    def safeClose(e: Throwable = null): Unit = {
      if (autoCloseable != null) {
        try {
          autoCloseable.close()
        } catch {
          case sup: Throwable =>
            if (e != null) {
              e.addSuppressed(sup)
            } else {
              throw sup
            }
        }
      }
    }
  }
}