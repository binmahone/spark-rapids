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

package com.nvidia.spark.rapids;

import ai.rapids.cudf.HostMemoryBuffer;

/**
 * A serialized Kudo partition whose bytes are already encoded as standard Zstd frames.
 *
 * Shuffle writers must send these bytes through encryption and storage only. Passing them through
 * Spark's compression wrapper would compress the payload twice.
 */
public final class PrecompressedSerializedColumnVector extends AdaptiveSerializedColumnVector {
  private final long uncompressedLength;

  public PrecompressedSerializedColumnVector(
      HostMemoryBuffer buffer,
      int start,
      int end,
      long uncompressedLength,
      boolean gpuProposed,
      boolean gpuReservationDenied,
      boolean reportDecision,
      long gpuCompressionTimeNs) {
    super(
        buffer,
        start,
        end,
        gpuProposed,
        true,
        gpuReservationDenied,
        reportDecision,
        gpuCompressionTimeNs);
    if (uncompressedLength <= 0) {
      throw new IllegalArgumentException("uncompressedLength must be positive");
    }
    this.uncompressedLength = uncompressedLength;
  }

  public long getUncompressedLength() {
    return uncompressedLength;
  }
}
