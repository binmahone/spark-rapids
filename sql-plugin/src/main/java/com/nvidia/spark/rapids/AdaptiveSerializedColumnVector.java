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
 * A serialized Kudo partition carrying the task-scoped adaptive compression decision.
 */
public class AdaptiveSerializedColumnVector extends SlicedSerializedColumnVector {
  private final boolean gpuProposed;
  private final boolean gpuSelected;
  private final boolean gpuReservationDenied;
  private final boolean reportDecision;
  private final long gpuCompressionTimeNs;

  public AdaptiveSerializedColumnVector(
      HostMemoryBuffer buffer,
      int start,
      int end,
      boolean gpuProposed,
      boolean gpuSelected,
      boolean gpuReservationDenied,
      boolean reportDecision,
      long gpuCompressionTimeNs) {
    super(buffer, start, end);
    this.gpuProposed = gpuProposed;
    this.gpuSelected = gpuSelected;
    this.gpuReservationDenied = gpuReservationDenied;
    this.reportDecision = reportDecision;
    this.gpuCompressionTimeNs = gpuCompressionTimeNs;
  }

  public boolean isGpuProposed() {
    return gpuProposed;
  }

  public boolean isGpuSelected() {
    return gpuSelected;
  }

  public boolean isGpuReservationDenied() {
    return gpuReservationDenied;
  }

  public boolean shouldReportDecision() {
    return reportDecision;
  }

  public long getGpuCompressionTimeNs() {
    return gpuCompressionTimeNs;
  }
}
