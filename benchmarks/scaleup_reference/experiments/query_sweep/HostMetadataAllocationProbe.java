/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
package com.nvidia.spark.rapids.experiments;

import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.PinnedMemoryPool;
import ai.rapids.cudf.Rmm;
import ai.rapids.cudf.RmmAllocationMode;

/** Tests host allocation readiness independently of unrelated work on a compute stream. */
public final class HostMetadataAllocationProbe {
  public static void main(String[] arguments) {
    Cuda.setDevice(0);
    Rmm.initialize(RmmAllocationMode.POOL, null, 64L * 1024 * 1024);
    PinnedMemoryPool.initialize(64L * 1024 * 1024, 0, true);
    System.load(arguments[0]);
    boolean mutation = arguments.length > 1 && "unsafe".equals(arguments[1]);
    try {
      try {
        System.out.println(run(PinnedMemoryPool.getTotalPoolSizeBytes(), mutation));
        if (mutation) {
          throw new AssertionError("Unsafe allocation mutation escaped the reuse guard");
        }
      } catch (RuntimeException error) {
        if (!mutation || !error.getMessage().contains("HOST_METADATA_REUSE_GUARD")) {
          throw error;
        }
        System.out.println("Unsafe allocation mutation rejected by pending GPU reuse guard");
      }
      if (Rmm.getTotalBytesAllocated() != 0) {
        throw new AssertionError("Device allocations remain after metadata test");
      }
    } finally {
      Cuda.DEFAULT_STREAM.sync();
      PinnedMemoryPool.shutdown();
      Rmm.shutdown();
    }
  }

  private static native String run(long poolBytes, boolean unsafeMutation);
  private HostMetadataAllocationProbe() {}
}
