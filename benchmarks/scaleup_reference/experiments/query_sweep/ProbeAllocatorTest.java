/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
package com.nvidia.spark.rapids.experiments;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.Rmm;
import ai.rapids.cudf.RmmAllocationMode;
import ai.rapids.cudf.Table;
import com.nvidia.spark.rapids.jni.PackedHostToDeviceCopy;
import java.util.Arrays;

/** Rejects native adapters whose allocations bypass the initialized process RMM pool. */
public final class ProbeAllocatorTest {
  public static void main(String[] arguments) {
    Cuda.setDevice(0);
    Rmm.initialize(RmmAllocationMode.POOL, null, 256L * 1024 * 1024);
    try {
      if ("scan".equals(arguments[0])) {
        int bytes = 40 * 1024 * 1024;
        try (HostMemoryBuffer host = HostMemoryBuffer.allocate(bytes, true);
             Cuda.Stream stream = new Cuda.Stream(true);
             Cuda.Event event = new Cuda.Event()) {
          host.setMemory(0, bytes, (byte) 42);
          long before = Rmm.getTotalBytesAllocated();
          try (PackedHostToDeviceCopy copy = PackedHostToDeviceCopy.copyAsync(
              new HostMemoryBuffer[]{host}, stream, 32 * 1024 * 1024, event)) {
            event.sync();
            requireTracked(before, bytes);
          }
          if (Rmm.getTotalBytesAllocated() != before) {
            throw new AssertionError("Copy allocations were not released");
          }
        }
      } else if ("concat".equals(arguments[0])) {
        char[] data = new char[2 * 1024 * 1024];
        Arrays.fill(data, 'x');
        try (ColumnVector strings = ColumnVector.fromStrings(new String(data));
             Table input = new Table(strings)) {
          long before = Rmm.getTotalBytesAllocated();
          try (Table output = Table.concatenate(input, input, input)) {
            requireTracked(before, 3L * data.length);
          }
          if (Rmm.getTotalBytesAllocated() != before) {
            throw new AssertionError("Concatenate allocations were not released");
          }
        }
      } else {
        throw new IllegalArgumentException("Expected scan or concat");
      }
      if (Rmm.getTotalBytesAllocated() != 0) {
        throw new AssertionError("Test leaked tracked device allocations");
      }
    } finally {
      Rmm.shutdown();
    }
    System.out.println("Shared RMM allocation accounting and release passed");
  }

  private static void requireTracked(long before, long expected) {
    long allocated = Rmm.getTotalBytesAllocated() - before;
    System.out.println("Probe RMM allocated bytes=" + allocated + " expected>=" + expected);
    if (allocated < expected) {
      throw new AssertionError("Native probe allocations bypass process RMM accounting");
    }
  }
  private ProbeAllocatorTest() {}
}
