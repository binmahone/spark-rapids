/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
package com.nvidia.spark.rapids.experiments;

import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.DeviceMemoryBuffer;
import ai.rapids.cudf.HostMemoryBuffer;
import java.util.Arrays;

/** Checks every byte, untouched guards, copy ordering, host fallback and range rejection. */
public final class ShuffleCopyProbeTest {
  private static void verify(Cuda.Stream stream, int sourceOffset, int destinationOffset,
      int length) {
    int capacity = Math.max(sourceOffset, destinationOffset) + length + 64;
    byte[] input = new byte[capacity];
    byte[] expected = new byte[capacity];
    Arrays.fill(expected, (byte) 0x5a);
    for (int i = 0; i < capacity; i++) { input[i] = (byte) ((i * 31) ^ (i >>> 9)); }
    try (HostMemoryBuffer host = HostMemoryBuffer.allocate(capacity);
         HostMemoryBuffer actual = HostMemoryBuffer.allocate(capacity);
         DeviceMemoryBuffer source = DeviceMemoryBuffer.allocate(capacity);
         DeviceMemoryBuffer bounce = DeviceMemoryBuffer.allocate(capacity);
         DeviceMemoryBuffer result = DeviceMemoryBuffer.allocate(capacity)) {
      host.setBytes(0, input, 0, capacity);
      actual.setBytes(0, expected, 0, capacity);
      source.copyFromHostBufferAsync(host, stream);
      result.copyFromHostBufferAsync(actual, stream);
      ShuffleCopyProbe.send(bounce, destinationOffset, source, sourceOffset, length, stream);
      ShuffleCopyProbe.receive(result, sourceOffset, bounce, destinationOffset, length, stream);
      actual.copyFromDeviceBuffer(result, stream);
      System.arraycopy(input, sourceOffset, expected, sourceOffset, length);
      byte[] observed = new byte[capacity];
      actual.getBytes(observed, 0, 0, capacity);
      if (!Arrays.equals(expected, observed)) { throw new AssertionError("SHUFFLE_COPY_BYTES"); }
      ShuffleCopyProbe.send(result, 0, host, 0, capacity, stream);
      actual.copyFromDeviceBuffer(result, stream);
      actual.getBytes(observed, 0, 0, capacity);
      if (!Arrays.equals(input, observed)) { throw new AssertionError("Host fallback differs"); }
      boolean rejected = false;
      try {
        ShuffleCopyProbe.receive(result, capacity, bounce, 0, 1, stream);
      } catch (IllegalArgumentException expectedError) { rejected = true; }
      if (!rejected) { throw new AssertionError("Invalid range accepted"); }
    }
  }

  public static void main(String[] args) throws Exception {
    Cuda.setDevice(0);
    ShuffleCopyProbe.verifyTransformation();
    try (Cuda.Stream stream = new Cuda.Stream(true)) {
      for (int length : new int[]{0, 1, 15, 16, 17, 4096, 4 * 1024 * 1024}) {
        verify(stream, 0, 16, length);
        verify(stream, 3, 7, length);
      }
    }
    verify(Cuda.DEFAULT_STREAM, 0, 0, 4 * 1024 * 1024);
    System.out.println("Shuffle copy byte parity, guards, ordering, host fallback and range rejection passed");
  }
  private ShuffleCopyProbeTest() {}
}
