/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
package com.nvidia.spark.rapids.experiments;

import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.DeviceMemoryBuffer;
import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.jni.PackedHostToDeviceCopy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Checks large sliced inputs, empty inputs, exact copied bytes, and failure recovery. */
public final class ScanCopyProbeTest {
  private static void verify(int seed, CountDownLatch ready) throws Exception {
    Cuda.setDevice(0);
    int length = 40 * 1024 * 1024;
    try (Cuda.Stream stream = new Cuda.Stream(true);
         Cuda.Event event = new Cuda.Event();
         HostMemoryBuffer large = HostMemoryBuffer.allocate(length, true);
         HostMemoryBuffer small = HostMemoryBuffer.allocate(17, true)) {
      byte[] pattern = new byte[length];
      for (int i = 0; i < length; i++) {
        pattern[i] = (byte) ((i * 31) ^ (i >>> 8) ^ (i >>> 20) ^ seed);
      }
      large.setBytes(0, pattern, 0, length);
      small.setMemory(0, 17, (byte) 0x3c);
      if (ready != null) {
        ready.countDown();
        if (!ready.await(30, TimeUnit.SECONDS)) {
          throw new AssertionError("Concurrent copy peer did not become ready");
        }
      }
      try (HostMemoryBuffer sliced = large.slice(7, length - 14);
           HostMemoryBuffer empty = large.slice(0, 0);
           PackedHostToDeviceCopy copy = PackedHostToDeviceCopy.copyAsync(
               new HostMemoryBuffer[]{sliced, empty, small}, stream, 32 * 1024 * 1024, event)) {
        event.sync();
        DeviceMemoryBuffer[] buffers = copy.getBuffers();
        if (buffers.length != 3 || buffers[0].getLength() != length - 14
            || buffers[1].getLength() != 0 || buffers[2].getLength() != 17) {
          throw new AssertionError("Packed buffer lengths differ");
        }
        for (int index : new int[]{0, 2}) {
          int count = (int) buffers[index].getLength();
          try (HostMemoryBuffer actual = HostMemoryBuffer.allocate(count)) {
            actual.copyFromDeviceBuffer(buffers[index], stream);
            byte[] values = new byte[count];
            actual.getBytes(values, 0, 0, count);
            for (int i = 0; i < values.length; i++) {
              byte expected = index == 0 ? pattern[i + 7] : (byte) 0x3c;
              if (values[i] != expected) { throw new AssertionError("Copied bytes differ at " + i); }
            }
          }
        }
      }
      boolean rejected = false;
      try (PackedHostToDeviceCopy unexpected = PackedHostToDeviceCopy.copyAsync(
          new HostMemoryBuffer[]{small}, stream, -1, event)) {
        throw new AssertionError("Negative packing limit accepted");
      } catch (RuntimeException expected) {
        rejected = true;
      }
      if (!rejected) { throw new AssertionError("Invalid copy was not rejected"); }
      try (PackedHostToDeviceCopy empty = PackedHostToDeviceCopy.copyAsync(
          new HostMemoryBuffer[0], stream, 32 * 1024 * 1024, event)) {
        if (empty.getBuffers().length != 0) { throw new AssertionError("Empty input differs"); }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    verify(0, null);
    ExecutorService workers = Executors.newFixedThreadPool(2);
    CountDownLatch ready = new CountDownLatch(2);
    try {
      Future<?> first = workers.submit(() -> { verify(1, ready); return null; });
      Future<?> second = workers.submit(() -> { verify(2, ready); return null; });
      first.get(60, TimeUnit.SECONDS);
      second.get(60, TimeUnit.SECONDS);
    } finally {
      workers.shutdownNow();
      if (!workers.awaitTermination(30, TimeUnit.SECONDS)) {
        throw new AssertionError("Concurrent copy workers did not terminate");
      }
    }
    System.out.println("Sequential and concurrent sliced copy bytes, empty input, and recovery passed");
  }
  private ScanCopyProbeTest() {}
}
