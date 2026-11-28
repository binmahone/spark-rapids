/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.Scalar;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CudaWaitPolicySmoke {
  public static void main(String[] args) throws Exception {
    CountDownLatch ready = new CountDownLatch(32);
    CountDownLatch go = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 32; i++) {
      Thread thread = new Thread(() -> {
        try {
          Cuda.setDevice(0);
          ready.countDown();
          if (!go.await(30, TimeUnit.SECONDS)) {
            throw new AssertionError("Concurrent sync gate timed out");
          }
          Cuda.DEFAULT_STREAM.sync();
        } catch (Throwable error) {
          failure.compareAndSet(null, error);
        }
      });
      threads.add(thread);
      thread.start();
    }
    boolean allReady = ready.await(30, TimeUnit.SECONDS);
    go.countDown();
    for (Thread thread : threads) {
      thread.join(30000);
      if (thread.isAlive()) {
        throw new AssertionError("Concurrent sync thread did not finish");
      }
    }
    if (!allReady || failure.get() != null) {
      throw new AssertionError("Concurrent native binding failed", failure.get());
    }
    Cuda.setDevice(0);
    try (ColumnVector input = ColumnVector.fromLongs(1, 2, 3);
         Scalar total = input.sum()) {
      Cuda.DEFAULT_STREAM.sync();
      if (total.getLong() != 6) {
        throw new AssertionError("Incorrect GPU sum");
      }
    }
    System.out.println("CUDA_WAIT_POLICY_SMOKE_PASSED");
  }
}
