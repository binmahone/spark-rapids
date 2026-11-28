/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.Scalar;

public class CudaWaitPolicySmoke {
  public static void main(String[] args) {
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
