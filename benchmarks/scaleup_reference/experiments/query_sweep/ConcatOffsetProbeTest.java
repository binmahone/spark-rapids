/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
package com.nvidia.spark.rapids.experiments;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.CudfException;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.Table;
import java.util.Arrays;

/** Checks the actual rebound JNI entry point with mixed columns and failure cleanup. */
public final class ConcatOffsetProbeTest {
  public static void main(String[] arguments) {
    Cuda.setDevice(0);
    char[] data = new char[2 * 1024 * 1024];
    Arrays.fill(data, 'a');
    String first = new String(data);
    Arrays.fill(data, 'z');
    String last = new String(data);
    String[] expected = {first, null, "", last};
    try (ColumnVector strings = ColumnVector.fromStrings(expected);
         ColumnVector ints = ColumnVector.fromInts(1, 2, 3, 4);
         ColumnVector emptyStrings = ColumnVector.fromStrings();
         ColumnVector emptyInts = ColumnVector.fromInts();
         Table input = new Table(strings, ints);
         Table empty = new Table(emptyStrings, emptyInts);
         Table numeric = new Table(ints, ints);
         Table result = Table.concatenate(input, empty, input, input);
         HostColumnVector outStrings = result.getColumn(0).copyToHost();
         HostColumnVector outInts = result.getColumn(1).copyToHost()) {
      if (result.getRowCount() != 12) { throw new AssertionError("Wrong result row count"); }
      for (int i = 0; i < 12; ++i) {
        String value = expected[i % 4];
        if (outStrings.isNull(i) != (value == null) ||
            (value != null && !value.equals(outStrings.getJavaString(i))) ||
            outInts.getInt(i) != i % 4 + 1) {
          throw new AssertionError("Incorrect row " + i);
        }
      }
      boolean rejected = false;
      try (Table invalid = Table.concatenate(input, numeric)) {
        throw new AssertionError("Incompatible column types accepted");
      } catch (CudfException expectedError) {
        rejected = true;
      }
      if (!rejected || ConcatOffsetProbe.counters()[0] != 2) {
        throw new AssertionError("Native entry point or rejection was not exercised");
      }
    }
    System.out.println("Mixed-column JNI parity, empty input, nulls and type rejection passed");
  }
  private ConcatOffsetProbeTest() {}
}
