/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
package com.nvidia.spark.rapids.experiments;

import ai.rapids.cudf.NativeDepsLoader;
import ai.rapids.cudf.Table;
import java.lang.instrument.Instrumentation;

/** Binds an isolated native implementation for controlled performance experiments. */
public final class ConcatOffsetProbe {
  public static void premain(String arguments, Instrumentation instrumentation) throws Exception {
    String[] parts = arguments.split(",", 2);
    if (parts.length != 2 || !(parts[0].equals("control") || parts[0].equals("candidate"))) {
      throw new IllegalArgumentException("Expected control|candidate,absolute-library-path");
    }
    NativeDepsLoader.loadNativeDeps();
    Class.forName(Table.class.getName());
    System.load(parts[1]);
    install(Table.class, parts[0].equals("candidate"));
    System.err.println("CONCAT_OFFSET_PROBE_INSTALLED mode=" + parts[0]);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      long[] counts = counters();
      System.err.println("CONCAT_OFFSET_PROBE_COUNTS mode=" + parts[0]
          + " calls=" + counts[0] + " candidate_string_columns=" + counts[1]
          + " input_tables=" + counts[2]);
    }, "concat-offset-probe-counters"));
  }

  private static native void install(Class<?> table, boolean candidate);
  public static native long[] counters();
  private ConcatOffsetProbe() {}
}
