/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
package com.nvidia.spark.rapids.experiments;

import ai.rapids.cudf.NativeDepsLoader;
import com.nvidia.spark.rapids.jni.PackedHostToDeviceCopy;
import java.io.FileInputStream;
import java.lang.instrument.Instrumentation;
import java.util.Properties;

/** Installs a diagnostic copy-submission policy without changing allocation or packing. */
public final class ScanCopyProbe {
  public static void premain(String arguments, Instrumentation instrumentation) throws Exception {
    String[] parts = arguments.split(",", 4);
    if (parts.length != 4) {
      throw new IllegalArgumentException("Expected mode,library,mapping,lease-directory");
    }
    int mode;
    switch (parts[0]) {
      case "original": mode = 0; break;
      case "sync": mode = 1; break;
      case "pair": mode = 2; break;
      default: throw new IllegalArgumentException("Unknown copy probe mode");
    }
    Properties mapping = new Properties();
    try (FileInputStream input = new FileInputStream(parts[2])) { mapping.load(input); }
    String visible = System.getenv("CUDA_VISIBLE_DEVICES");
    String group = visible == null || visible.isEmpty() ? "driver" : mapping.getProperty(visible);
    if (group == null) { throw new IllegalArgumentException("GPU absent from measured topology"); }
    NativeDepsLoader.loadNativeDeps();
    Class.forName(PackedHostToDeviceCopy.class.getName());
    System.load(parts[1]);
    install(PackedHostToDeviceCopy.class, mode, parts[3] + "/" + group + ".lock");
    System.err.println("SCAN_COPY_PROBE_INSTALLED mode=" + mode + " group=" + group);
  }

  private static native void install(Class<?> type, int mode, String path);
  private ScanCopyProbe() {}
}
