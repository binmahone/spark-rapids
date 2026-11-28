/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.Scalar;
import com.nvidia.spark.rapids.jni.Hash;
import java.nio.file.Files;
import java.nio.file.Paths;

/** Verifies both JNI layers and a GPU operation before starting Spark executors. */
public final class NativeStackSmoke {
  public static void main(String[] args) throws Exception {
    if (Hash.MAX_STACK_DEPTH <= 0 || Cuda.getDeviceCount() != 1) {
      throw new AssertionError("JNI constants or isolated GPU visibility differ");
    }
    Cuda.setDevice(0);
    try (ColumnVector values = ColumnVector.fromLongs(1, 2, 3);
         Scalar sum = values.sum()) {
      if (sum.getLong() != 6) {
        throw new AssertionError("GPU reduction returned the wrong value");
      }
    }
    String expected = Paths.get(System.getProperty("ai.rapids.cudf.lib-native-dir"),
        "libcudf.so").toRealPath().toString();
    int mappings = 0;
    for (String line : Files.readAllLines(Paths.get("/proc/self/maps"))) {
      if (line.contains("libcudf.so")) {
        if (!line.endsWith(expected)) {
          throw new AssertionError("Unexpected cuDF mapping: " + line);
        }
        mappings++;
      }
    }
    if (mappings == 0) {
      throw new AssertionError("No cuDF library mapping found");
    }
    System.out.println("NATIVE_STACK_SMOKE_PASSED library=" + expected);
  }
}
