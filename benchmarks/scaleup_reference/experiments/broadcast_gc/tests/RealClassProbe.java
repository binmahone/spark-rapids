/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
import com.nvidia.spark.rapids.benchmark.gc.BroadcastGcAgent;

/** Verifies the real frozen Scala class without constructing an object or initializing CUDA. */
public final class RealClassProbe {
  public static void main(String[] args) throws Exception {
    Class<?> target = Class.forName("org.apache.spark.sql.rapids.execution." +
        "SerializeConcatHostBuffersDeserializeBatch");
    String source = target.getProtectionDomain().getCodeSource().getLocation().getPath();
    if (!source.endsWith(args[0])) {
      throw new AssertionError("The real frozen class was not loaded: " + source);
    }
    target.getDeclaredMethods();
    BroadcastGcAgent.checkHealthy();
    System.out.println("PASS real_engine_class_transformed=true cuda_not_initialized=true");
  }
}
