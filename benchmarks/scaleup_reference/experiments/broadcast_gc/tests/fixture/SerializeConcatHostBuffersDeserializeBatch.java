/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package org.apache.spark.sql.rapids.execution;

/** CPU-only transformer fixture. Never packaged with the diagnostic tools or Spark workload. */
public final class SerializeConcatHostBuffersDeserializeBatch {
  public static volatile int released;
  public boolean fail;
  private boolean closed;

  public SerializeConcatHostBuffersDeserializeBatch() {}

  public long retireAfterIndependentQuery() {
    closeInternal();
    return 1;
  }

  public synchronized void closeInternal() {
    if (fail) {
      throw new IllegalStateException("expected fixture failure");
    }
    if (!closed) {
      released++;
      closed = true;
    }
  }

  @Override
  public void finalize() {
    closeInternal();
  }
}
