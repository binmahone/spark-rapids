/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
import java.lang.management.ManagementFactory;
import java.lang.ref.WeakReference;

import com.nvidia.spark.rapids.benchmark.gc.BroadcastGcAgent;
import org.apache.spark.sql.rapids.execution.SerializeConcatHostBuffersDeserializeBatch;

/** Verifies that tracing preserves close/throw behavior and does not retain the observed object. */
public final class AgentProbe {
  private static WeakReference<Object> exercise() {
    SerializeConcatHostBuffersDeserializeBatch value =
        new SerializeConcatHostBuffersDeserializeBatch();
    value.fail = true;
    try {
      value.closeInternal();
      throw new AssertionError("Expected exception was suppressed");
    } catch (IllegalStateException expected) {
      if (!expected.getMessage().equals("expected fixture failure")) {
        throw expected;
      }
    }
    value.fail = false;
    if (value.retireAfterIndependentQuery() != 1) {
      throw new AssertionError("Retirement return value changed");
    }
    if (SerializeConcatHostBuffersDeserializeBatch.released != 1) {
      throw new AssertionError("Close did not release the fixture resource");
    }
    BroadcastGcAgent.checkHealthy();
    return new WeakReference<>(value);
  }

  private static long oldCount() {
    return ManagementFactory.getGarbageCollectorMXBeans().stream()
        .filter(bean -> bean.getName().contains("Old"))
        .mapToLong(bean -> bean.getCollectionCount()).sum();
  }

  public static void main(String[] args) throws Exception {
    WeakReference<Object> reference = exercise();
    long before = oldCount();
    BroadcastGcAgent.requestGc("cpu-positive-control");
    long deadline = System.nanoTime() + 10000000000L;
    while ((!BroadcastGcAgent.gcCompleted() || reference.get() != null ||
        BroadcastGcAgent.finalizedIds().size() != 1) && System.nanoTime() < deadline) {
      Thread.sleep(25);
    }
    if (!BroadcastGcAgent.gcCompleted() || reference.get() != null ||
        BroadcastGcAgent.finalizedIds().size() != 1) {
      throw new AssertionError("GC completion/finalization was not observed; tracing may retain objects");
    }
    if (SerializeConcatHostBuffersDeserializeBatch.released != 1) {
      throw new AssertionError("Resource was released more than once");
    }
    System.out.println("PASS gc_log_completed=true object_collected=true finalized=true old_gc_delta=" +
        (oldCount() - before));
  }
}
