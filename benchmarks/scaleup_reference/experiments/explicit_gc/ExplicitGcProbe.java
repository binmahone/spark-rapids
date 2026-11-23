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
import com.sun.management.HotSpotDiagnosticMXBean;

/** Positive control only; this program is never inserted into the SQL workload. */
public class ExplicitGcProbe {
  private static volatile Object held;

  public static void main(String[] args) throws Exception {
    HotSpotDiagnosticMXBean bean = ManagementFactory.getPlatformMXBean(
        HotSpotDiagnosticMXBean.class);
    for (String name : new String[]{"UseG1GC", "DisableExplicitGC",
        "ExplicitGCInvokesConcurrent"}) {
      System.out.println(name + "=" + bean.getVMOption(name).getValue());
    }
    held = new byte[32 * 1024 * 1024];
    held = null;
    System.out.println("POSITIVE_CONTROL_SYSTEM_GC_BEGIN");
    System.gc();
    Thread.sleep(500);
    System.out.println("POSITIVE_CONTROL_SYSTEM_GC_END");
  }
}
