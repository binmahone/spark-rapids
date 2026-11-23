/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.nvidia.spark.rapids.benchmark.gc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.management.ManagementFactory;
import java.security.MessageDigest;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.xbean.asm9.ClassReader;
import org.apache.xbean.asm9.ClassVisitor;
import org.apache.xbean.asm9.ClassWriter;
import org.apache.xbean.asm9.MethodVisitor;
import org.apache.xbean.asm9.Opcodes;
import org.apache.xbean.asm9.Type;
import org.apache.xbean.asm9.commons.AdviceAdapter;
import org.apache.xbean.asm9.commons.Method;

/** Passive lifecycle tracing: retained state consists only of numeric identities and strings. */
public final class BroadcastGcAgent {
  private static final String TARGET = "org/apache/spark/sql/rapids/execution/" +
      "SerializeConcatHostBuffersDeserializeBatch";
  private static BufferedWriter output;
  private static volatile String failure;
  private static volatile boolean transformed;
  private static final ConcurrentHashMap<Integer, Boolean> created = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<Integer, Boolean> finalized = new ConcurrentHashMap<>();
  private static volatile boolean identityCollision;
  private static File gcLog;
  private static volatile long gcOffset = -1;

  private BroadcastGcAgent() {}

  public static void premain(String directory, Instrumentation instrumentation) throws Exception {
    File folder = new File(directory);
    if (!folder.isDirectory() && !folder.mkdirs()) {
      throw new IllegalStateException("Cannot create lifecycle log directory");
    }
    String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
    gcLog = new File(folder.getParentFile(), "jvm-gc/gc-" + pid + ".log");
    output = new BufferedWriter(new FileWriter(new File(folder, "lifecycle-" + pid + ".jsonl")));
    mark("agent_started", "");
    instrumentation.addTransformer(new ClassFileTransformer() {
      @Override
      public byte[] transform(ClassLoader loader, String name, Class<?> redefined,
          ProtectionDomain domain, byte[] bytes) {
        if (!TARGET.equals(name)) {
          return null;
        }
        try {
          ClassReader reader = new ClassReader(bytes);
          ClassWriter writer = new ClassWriter(reader, ClassWriter.COMPUTE_MAXS);
          Set<String> instrumented = new HashSet<>();
          reader.accept(new ClassVisitor(Opcodes.ASM9, writer) {
            @Override
            public MethodVisitor visitMethod(int access, String methodName, String descriptor,
                String signature, String[] exceptions) {
              MethodVisitor visitor = super.visitMethod(access, methodName, descriptor,
                  signature, exceptions);
              if (!(methodName.equals("<init>") ||
                  (descriptor.equals("()J") && methodName.equals("retireAfterIndependentQuery")) ||
                  (descriptor.equals("()V") && (methodName.equals("finalize") ||
                      methodName.equals("closeInternal"))))) {
                return visitor;
              }
              instrumented.add(methodName);
              return new AdviceAdapter(Opcodes.ASM9, visitor, access, methodName, descriptor) {
                private void emit(String event) {
                  loadThis();
                  push(event);
                  invokeStatic(Type.getType(BroadcastGcAgent.class),
                      new Method("event", "(Ljava/lang/Object;Ljava/lang/String;)V"));
                }
                @Override
                protected void onMethodEnter() {
                  if (!methodName.equals("<init>")) {
                    emit(methodName + "_enter");
                  }
                }
                @Override
                protected void onMethodExit(int opcode) {
                  emit(methodName + (opcode == ATHROW ? "_throw" : "_return"));
                }
              };
            }
          }, ClassReader.EXPAND_FRAMES);
          if (instrumented.size() != 4) {
            throw new IllegalStateException("Expected constructor, finalize, closeInternal and retirement");
          }
          byte[] changed = writer.toByteArray();
          mark("class_transformed", "input_sha256=" + sha(bytes) +
              ";output_sha256=" + sha(changed));
          transformed = true;
          return changed;
        } catch (Throwable error) {
          failure = error.toString();
          return null;
        }
      }
    });
  }

  private static String sha(byte[] bytes) throws Exception {
    StringBuilder value = new StringBuilder();
    for (byte b : MessageDigest.getInstance("SHA-256").digest(bytes)) {
      value.append(String.format("%02x", b & 255));
    }
    return value.toString();
  }

  public static void event(Object instance, String event) {
    int id = System.identityHashCode(instance);
    if (event.equals("<init>_return") && created.putIfAbsent(id, true) != null) {
      identityCollision = true;
    }
    if (event.equals("finalize_return")) {
      finalized.put(id, true);
    }
    mark(event, Integer.toString(id));
  }

  private static String quoted(String value) {
    return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"")
        .replace("\n", "\\n").replace("\r", "\\r") + "\"";
  }

  public static synchronized void mark(String event, String detail) {
    try {
      output.write("{\"time_ms\":" + System.currentTimeMillis() + ",\"uptime_ms\":" +
          ManagementFactory.getRuntimeMXBean().getUptime() + ",\"thread\":" +
          quoted(Thread.currentThread().getName()) + ",\"event\":" + quoted(event) +
          ",\"detail\":" + quoted(detail) + "}\n");
      output.flush();
    } catch (Exception error) {
      failure = error.toString();
    }
  }

  public static void checkHealthy() {
    if (!transformed || failure != null || identityCollision) {
      throw new IllegalStateException("Lifecycle tracing unhealthy: transformed=" + transformed +
          ", failure=" + failure + ", identityCollision=" + identityCollision);
    }
  }

  public static List<Integer> finalizedIds() {
    return new ArrayList<>(finalized.keySet());
  }

  public static void requestGc(String phase) {
    if (!gcLog.isFile()) {
      throw new IllegalStateException("GC logging is required: " + gcLog);
    }
    gcOffset = gcLog.length();
    mark("gc_requested", phase);
    System.gc();
    mark("gc_request_returned", phase);
  }

  public static boolean gcCompleted() {
    if (gcOffset < 0) {
      return false;
    }
    try (RandomAccessFile input = new RandomAccessFile(gcLog, "r")) {
      input.seek(gcOffset);
      String line;
      while ((line = input.readLine()) != null) {
        if (line.matches(".*GC\\(\\d+\\) (Pause Full \\(System.gc\\(\\)\\).*|" +
            "Concurrent Mark Cycle) .*\\d+(\\.\\d+)?ms\\s*$")) {
          return true;
        }
      }
      return false;
    } catch (Exception error) {
      throw new IllegalStateException("Cannot read GC completion evidence", error);
    }
  }
}
