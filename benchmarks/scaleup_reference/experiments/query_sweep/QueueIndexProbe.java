/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
package com.nvidia.spark.rapids.experiments;

import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.security.MessageDigest;
import java.security.ProtectionDomain;
import java.util.Properties;
import org.apache.xbean.asm9.ClassReader;
import org.apache.xbean.asm9.ClassVisitor;
import org.apache.xbean.asm9.ClassWriter;
import org.apache.xbean.asm9.MethodVisitor;
import org.apache.xbean.asm9.Opcodes;

/** Replaces one frozen queue implementation while retaining its production callers. */
public final class QueueIndexProbe {
  private static final String QUEUE = "com/nvidia/spark/rapids/HashedPriorityQueue";
  private static final String SELF = "com/nvidia/spark/rapids/experiments/QueueIndexProbe";
  private static volatile boolean executed;
  private static String mode;

  private static byte[] resource(String name) throws Exception {
    try (InputStream input = QueueIndexProbe.class.getResourceAsStream("/" + name)) {
      if (input == null) { throw new IllegalStateException(name); }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buffer = new byte[8192];
      int count;
      while ((count = input.read(buffer)) >= 0) { out.write(buffer, 0, count); }
      return out.toByteArray();
    }
  }

  private static String hash(byte[] bytes) throws Exception {
    StringBuilder result = new StringBuilder();
    for (byte b : MessageDigest.getInstance("SHA-256").digest(bytes)) {
      result.append(String.format("%02x", b & 255));
    }
    return result.toString();
  }

  public static void premain(String arguments, Instrumentation instrumentation) throws Exception {
    if (!("control".equals(arguments) || "candidate".equals(arguments))) {
      throw new IllegalArgumentException("Expected control or candidate");
    }
    mode = arguments;
    Properties expected = new Properties();
    expected.load(new ByteArrayInputStream(resource("queue-baseline.properties")));
    instrumentation.addTransformer(new ClassFileTransformer() {
      @Override
      public byte[] transform(ClassLoader loader, String name, Class<?> type,
          ProtectionDomain domain, byte[] bytes) {
        if (!(name.equals(QUEUE) || name.equals(QUEUE + "$1"))) { return null; }
        try {
          if (!hash(bytes).equals(expected.getProperty(name))) {
            throw new IllegalStateException("Frozen queue bytecode differs: " + name);
          }
          byte[] replacement = mode.equals("candidate")
              ? resource("replacement/" + name + ".class") : bytes;
          if (name.equals(QUEUE)) {
            ClassReader reader = new ClassReader(replacement);
            ClassWriter writer = new ClassWriter(reader, ClassWriter.COMPUTE_MAXS);
            int[] count = {0};
            reader.accept(new ClassVisitor(Opcodes.ASM9, writer) {
              @Override
              public MethodVisitor visitMethod(int access, String method, String desc,
                  String signature, String[] exceptions) {
                MethodVisitor delegate = super.visitMethod(access, method, desc, signature, exceptions);
                if (!method.equals("poll") || !desc.equals("()Ljava/lang/Object;")) { return delegate; }
                count[0]++;
                return new MethodVisitor(Opcodes.ASM9, delegate) {
                  @Override
                  public void visitCode() {
                    super.visitCode();
                    super.visitVarInsn(Opcodes.ALOAD, 0);
                    super.visitMethodInsn(Opcodes.INVOKESTATIC, SELF, "onPoll", "(Ljava/lang/Object;)V", false);
                  }
                };
              }
            }, 0);
            if (count[0] != 1) { throw new IllegalStateException("Expected one queue poll method"); }
            replacement = writer.toByteArray();
          }
          System.err.println("QUEUE_INDEX_TRANSFORMED mode=" + mode + " class=" + name);
          return replacement;
        } catch (Throwable error) {
          error.printStackTrace();
          System.err.println("QUEUE_INDEX_REJECTED");
          return new byte[0];
        }
      }
    });
  }

  public static void onPoll(Object queue) {
    if (executed) { return; }
    try {
      Object value = queue.getClass().getMethod("peek").invoke(queue);
      if (value != null && value.getClass().getName().equals(
          "com.nvidia.spark.rapids.shuffle.PendingTransferRequest")) {
        boolean caller = false;
        for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
          caller |= frame.getClassName().equals(
              "com.nvidia.spark.rapids.shuffle.ucx.UCXShuffleTransport");
        }
        if (!caller) { throw new IllegalStateException("Unexpected transfer queue caller"); }
        executed = true;
        System.err.println("QUEUE_INDEX_EXECUTED mode=" + mode + " caller=UCXShuffleTransport item=PendingTransferRequest");
      }
    } catch (Exception error) {
      throw new IllegalStateException(error);
    }
  }
  private QueueIndexProbe() {}
}
