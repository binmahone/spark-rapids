/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
package com.nvidia.spark.rapids.experiments;

import ai.rapids.cudf.BaseDeviceMemoryBuffer;
import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.DeviceMemoryBuffer;
import ai.rapids.cudf.MemoryBuffer;
import ai.rapids.cudf.NativeDepsLoader;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.security.ProtectionDomain;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.xbean.asm9.ClassReader;
import org.apache.xbean.asm9.ClassVisitor;
import org.apache.xbean.asm9.ClassWriter;
import org.apache.xbean.asm9.MethodVisitor;
import org.apache.xbean.asm9.Opcodes;

/** Replaces only the two shuffle bounce-buffer copy call sites for a diagnostic trial. */
public final class ShuffleCopyProbe {
  private static final String PREFIX = "com/nvidia/spark/rapids/shuffle/";
  private static final String SELF = "com/nvidia/spark/rapids/experiments/ShuffleCopyProbe";
  private static final AtomicBoolean SEND = new AtomicBoolean();
  private static final AtomicBoolean RECEIVE = new AtomicBoolean();
  private static volatile boolean kernel;
  private static volatile int transformed;

  public static void premain(String arguments, Instrumentation instrumentation) {
    String[] parts = arguments.split(",", 2);
    if (parts.length != 2 || !(parts[0].equals("control") || parts[0].equals("kernel"))) {
      throw new IllegalArgumentException("Expected control|kernel,library");
    }
    kernel = parts[0].equals("kernel");
    NativeDepsLoader.loadNativeDeps();
    System.load(parts[1]);
    instrumentation.addTransformer(new ClassFileTransformer() {
      @Override
      public byte[] transform(ClassLoader loader, String name, Class<?> type,
          ProtectionDomain domain, byte[] bytes) {
        boolean send = name.equals(PREFIX + "BufferSendState");
        boolean receive = name.equals(PREFIX + "BufferReceiveState");
        if (!send && !receive) { return null; }
        ClassReader reader = new ClassReader(bytes);
        ClassWriter writer = new ClassWriter(reader, 0);
        int[] count = {0};
        reader.accept(new ClassVisitor(Opcodes.ASM9, writer) {
          @Override
          public MethodVisitor visitMethod(int access, String method, String descriptor,
              String signature, String[] exceptions) {
            return new MethodVisitor(Opcodes.ASM9,
                super.visitMethod(access, method, descriptor, signature, exceptions)) {
              @Override
              public void visitMethodInsn(int opcode, String owner, String methodName,
                  String desc, boolean isInterface) {
                String expected = send
                    ? "(JLai/rapids/cudf/MemoryBuffer;JJLai/rapids/cudf/Cuda$Stream;)V"
                    : "(JLai/rapids/cudf/BaseDeviceMemoryBuffer;JJLai/rapids/cudf/Cuda$Stream;)V";
                if (opcode == Opcodes.INVOKEVIRTUAL
                    && owner.equals("ai/rapids/cudf/" + (send ? "MemoryBuffer" : "DeviceMemoryBuffer"))
                    && methodName.equals(send ? "copyFromMemoryBufferAsync" : "copyFromDeviceBufferAsync")
                    && desc.equals(expected)) {
                  count[0]++;
                  super.visitMethodInsn(Opcodes.INVOKESTATIC, SELF, send ? "send" : "receive",
                      "(L" + owner + ";" + desc.substring(1), false);
                } else {
                  super.visitMethodInsn(opcode, owner, methodName, desc, isInterface);
                }
              }
            };
          }
        }, 0);
        if (count[0] != (send ? 1 : 3)) {
          // Returning invalid bytes prevents the JVM from silently ignoring transformer errors.
          System.err.println("SHUFFLE_COPY_TRANSFORM_REJECTED class=" + name + " calls=" + count[0]);
          return new byte[0];
        }
        synchronized (ShuffleCopyProbe.class) { transformed |= send ? 1 : 2; }
        System.err.println("SHUFFLE_COPY_TRANSFORMED class=" + name + " calls=" + count[0]);
        return writer.toByteArray();
      }
    });
  }

  private static void checkRange(MemoryBuffer buffer, long offset, long length) {
    if (offset < 0 || length < 0 || offset > buffer.getLength()
        || length > buffer.getLength() - offset) {
      throw new IllegalArgumentException("Copy exceeds buffer bounds");
    }
  }

  public static void send(MemoryBuffer dst, long offset, MemoryBuffer src, long sourceOffset,
      long bytes, Cuda.Stream stream) {
    checkRange(dst, offset, bytes);
    checkRange(src, sourceOffset, bytes);
    boolean eligible = dst instanceof BaseDeviceMemoryBuffer && src instanceof BaseDeviceMemoryBuffer;
    if (kernel && eligible && bytes > 0) {
      copy(dst.getAddress() + offset, src.getAddress() + sourceOffset, bytes, stream.getStream());
    } else {
      dst.copyFromMemoryBufferAsync(offset, src, sourceOffset, bytes, stream);
    }
    if (eligible && bytes > 0 && SEND.compareAndSet(false, true)) {
      System.err.println("SHUFFLE_COPY_SUBMITTED side=send mode=" + (kernel ? "kernel" : "control"));
    }
  }

  public static void receive(DeviceMemoryBuffer dst, long offset, BaseDeviceMemoryBuffer src,
      long sourceOffset, long bytes, Cuda.Stream stream) {
    checkRange(dst, offset, bytes);
    checkRange(src, sourceOffset, bytes);
    if (kernel && bytes > 0) {
      copy(dst.getAddress() + offset, src.getAddress() + sourceOffset, bytes, stream.getStream());
    } else {
      dst.copyFromDeviceBufferAsync(offset, src, sourceOffset, bytes, stream);
    }
    if (bytes > 0 && RECEIVE.compareAndSet(false, true)) {
      System.err.println("SHUFFLE_COPY_SUBMITTED side=receive mode=" + (kernel ? "kernel" : "control"));
    }
  }

  static void verifyTransformation() throws Exception {
    Class.forName("com.nvidia.spark.rapids.shuffle.BufferSendState", false, ShuffleCopyProbe.class.getClassLoader());
    Class.forName("com.nvidia.spark.rapids.shuffle.BufferReceiveState", false, ShuffleCopyProbe.class.getClassLoader());
    if (transformed != 3) { throw new AssertionError("Shuffle call sites were not transformed"); }
  }

  private static native void copy(long destination, long source, long bytes, long stream);
  private ShuffleCopyProbe() {}
}
