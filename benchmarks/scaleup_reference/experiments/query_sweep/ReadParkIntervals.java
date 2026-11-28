/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
package com.nvidia.spark.rapids.experiments;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.time.Instant;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingFile;

/** Extracts observed Java park intervals without converting samples into elapsed time. */
public final class ReadParkIntervals {
  private static long nanos(Instant time) {
    return Math.addExact(Math.multiplyExact(time.getEpochSecond(), 1000000000L), time.getNano());
  }
  private static String json(String text) {
    return "\"" + text.replace("\\", "\\\\").replace("\"", "\\\"")
        .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\"";
  }
  public static void main(String[] args) throws Exception {
    long begin = Long.parseLong(args[1]);
    long end = Long.parseLong(args[2]);
    try (RecordingFile input = new RecordingFile(Path.of(args[0]));
         PrintWriter output = new PrintWriter(args[3])) {
      while (input.hasMoreEvents()) {
        RecordedEvent event = input.readEvent();
        String type = event.getEventType().getName();
        if (type.equals("jdk.DataLoss")) {
          throw new AssertionError("JFR data loss prevents interval qualification");
        }
        if (!type.equals("jdk.ThreadPark")) { continue; }
        long start = nanos(event.getStartTime());
        long stop = nanos(event.getEndTime());
        if (stop <= begin || start >= end) { continue; }
        RecordedThread thread = event.getThread();
        StringBuilder stack = new StringBuilder();
        if (event.getStackTrace() != null) {
          for (RecordedFrame frame : event.getStackTrace().getFrames()) {
            if (stack.length() > 0) { stack.append(';'); }
            stack.append(frame.getMethod().getType().getName()).append('.')
                .append(frame.getMethod().getName());
          }
        }
        output.println("{\"start\":" + start + ",\"end\":" + stop
            + ",\"truncated\":" + (event.getStackTrace() != null
                && event.getStackTrace().isTruncated())
            + ",\"frame_count\":" + (event.getStackTrace() == null ? 0
                : event.getStackTrace().getFrames().size())
            + ",\"os_tid\":" + thread.getOSThreadId()
            + ",\"thread\":" + json(thread.getJavaName())
            + ",\"stack\":" + json(stack.toString()) + "}");
      }
    }
  }
  private ReadParkIntervals() {}
}
