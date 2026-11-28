/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
package com.nvidia.spark.rapids.experiments;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingFile;

/** Correlates sampled stacks with recorded GPU activity gaps, retaining sampling limitations. */
public final class ReadPairedStackSamples {
  private static long nanos(Instant time) {
    return Math.addExact(Math.multiplyExact(time.getEpochSecond(), 1000000000L), time.getNano());
  }
  private static String json(String text) {
    return "\"" + text.replace("\\", "\\\\").replace("\"", "\\\"")
        .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\"";
  }
  private static final class Group {
    long events;
    long samples;
    final Map<String, Long> stacks = new HashMap<>();
    final Map<String, Long> inclusive = new HashMap<>();
  }
  public static void main(String[] args) throws Exception {
    long begin = Long.parseLong(args[1]);
    long end = Long.parseLong(args[2]);
    List<long[]> gaps = new ArrayList<>();
    for (String line : Files.readAllLines(Path.of(args[3]))) {
      String[] fields = line.split("\t");
      gaps.add(new long[]{Long.parseLong(fields[0]), Long.parseLong(fields[1])});
    }
    Map<String, Group> groups = new HashMap<>();
    try (RecordingFile input = new RecordingFile(Path.of(args[0]))) {
      while (input.hasMoreEvents()) {
        RecordedEvent event = input.readEvent();
        String type = event.getEventType().getName();
        boolean wall = type.equals("profiler.WallClockSample");
        if (!wall && !type.equals("jdk.ExecutionSample")) { continue; }
        long time = nanos(event.getStartTime());
        if (time < begin || time >= end || event.getStackTrace() == null) { continue; }
        long span = wall ? event.getDuration("timeSpan").toNanos() : 0;
        long samples = wall ? Integer.toUnsignedLong(event.getInt("samples")) : 1;
        RecordedThread thread = event.getThread("sampledThread");
        String name = thread == null ? "unknown" : thread.getJavaName();
        if (name == null && thread != null) { name = thread.getOSName(); }
        StringBuilder trace = new StringBuilder();
        for (RecordedFrame frame : event.getStackTrace().getFrames()) {
          if (trace.length() > 0) { trace.append(';'); }
          trace.append(frame.getMethod().getType().getName()).append('.')
              .append(frame.getMethod().getName());
        }
        String stack = trace.toString();
        String domain = stack.contains("Executor$TaskRunner.run") ? "task"
            : name != null && name.startsWith("kvikio") ? "native-io"
            : stack.contains("rapids.shuffle") ? "shuffle" : "other";
        boolean overlaps = false;
        boolean inside = false;
        for (long[] gap : gaps) {
          overlaps |= time + span >= gap[0] && time - span < gap[1];
          inside |= time - span >= gap[0] && time + span < gap[1];
        }
        String region = inside ? "recorded-gpu-gap"
            : overlaps || time - span < begin || time + span >= end ? "mixed-or-unresolved"
            : "recorded-gpu-active";
        String key = (wall ? "wall" : "cpu") + ":" + domain + ":" + region;
        Group group = groups.computeIfAbsent(key, ignored -> new Group());
        group.events++;
        group.samples += samples;
        for (String term : new String[]{"FutureTask.get", "GpuSemaphore", "codec_stats::add_pages",
            "allocate_nesting_info", "allocate_level_decode_space", "decompress_page_data",
            "decode_page_data", "CompactProtocolReader", "cudaStreamSynchronize",
            "cuStreamSynchronize", "cudaMallocAsync", "cudaFreeAsync", "hostdevice_vector",
            "materialize_all_columns", "PackedHostToDeviceCopy", "pread", "memcpy", "RmmSpark"}) {
          if (stack.contains(term)) { group.inclusive.merge(term, samples, Long::sum); }
        }
        if (!domain.equals("other")) { group.stacks.merge(stack, samples, Long::sum); }
      }
    }
    try (PrintWriter output = new PrintWriter(args[4])) {
      for (Map.Entry<String, Group> item : groups.entrySet()) {
        Group group = item.getValue();
        output.print("{\"group\":" + json(item.getKey()) + ",\"events\":" + group.events
            + ",\"weighted_samples\":" + group.samples + ",\"top_stacks\":[");
        List<Map.Entry<String, Long>> entries = new ArrayList<>(group.stacks.entrySet());
        entries.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));
        for (int i = 0; i < Math.min(25, entries.size()); i++) {
          if (i > 0) { output.print(','); }
          output.print("{\"samples\":" + entries.get(i).getValue()
              + ",\"stack\":" + json(entries.get(i).getKey()) + "}");
        }
        output.print("],\"inclusive_terms\":{");
        int index = 0;
        for (Map.Entry<String, Long> term : group.inclusive.entrySet()) {
          if (index++ > 0) { output.print(','); }
          output.print(json(term.getKey()) + ":" + term.getValue());
        }
        output.println("}}");
      }
    }
  }
  private ReadPairedStackSamples() {}
}
