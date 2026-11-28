/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
package com.nvidia.spark.rapids.experiments;

import ai.rapids.cudf.BinaryOp;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.DeviceMemoryBuffer;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.HybridScanReader;
import ai.rapids.cudf.NvtxColor;
import ai.rapids.cudf.NvtxRange;
import ai.rapids.cudf.ParquetOptions;
import ai.rapids.cudf.PinnedMemoryPool;
import ai.rapids.cudf.Rmm;
import ai.rapids.cudf.RmmAllocationMode;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import com.nvidia.spark.rapids.jni.PackedHostToDeviceCopy;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Isolates bounded H2D lookahead on real compressed input; not a Spark benchmark. */
public final class BoundedScanPipelineProbe {
  private static final long GIB = 1024L * 1024 * 1024;
  private static final long INPUT_LIMIT = 4 * GIB;
  private static final String[] COLUMNS = {
      "l_orderkey", "l_partkey", "l_suppkey", "l_extendedprice", "l_discount"};
  private static final ParquetOptions OPTIONS = ParquetOptions.builder()
      .includeColumn(COLUMNS).build();

  private static final class Batch implements AutoCloseable {
    final int[] groups;
    final long rows;
    final HostMemoryBuffer backing;
    final HostMemoryBuffer[] chunks;

    Batch(HybridScanReader reader, FileChannel file, long start, long length) throws Exception {
      ArrayList<Integer> selected = new ArrayList<>();
      for (int group : reader.allRowGroups()) {
        long first = Arrays.stream(reader.allColumnChunksByteRanges(new int[]{group}))
            .mapToLong(range -> range.offset()).min().orElse(0);
        if (first >= start && first < start + length) {
          selected.add(group);
        }
      }
      groups = selected.stream().mapToInt(Integer::intValue).toArray();
      if (groups.length == 0) {
        throw new IllegalArgumentException("Empty split");
      }
      rows = reader.totalRowsInRowGroups(groups);
      var ranges = reader.allColumnChunksByteRanges(groups);
      long bytes = Arrays.stream(ranges).mapToLong(range -> range.size()).sum();
      if (bytes <= 0 || bytes + 8L * ranges.length > INPUT_LIMIT / 2) {
        throw new IllegalArgumentException("Split exceeds the two-input memory budget: " + bytes);
      }
      backing = PinnedMemoryPool.tryAllocate(bytes);
      if (backing == null) {
        throw new IllegalStateException("Pinned input allocation failed; no pageable fallback");
      }
      chunks = new HostMemoryBuffer[ranges.length];
      try {
        long offset = 0;
        for (int i = 0; i < ranges.length; i++) {
          long size = ranges[i].size();
          ByteBuffer target = backing.asByteBuffer(offset, Math.toIntExact(size));
          long position = ranges[i].offset();
          while (target.hasRemaining()) {
            int read = file.read(target, position);
            if (read <= 0) {
              throw new IllegalStateException("Short Parquet read");
            }
            position += read;
          }
          chunks[i] = backing.slice(offset, size);
          offset += size;
        }
      } catch (Throwable error) {
        try { close(); } catch (Throwable cleanup) { error.addSuppressed(cleanup); }
        throw error;
      }
      System.out.printf("{\"split_start\":%d,\"groups\":%s,\"rows\":%d,\"bytes\":%d}%n",
          start, Arrays.toString(groups), rows, bytes);
    }

    @Override
    public void close() {
      for (HostMemoryBuffer chunk : chunks) {
        if (chunk != null) {
          chunk.close();
        }
      }
      backing.close();
    }
  }

  private static Table decode(HostMemoryBuffer footer, Batch batch,
      PackedHostToDeviceCopy staged) {
    staged.makeStreamWait(Cuda.DEFAULT_STREAM);
    staged.setFreeStream(Cuda.DEFAULT_STREAM);
    try (HybridScanReader reader = new HybridScanReader(footer, OPTIONS)) {
      Table output = reader.materializeAllColumns(batch.groups, staged.getBuffers());
      if (output.getRowCount() != batch.rows || output.getNumberOfColumns() != COLUMNS.length) {
        output.close();
        throw new AssertionError("Decoded shape differs from footer/projection");
      }
      return output;
    }
  }

  private static void equal(Table reference, Table actual) {
    if (reference.getRowCount() != actual.getRowCount()) {
      throw new AssertionError("Row count differs");
    }
    for (int i = 0; i < COLUMNS.length; i++) {
      if (!reference.getColumn(i).getType().equals(actual.getColumn(i).getType())) {
        throw new AssertionError("Type differs at column " + i);
      }
      try (ColumnVector match = reference.getColumn(i).binaryOp(
          BinaryOp.NULL_EQUALS, actual.getColumn(i), DType.BOOL8);
          Scalar all = match.all()) {
        if (!all.isValid() || !all.getBoolean()) {
          throw new AssertionError("Full ordered column differs: " + COLUMNS[i]);
        }
      }
    }
  }

  private static Table reference(HostMemoryBuffer footer, Batch batch, Cuda.Event event) {
    try (PackedHostToDeviceCopy copy = PackedHostToDeviceCopy.copyAsync(
        batch.chunks, Cuda.DEFAULT_STREAM, 32 * 1024 * 1024, event)) {
      return decode(footer, batch, copy);
    }
  }

  /** Two fixed input slots; the worker performs no device allocation or decoding. */
  private static final class CopyWindow implements AutoCloseable {
    final DeviceMemoryBuffer[] buffers = new DeviceMemoryBuffer[2];
    final DeviceMemoryBuffer[][] chunks = new DeviceMemoryBuffer[2][];
    final ExecutorService worker = Executors.newSingleThreadExecutor();
    final Cuda.Stream stream;
    final Batch[] batches;
    final long chunkBytes;

    CopyWindow(Batch[] batches, Cuda.Stream stream, long chunkBytes) {
      this.batches = batches;
      this.stream = stream;
      this.chunkBytes = chunkBytes;
      try {
        long before = Rmm.getTotalBytesAllocated();
        long bytes = 0;
        for (int slot = 0; slot < 2; slot++) {
          buffers[slot] = DeviceMemoryBuffer.allocate(batches[slot].backing.getLength());
          bytes += buffers[slot].getLength();
          chunks[slot] = new DeviceMemoryBuffer[batches[slot].chunks.length];
          long offset = 0;
          for (int i = 0; i < chunks[slot].length; i++) {
            long size = batches[slot].chunks[i].getLength();
            chunks[slot][i] = buffers[slot].slice(offset, size);
            offset += size;
          }
        }
        if (bytes > INPUT_LIMIT || Rmm.getTotalBytesAllocated() - before < bytes) {
          throw new AssertionError("Input budget or RMM accounting violation");
        }
        Cuda.DEFAULT_STREAM.sync();
      } catch (Throwable error) {
        try { close(); } catch (Throwable cleanup) { error.addSuppressed(cleanup); }
        throw error;
      }
    }

    Future<?> submit(int slot) {
      return worker.submit(() -> {
        Cuda.setDevice(0);
        HostMemoryBuffer host = batches[slot].backing;
        for (long offset = 0; offset < host.getLength(); offset += chunkBytes) {
          long length = Math.min(chunkBytes, host.getLength() - offset);
          buffers[slot].copyFromHostBufferAsync(offset, host, offset, length, stream);
          // One transfer in flight permits decode metadata transfers to enter the copy queue.
          stream.sync();
        }
      });
    }

    @Override
    public void close() {
      worker.shutdown();
      boolean interrupted = false;
      for (;;) {
        try {
          if (worker.awaitTermination(1, TimeUnit.SECONDS)) {
            break;
          }
        } catch (InterruptedException error) {
          interrupted = true;
        }
      }
      try {
        // Every queued transfer finishes before its input or destination is released.
        for (DeviceMemoryBuffer[] views : chunks) {
          if (views != null) {
            for (DeviceMemoryBuffer view : views) {
              if (view != null) {
                view.close();
              }
            }
          }
        }
        for (DeviceMemoryBuffer buffer : buffers) {
          if (buffer != null) {
            buffer.close();
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private static void runWindowed(String mode, int iterations, HostMemoryBuffer footer,
      Batch[] batches, Cuda.Stream stream, Table[] references) throws Exception {
    boolean lookahead = mode.equals("paced") || mode.equals("unpaced");
    long chunkBytes = mode.equals("paced") || mode.equals("pacedserial")
        ? 32 * 1024 * 1024 : 2 * GIB;
    try (CopyWindow window = new CopyWindow(batches, stream, chunkBytes)) {
      long start = System.nanoTime();
      try (NvtxRange range = new NvtxRange("bounded-scan-" + mode, NvtxColor.BLUE)) {
        Future<?> pending = window.submit(0);
        for (int i = 0; i < iterations; i++) {
          int slot = i % 2;
          pending.get();
          if (lookahead && i + 1 < iterations) {
            pending = window.submit((i + 1) % 2);
          }
          try (HybridScanReader reader = new HybridScanReader(footer, OPTIONS);
              Table output = reader.materializeAllColumns(batches[slot].groups, window.chunks[slot])) {
            if (output.getRowCount() != batches[slot].rows) {
              throw new AssertionError("Decoded row count differs");
            }
            if (references != null) {
              equal(references[slot], output);
            }
            // Input slots can be overwritten only after their last GPU reader finishes.
            Cuda.DEFAULT_STREAM.sync();
          }
          if (!lookahead && i + 1 < iterations) {
            pending = window.submit((i + 1) % 2);
          }
        }
      }
      System.out.printf("{\"mode\":\"%s\",\"iterations\":%d,\"seconds\":%.9f,"
          + "\"full_comparison\":%s,\"chunk_bytes\":%d,\"retained_input_bytes\":%d}%n",
          mode, iterations, (System.nanoTime() - start) / 1e9, references != null,
          chunkBytes, batches[0].backing.getLength() + batches[1].backing.getLength());
    }
  }

  private static void run(String mode, int iterations, HostMemoryBuffer footer,
      Batch[] batches, Cuda.Stream copyStream, Cuda.Event[] events, Table[] reference) {
    Cuda.Stream stream = mode.equals("original") ? Cuda.DEFAULT_STREAM : copyStream;
    boolean lookahead = mode.equals("pipeline");
    PackedHostToDeviceCopy pending = null;
    long start = System.nanoTime();
    try (NvtxRange range = new NvtxRange("bounded-scan-" + mode, NvtxColor.GREEN)) {
      for (int i = 0; i < iterations; i++) {
        int slot = i % 2;
        PackedHostToDeviceCopy current = pending;
        pending = null;
        if (current == null) {
          current = PackedHostToDeviceCopy.copyAsync(
              batches[slot].chunks, stream, 32 * 1024 * 1024, events[slot]);
        }
        try (PackedHostToDeviceCopy owned = current) {
          if (lookahead && i + 1 < iterations) {
            int next = (i + 1) % 2;
            pending = PackedHostToDeviceCopy.copyAsync(
                batches[next].chunks, stream, 32 * 1024 * 1024, events[next]);
          }
          try (Table output = decode(footer, batches[slot], owned)) {
            if (reference != null) {
              equal(reference[slot], output);
            }
          }
        }
      }
      Cuda.DEFAULT_STREAM.sync();
      stream.sync();
    } finally {
      if (pending != null) {
        pending.close();
      }
    }
    System.out.printf("{\"mode\":\"%s\",\"iterations\":%d,\"seconds\":%.9f,"
        + "\"full_comparison\":%s,\"device_input_limit\":%d,\"allocated_after\":%d}%n",
        mode, iterations, (System.nanoTime() - start) / 1e9, reference != null,
        INPUT_LIMIT, Rmm.getTotalBytesAllocated());
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      throw new IllegalArgumentException("Expected Parquet path, iterations, mode (all/original/serial/pipeline)");
    }
    int iterations = Integer.parseInt(args[1]);
    if (iterations < 2 || !Arrays.asList("all", "original", "serial", "pipeline", "windowed").contains(args[2])) {
      throw new IllegalArgumentException("Invalid iteration count or mode");
    }
    Cuda.setDevice(0);
    Rmm.initialize(RmmAllocationMode.POOL, null, 24 * GIB);
    PinnedMemoryPool.initialize(INPUT_LIMIT + GIB);
    try (RandomAccessFile input = new RandomAccessFile(args[0], "r")) {
      input.seek(input.length() - 8);
      byte[] trailer = new byte[8];
      input.readFully(trailer);
      if (!Arrays.equals(Arrays.copyOfRange(trailer, 4, 8), new byte[]{'P', 'A', 'R', '1'})) {
        throw new IllegalArgumentException("Invalid Parquet trailer");
      }
      int footerLength = ByteBuffer.wrap(trailer).order(ByteOrder.LITTLE_ENDIAN).getInt();
      if (footerLength <= 0 || footerLength > input.length() - 8) {
        throw new IllegalArgumentException("Invalid footer size");
      }
      byte[] bytes = new byte[footerLength];
      input.seek(input.length() - 8 - footerLength);
      input.readFully(bytes);
      try (HostMemoryBuffer footer = HostMemoryBuffer.allocate(footerLength)) {
        footer.setBytes(0, bytes, 0, bytes.length);
        try (HybridScanReader reader = new HybridScanReader(footer, OPTIONS);
            Batch first = new Batch(reader, input.getChannel(), 0, 2 * GIB);
            Batch second = new Batch(reader, input.getChannel(), 2 * GIB, 2 * GIB);
            Cuda.Stream stream = new Cuda.Stream(true);
            Cuda.Event event0 = new Cuda.Event();
            Cuda.Event event1 = new Cuda.Event()) {
          Batch[] batches = {first, second};
          Cuda.Event[] events = {event0, event1};
          try (Table reference0 = reference(footer, first, event0);
              Table reference1 = reference(footer, second, event1)) {
            Table[] reference = {reference0, reference1};
            run("pipeline", 4, footer, batches, stream, events, reference);
            if (args[2].equals("windowed")) {
              for (String mode : new String[]{"bulkserial", "unpaced", "pacedserial", "paced"}) {
                runWindowed(mode, 4, footer, batches, stream, reference);
              }
            }
          }
          if (args[2].equals("all")) {
            for (String mode : new String[]{"original", "serial", "pipeline", "pipeline", "serial", "original"}) {
              run(mode, iterations, footer, batches, stream, events, null);
            }
          } else if (args[2].equals("windowed")) {
            for (String mode : new String[]{"bulkserial", "unpaced", "pacedserial", "paced",
                "paced", "pacedserial", "unpaced", "bulkserial"}) {
              runWindowed(mode, iterations, footer, batches, stream, null);
            }
          } else {
            run(args[2], iterations, footer, batches, stream, events, null);
          }
        }
      }
      if (Rmm.getTotalBytesAllocated() != 0) {
        throw new AssertionError("Leaked tracked device allocation");
      }
    } finally {
      PinnedMemoryPool.shutdown();
      Rmm.shutdown();
    }
  }

  private BoundedScanPipelineProbe() {}
}
