/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.fileio.hadoop;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import com.nvidia.spark.rapids.jni.fileio.SeekableInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of {@link RapidsInputFile} using the Hadoop file system.
 * <br/>
 * This class provides methods to get the length of the file and to open a seekable input stream
 * for reading the file.
 */
public class HadoopInputFile implements RapidsInputFile {
    private static final String PARQUET_READ_ALLOCATION_SIZE = "parquet.read.allocation.size";

    private final Path filePath;
    private final FileSystem fs;
    private final int copyBufferSize;

    public static HadoopInputFile create(Path filePath, Configuration conf) throws IOException {
        Objects.requireNonNull(filePath, "filePath can't be null!");
        Objects.requireNonNull(conf, "Hadoop conf can't be null");
        FileSystem fs = filePath.getFileSystem(conf);
        int copyBufferSize = conf.getInt(PARQUET_READ_ALLOCATION_SIZE,
                RapidsInputFile.DEFAULT_READ_VECTORED_COPY_BUFFER_SIZE);
        return new HadoopInputFile(filePath, fs, copyBufferSize);
    }

    private HadoopInputFile(Path filePath, FileSystem fs, int copyBufferSize) {
        Objects.requireNonNull(filePath, "filePath can't be null!");
        Objects.requireNonNull(fs, "FileSystem can't be null");
        if (copyBufferSize <= 0) {
            throw new IllegalArgumentException(PARQUET_READ_ALLOCATION_SIZE + " must be positive");
        }
        this.filePath = filePath;
        this.fs = fs;
        this.copyBufferSize = copyBufferSize;
    }

    @Override
    public String path() {
        return filePath.toString();
    }

    @Override
    public long getLength() throws IOException {
        return fs.getFileStatus(this.filePath).getLen();
    }

    @Override
    public OptionalLong getLastModificationTime() throws IOException {
        return OptionalLong.of(fs.getFileStatus(this.filePath).getModificationTime());
    }

    @Override
    public SeekableInputStream open() throws IOException {
        return new HadoopInputStream(fs.open(filePath));
    }

    @Override
    public void readVectored(HostMemoryBuffer output, List<RapidsInputFile.CopyRange> copyRanges)
            throws IOException {
        if (copyRanges.isEmpty()) {
            return;
        }
        boolean timing = Boolean.getBoolean("rapids.test.rangeIoTiming");
        long started = timing ? System.nanoTime() : 0;
        if (Boolean.getBoolean("rapids.test.kvikioHostRead")) {
            KvikioHostRangeReader.read(filePath.toUri(), output, copyRanges);
            return;
        }
        if (Boolean.getBoolean("rapids.test.directHostRead")) {
            try {
                readDirectToHost(output, copyRanges);
            } finally {
                if (timing) {
                    System.err.println("RANGE_IO_OUTER mode=direct_host thread="
                            + Thread.currentThread().getName().replace(' ', '_')
                            + " allocateNs=0 totalNs=" + (System.nanoTime() - started)
                            + " path=" + filePath);
                }
            }
            return;
        }
        byte[] copyBuffer = new byte[getCopyBufferAllocationSize(copyRanges, copyBufferSize)];
        long allocated = timing ? System.nanoTime() : 0;
        try {
            RapidsInputFile.readVectoredUsingCopyBuffer(this, output, copyRanges, copyBuffer);
        } finally {
            if (timing) {
                System.err.println("RANGE_IO_OUTER mode=hadoop_heap thread="
                        + Thread.currentThread().getName()
                        .replace(' ', '_') + " allocateNs=" + (allocated - started)
                        + " totalNs=" + (System.nanoTime() - started) + " path=" + filePath);
            }
        }
    }

    /** Diagnostic local-file read with the same ranges and chunk cap, without heap staging. */
    private void readDirectToHost(HostMemoryBuffer output, List<RapidsInputFile.CopyRange> ranges)
            throws IOException {
        Objects.requireNonNull(output, "output can't be null");
        Objects.requireNonNull(ranges, "ranges can't be null");
        String scheme = filePath.toUri().getScheme();
        String authority = filePath.toUri().getAuthority();
        if ((scheme != null && !"file".equals(scheme))
                || (authority != null && !authority.isEmpty())
                || !filePath.toUri().getPath().startsWith("/")) {
            throw new IOException("Diagnostic direct-host reads require an absolute local file");
        }
        for (RapidsInputFile.CopyRange range : ranges) {
            Objects.requireNonNull(range, "range can't be null");
            long length = range.getLength();
            long offset = range.getOutputOffset();
            if (range.getInputOffset() < 0 || length < 0 || offset < 0
                    || offset > output.getLength() || length > output.getLength() - offset
                    || range.getInputOffset() > Long.MAX_VALUE - length) {
                throw new IllegalArgumentException("Invalid diagnostic direct-host range");
            }
        }
        int lanes = Integer.getInteger("rapids.test.rangeReadConcurrency", 1);
        if (lanes != 1) {
            readParallelToHost(output, ranges, lanes);
            return;
        }
        boolean timing = Boolean.getBoolean("rapids.test.rangeIoTiming");
        long bytes = 0;
        long calls = 0;
        long readNanos = 0;
        boolean completed = false;
        try (FileChannel channel = FileChannel.open(Paths.get(filePath.toUri().getPath()),
                StandardOpenOption.READ)) {
            long started = timing ? System.nanoTime() : 0;
            try {
                for (RapidsInputFile.CopyRange range : ranges) {
                    long remaining = range.getLength();
                    long inputOffset = range.getInputOffset();
                    long outputOffset = range.getOutputOffset();
                    while (remaining > 0) {
                        int chunk = (int) Math.min(remaining, copyBufferSize);
                        ByteBuffer view = output.asByteBuffer(outputOffset, chunk);
                        if (!view.isDirect()) {
                            throw new IllegalStateException("Diagnostic host view must be direct");
                        }
                        long readStarted = timing ? System.nanoTime() : 0;
                        while (view.hasRemaining()) {
                            int count = channel.read(view, inputOffset);
                            if (count < 0) {
                                throw new EOFException("Short diagnostic direct-host read");
                            }
                            if (count == 0) {
                                throw new IOException("Diagnostic direct-host read made no progress");
                            }
                            inputOffset += count;
                        }
                        if (timing) {
                            readNanos += System.nanoTime() - readStarted;
                        }
                        bytes += chunk;
                        calls++;
                        outputOffset += chunk;
                        remaining -= chunk;
                    }
                }
                completed = true;
            } finally {
                if (timing) {
                    System.err.println("RANGE_IO_TIMING mode=direct_host thread="
                            + Thread.currentThread().getName().replace(' ', '_')
                            + " bytes=" + bytes + " calls=" + calls + " readNs=" + readNanos
                            + " copyNs=0 seekNs=0 totalNs=" + (System.nanoTime() - started)
                            + " bufferBytes=" + copyBufferSize + " ranges=" + ranges.size()
                            + " completed=" + completed);
                }
            }
        }
    }

    /** Positional reads use separate channels and disjoint slices of the caller's host buffer. */
    private void readParallelToHost(HostMemoryBuffer output,
            List<RapidsInputFile.CopyRange> ranges, int requestedLanes) throws IOException {
        List<RapidsInputFile.CopyRange> nonempty = new ArrayList<>();
        for (RapidsInputFile.CopyRange range : ranges) {
            if (range.getLength() > 0) {
                nonempty.add(range);
            }
        }
        List<RapidsInputFile.CopyRange> byOutput = new ArrayList<>(nonempty);
        byOutput.sort(Comparator.comparingLong(RapidsInputFile.CopyRange::getOutputOffset));
        long end = 0;
        for (RapidsInputFile.CopyRange range : byOutput) {
            if (range.getOutputOffset() < end) {
                throw new IllegalArgumentException("Parallel diagnostic ranges overlap in output");
            }
            end = range.getOutputOffset() + range.getLength();
        }
        if (requestedLanes < 1 || requestedLanes > DiagnosticRangeExecutor.GLOBAL_LIMIT) {
            throw new IllegalArgumentException("Invalid diagnostic range concurrency");
        }
        int lanes = Math.min(requestedLanes, Math.max(1, nonempty.size()));
        AtomicLong bytes = new AtomicLong();
        AtomicLong calls = new AtomicLong();
        AtomicLong readNanos = new AtomicLong();
        boolean completed = false;
        int observedPeak = 0;
        long started = System.nanoTime();
        try {
            observedPeak = DiagnosticRangeExecutor.run(lanes, lane -> {
                try (FileChannel channel = FileChannel.open(Paths.get(filePath.toUri().getPath()),
                        StandardOpenOption.READ)) {
                    for (int index = lane; index < nonempty.size(); index += lanes) {
                        RapidsInputFile.CopyRange range = nonempty.get(index);
                        long remaining = range.getLength();
                        long inputOffset = range.getInputOffset();
                        long outputOffset = range.getOutputOffset();
                        while (remaining > 0) {
                            int chunk = (int) Math.min(remaining, copyBufferSize);
                            ByteBuffer view = output.asByteBuffer(outputOffset, chunk);
                            if (!view.isDirect()) {
                                throw new IllegalStateException("Diagnostic host view must be direct");
                            }
                            long readStarted = System.nanoTime();
                            while (view.hasRemaining()) {
                                int count = channel.read(view, inputOffset);
                                if (count < 0) {
                                    throw new EOFException("Short parallel diagnostic read");
                                }
                                if (count == 0) {
                                    throw new IOException("Parallel read made no progress");
                                }
                                inputOffset += count;
                            }
                            readNanos.addAndGet(System.nanoTime() - readStarted);
                            bytes.addAndGet(chunk);
                            calls.incrementAndGet();
                            outputOffset += chunk;
                            remaining -= chunk;
                        }
                    }
                }
            });
            completed = true;
        } finally {
            System.err.println("RANGE_IO_TIMING mode=direct_host thread="
                    + Thread.currentThread().getName().replace(' ', '_')
                    + " bytes=" + bytes.get() + " calls=" + calls.get()
                    + " readNs=" + readNanos.get() + " copyNs=0 seekNs=0 totalNs="
                    + (System.nanoTime() - started) + " bufferBytes=" + copyBufferSize
                    + " ranges=" + ranges.size() + " completed=" + completed);
            System.err.println("RANGE_IO_PARALLEL requested=" + requestedLanes + " lanes=" + lanes
                    + " observedPeak=" + observedPeak + " globalPeak="
                    + DiagnosticRangeExecutor.globalPeak() + " globalLimit="
                    + DiagnosticRangeExecutor.GLOBAL_LIMIT + " completed=" + completed);
        }
    }

    static int getCopyBufferAllocationSize(
            List<RapidsInputFile.CopyRange> copyRanges, int copyBufferSize) {
        long maxRangeLength = 0;
        for (RapidsInputFile.CopyRange copyRange : copyRanges) {
            maxRangeLength = Math.max(maxRangeLength, copyRange.getLength());
            if (maxRangeLength >= copyBufferSize) {
                return copyBufferSize;
            }
        }
        return (int) maxRangeLength;
    }
}
