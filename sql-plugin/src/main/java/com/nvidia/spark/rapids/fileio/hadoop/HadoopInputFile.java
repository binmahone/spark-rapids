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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

/**
 * Implementation of {@link RapidsInputFile} using the Hadoop file system.
 * <br/>
 * Provides length / open / vectored read. Vectored read dispatches through
 * {@link FSDataInputStream#readVectored(List, IntFunction)} so the underlying
 * filesystem driver's parallel + range-merged path is used
 * (gcs-connector 3.1.9 {@code VectoredIOImpl} for {@code gs://}, S3A vectored
 * read for {@code s3a://}, etc.).
 *
 * <p>The {@code FSDataInputStream.readVectored} API was added in Hadoop 3.3.5
 * (HADOOP-18103). spark-rapids' compile classpath is uniformly Hadoop 3.4.x so
 * the call compiles for every shim build. At runtime, if the deployed Hadoop is
 * older or the FS driver doesn't implement vectored I/O, the call throws
 * {@link UnsupportedOperationException} or {@link NoSuchMethodError} and we
 * fall back to {@link RapidsInputFile}'s sequential default. Until further
 * notice this path is exercised primarily on the Spark 4 / Dataproc 3.0 stack;
 * older shims silently take the fallback path.
 */
public class HadoopInputFile implements RapidsInputFile {
    // ----------------------------------------------------------------------
    // Per-executor DirectByteBuffer pool for the vectored read path.
    //
    // Rationale: under sustained concurrent vectored reads (NDS queries with
    // wide parquet column scans), repeatedly calling ByteBuffer.allocateDirect
    // -> GC -> Cleaner -> munmap churns off-heap memory. The previous
    // optimization attempted to short-circuit this with reflective
    // Cleaner.clean per range; that helped one outlier query but added
    // per-range reflection overhead that net-slowed other queries.
    //
    // This pool reuses DirectByteBuffers across calls. Keying by exact
    // capacity is intentional — parquet column chunks within a file repeat at
    // identical sizes, so exact-match hit rate is high on real workloads and
    // we never hand out a buffer larger than requested (avoids confusing
    // the FS impl's range bookkeeping).
    //
    // Caps:
    //   - MAX_DISTINCT_SIZES bounds the number of size classes tracked
    //     (excess sizes bypass the pool, get GC'd normally)
    //   - MAX_PER_SIZE bounds how many idle buffers per size class
    //   - On overflow the buffer is dropped to GC, no leak
    //
    // The pool is a static JVM-singleton on the executor, shared across all
    // HadoopInputFile instances and all tasks. ConcurrentLinkedQueue gives
    // lock-free poll/offer; ConcurrentHashMap.computeIfAbsent is the only
    // synchronization point and only on first sight of a new size class.
    // ----------------------------------------------------------------------
    private static final int MAX_DISTINCT_SIZES = 256;
    private static final int MAX_PER_SIZE = 64;
    private static final ConcurrentHashMap<Integer, ConcurrentLinkedQueue<ByteBuffer>> POOL =
            new ConcurrentHashMap<>();
    private static final AtomicInteger POOL_HITS = new AtomicInteger();
    private static final AtomicInteger POOL_MISSES = new AtomicInteger();

    private static ByteBuffer poolAllocate(int size) {
        ConcurrentLinkedQueue<ByteBuffer> q = POOL.get(size);
        if (q != null) {
            ByteBuffer buf = q.poll();
            if (buf != null) {
                buf.clear();
                POOL_HITS.incrementAndGet();
                return buf;
            }
        }
        POOL_MISSES.incrementAndGet();
        return ByteBuffer.allocateDirect(size);
    }

    private static void poolRelease(ByteBuffer buf) {
        if (buf == null || !buf.isDirect()) {
            return;
        }
        int capacity = buf.capacity();
        if (POOL.size() >= MAX_DISTINCT_SIZES && !POOL.containsKey(capacity)) {
            // Don't admit a new size class once we're at the size-class cap.
            // The buffer will be reclaimed by GC via its Cleaner.
            return;
        }
        ConcurrentLinkedQueue<ByteBuffer> q =
                POOL.computeIfAbsent(capacity, k -> new ConcurrentLinkedQueue<>());
        if (q.size() < MAX_PER_SIZE) {
            q.offer(buf);
        }
        // else: drop to GC.
    }

    private static void cancelRemainingFutures(List<FileRange> ranges, int fromIdx) {
        for (int i = fromIdx; i < ranges.size(); i++) {
            try {
                ranges.get(i).getData().cancel(true);
            } catch (Throwable ignored) {
                // best effort; the JVM will clean up via Cleaner eventually
            }
        }
    }

    protected final Path filePath;
    protected final FileSystem fs;

    public static HadoopInputFile create(Path filePath, Configuration conf) throws IOException {
        Objects.requireNonNull(filePath, "filePath can't be null!");
        Objects.requireNonNull(conf, "Hadoop conf can't be null");
        FileSystem fs = filePath.getFileSystem(conf);
        return new HadoopInputFile(filePath, fs);
    }

    protected HadoopInputFile(Path filePath, FileSystem fs) {
        Objects.requireNonNull(filePath, "filePath can't be null!");
        Objects.requireNonNull(fs, "FileSystem can't be null");
        this.filePath = filePath;
        this.fs = fs;
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

    /**
     * Vectored read via Hadoop's {@link FSDataInputStream#readVectored}.
     * Activates the parallel + range-merged read path of the underlying
     * filesystem driver. On filesystems that don't support vectored I/O or
     * on Hadoop versions older than 3.3.5, falls back to the sequential
     * default inherited from {@link RapidsInputFile}.
     *
     * <p>After each per-range copy into {@code output}, the source
     * DirectByteBuffer is returned to a per-executor pool keyed by exact
     * capacity. Subsequent vectored reads that need the same size hit the
     * pool instead of allocating a fresh direct buffer + later triggering
     * its Cleaner. This keeps off-heap churn flat under sustained vectored
     * read concurrency.
     */
    @Override
    public void readVectored(HostMemoryBuffer output, List<RapidsInputFile.CopyRange> copyRanges)
            throws IOException {
        Objects.requireNonNull(output, "output can't be null");
        Objects.requireNonNull(copyRanges, "copyRanges can't be null");
        if (copyRanges.isEmpty()) {
            return;
        }

        // Validate output bounds before issuing any I/O (matches the default impl).
        long outputLength = output.getLength();
        for (RapidsInputFile.CopyRange r : copyRanges) {
            Objects.requireNonNull(r, "copyRange can't be null");
            long end = r.getOutputOffset() + r.getLength();
            if (end < 0 || end > outputLength) {
                throw new IllegalArgumentException(
                        "Output buffer length " + outputLength
                                + " is smaller than requested end " + end);
            }
        }

        // CopyRange.length is long; FileRange takes int. Split ranges that
        // exceed Integer.MAX_VALUE into chunks. Parquet column chunks rarely
        // exceed 2 GiB but the split keeps the contract safe. The destination
        // offset is stashed in the FileRange reference for the drain phase.
        List<FileRange> fileRanges = new ArrayList<>(copyRanges.size());
        for (RapidsInputFile.CopyRange r : copyRanges) {
            long remaining = r.getLength();
            long inOff = r.getInputOffset();
            long outOff = r.getOutputOffset();
            while (remaining > 0) {
                int chunkLen = (int) Math.min(remaining, (long) Integer.MAX_VALUE);
                fileRanges.add(FileRange.createFileRange(inOff, chunkLen, Long.valueOf(outOff)));
                inOff += chunkLen;
                outOff += chunkLen;
                remaining -= chunkLen;
            }
        }

        // Pooled direct-buffer allocator: returns a recycled DirectByteBuffer
        // from the per-executor pool when one of the exact requested size is
        // available, otherwise allocates fresh. Reuse eliminates the off-heap
        // alloc / Cleaner churn under sustained concurrent vectored reads.
        IntFunction<ByteBuffer> allocate = HadoopInputFile::poolAllocate;

        try (FSDataInputStream stream = fs.open(filePath)) {
            try {
                stream.readVectored(fileRanges, allocate);
            } catch (UnsupportedOperationException | NoSuchMethodError uoe) {
                // FS driver / runtime Hadoop does not implement vectored I/O.
                // Fall back to the sequential default.
                RapidsInputFile.super.readVectored(output, copyRanges);
                return;
            }

            // Drain each future and copy into the destination at the offset
            // stashed in FileRange.reference. After each copy, explicitly free
            // the source DirectByteBuffer instead of waiting for GC to trigger
            // its Cleaner — this keeps off-heap RSS flat under sustained
            // vectored read concurrency.
            for (int i = 0; i < fileRanges.size(); i++) {
                FileRange r = fileRanges.get(i);
                ByteBuffer src;
                try {
                    src = r.getData().get();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    cancelRemainingFutures(fileRanges, i + 1);
                    throw new IOException(
                            "readVectored interrupted at offset " + r.getOffset(), ie);
                } catch (ExecutionException ee) {
                    cancelRemainingFutures(fileRanges, i + 1);
                    Throwable cause = ee.getCause();
                    if (cause instanceof IOException) {
                        throw (IOException) cause;
                    }
                    throw new IOException(
                            "readVectored failed at offset " + r.getOffset(), cause);
                }
                long destOffset = (Long) r.getReference();
                int len = src.remaining();
                // asByteBuffer returns a view into HostMemoryBuffer at the
                // destination slice; put(src) copies bytes (direct-to-direct
                // memcpy when both buffers are direct).
                ByteBuffer dst = output.asByteBuffer(destOffset, len);
                dst.put(src);
                // Return the source DirectByteBuffer to the pool for reuse on
                // the next readVectored call needing this size class. Avoids
                // both the alloc churn and the per-call reflection cost of an
                // explicit Cleaner.clean.
                poolRelease(src);
            }
        }
    }
}
