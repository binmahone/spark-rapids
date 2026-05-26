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
    // Per-executor scratch DirectByteBuffer pool for vectored read.
    //
    // Each readVectored call allocates ONE scratch DirectByteBuffer sized to
    // sum(range.length). The allocator hands out non-overlapping slices of
    // that scratch to Hadoop's IntFunction, advancing an offset counter. After
    // the drain loop copies each range's data into the destination
    // HostMemoryBuffer, the whole scratch goes back into a per-executor pool
    // for reuse on the next call.
    //
    // Why one big buffer + slices instead of one DirectByteBuffer per range:
    //   - 1 mmap per readVectored call instead of N — far less kernel
    //     mm_struct lock pressure on the executor's high-concurrency path
    //   - Slice operations are pure arithmetic on the parent's address; no
    //     extra native allocation
    //   - On exception, freeing/recycling one scratch is simpler than tracking
    //     N partially filled buffers
    //
    // Pool keying: size classes (next power of 2 of the requested total size,
    // floored at MIN_BUCKET to avoid micro-allocations, ceilinged at
    // MAX_BUCKET to keep the worst-case waste bounded). Power-of-2 bucketing
    // guarantees at most 2x waste vs the requested size and gives a small
    // fixed bucket count.
    //
    // Slicing uses duplicate() + position/limit + slice() (Java 8+ available;
    // spark-rapids sql-plugin's compile target is older than JDK 13 which
    // would otherwise allow the more concise slice(int, int)). duplicate()
    // returns a buffer that shares the SAME memory as the parent but has
    // INDEPENDENT position/limit/mark — that means concurrent IntFunction
    // calls from Hadoop's internal worker threads each get their own
    // duplicate and don't race on parent state. AtomicInteger handles the
    // offset bookkeeping. As long as the scratch capacity is at least the
    // sum of requested lengths, all slices land in disjoint regions.
    //
    // Caps: SCRATCH_BUCKETS bounds the number of size classes (max ~32 from
    // MIN_BUCKET=64KB to MAX_BUCKET=2GB, power-of-2 spaced). SCRATCH_PER_BUCKET
    // bounds idle scratches per bucket. Overflow drops to GC, no leak.
    // ----------------------------------------------------------------------
    private static final int MIN_BUCKET = 64 * 1024;          // 64 KiB
    private static final int MAX_BUCKET = Integer.MAX_VALUE;  // ~2 GiB
    private static final int SCRATCH_PER_BUCKET = 8;
    private static final ConcurrentHashMap<Integer, ConcurrentLinkedQueue<ByteBuffer>> SCRATCH_POOL =
            new ConcurrentHashMap<>();
    private static final AtomicInteger SCRATCH_HITS = new AtomicInteger();
    private static final AtomicInteger SCRATCH_MISSES = new AtomicInteger();

    /** Round size up to the next power of 2, clamped to [MIN_BUCKET, MAX_BUCKET]. */
    private static int sizeClass(int size) {
        if (size <= MIN_BUCKET) return MIN_BUCKET;
        // highestOneBit(size-1) << 1 gives next power of 2 (size > 0).
        int bucket = Integer.highestOneBit(size - 1) << 1;
        // Defensive: if size-1 has the top bit set (>=2^30), the shift overflows
        // negative — fall through to MAX_BUCKET.
        if (bucket <= 0 || bucket > MAX_BUCKET) return MAX_BUCKET;
        return bucket;
    }

    /** Allocate a scratch buffer of capacity >= size from the pool, or fresh. */
    private static ByteBuffer allocateScratch(int size) {
        int bucket = sizeClass(size);
        ConcurrentLinkedQueue<ByteBuffer> q = SCRATCH_POOL.get(bucket);
        if (q != null) {
            ByteBuffer buf = q.poll();
            if (buf != null) {
                // Reset state. position/limit may have been set by Hadoop's
                // last use; slice(int, int) ignores them but clear() keeps
                // the buffer's invariants tidy for any future bulk ops.
                buf.clear();
                SCRATCH_HITS.incrementAndGet();
                return buf;
            }
        }
        SCRATCH_MISSES.incrementAndGet();
        return ByteBuffer.allocateDirect(bucket);
    }

    /** Return a scratch buffer to the pool (or drop to GC on overflow). */
    private static void releaseScratch(ByteBuffer scratch) {
        if (scratch == null || !scratch.isDirect()) {
            return;
        }
        int bucket = scratch.capacity();
        ConcurrentLinkedQueue<ByteBuffer> q =
                SCRATCH_POOL.computeIfAbsent(bucket, k -> new ConcurrentLinkedQueue<>());
        if (q.size() < SCRATCH_PER_BUCKET) {
            q.offer(scratch);
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
     * <p>Buffer management: a single scratch DirectByteBuffer is allocated
     * (or reused from a pool) sized to {@code sum(range.length)}. The
     * IntFunction allocator hands out non-overlapping slices of that scratch
     * to Hadoop. After all ranges are drained and copied into the destination
     * {@code output}, the scratch is returned to the pool. This collapses N
     * mmap/munmap cycles per call into one (or zero, on pool hit) while
     * keeping the existing memcpy from scratch to {@code output}.
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

        // Build FileRange list and accumulate total bytes needed for the scratch.
        // CopyRange.length is long; FileRange takes int. Split ranges that
        // exceed Integer.MAX_VALUE into chunks. Parquet column chunks rarely
        // exceed 2 GiB but the split keeps the contract safe. The destination
        // offset is stashed in the FileRange reference for the drain phase.
        List<FileRange> fileRanges = new ArrayList<>(copyRanges.size());
        long totalBytes = 0L;
        for (RapidsInputFile.CopyRange r : copyRanges) {
            long remaining = r.getLength();
            long inOff = r.getInputOffset();
            long outOff = r.getOutputOffset();
            while (remaining > 0) {
                int chunkLen = (int) Math.min(remaining, (long) Integer.MAX_VALUE);
                fileRanges.add(FileRange.createFileRange(inOff, chunkLen, Long.valueOf(outOff)));
                totalBytes += chunkLen;
                inOff += chunkLen;
                outOff += chunkLen;
                remaining -= chunkLen;
            }
        }

        // The scratch capacity is a single int, so the sum of all range
        // lengths must fit. If a single readVectored call exceeds 2 GiB
        // (extremely rare — Parquet column chunks combined), fall through
        // to the sequential default which uses a stream per range.
        if (totalBytes > MAX_BUCKET) {
            RapidsInputFile.super.readVectored(output, copyRanges);
            return;
        }

        final ByteBuffer scratch = allocateScratch((int) totalBytes);
        // The slice offset is advanced atomically per IntFunction call; Hadoop
        // may invoke this concurrently from internal worker threads. We use
        // duplicate() + position/limit + slice() (Java 8 compatible) — each
        // duplicate has independent position/limit/mark so concurrent callers
        // don't race on the parent's state.
        final AtomicInteger sliceOffset = new AtomicInteger(0);
        IntFunction<ByteBuffer> allocate = length -> {
            int start = sliceOffset.getAndAdd(length);
            ByteBuffer dup = scratch.duplicate();
            dup.position(start);
            dup.limit(start + length);
            return dup.slice();
        };

        try (FSDataInputStream stream = fs.open(filePath)) {
            try {
                stream.readVectored(fileRanges, allocate);
            } catch (UnsupportedOperationException | NoSuchMethodError uoe) {
                // FS driver / runtime Hadoop does not implement vectored I/O.
                // Fall back to the sequential default.
                releaseScratch(scratch);
                RapidsInputFile.super.readVectored(output, copyRanges);
                return;
            }

            // Drain each range's slice and copy into the destination at the
            // offset stashed in FileRange.reference. The drain loop runs on
            // the caller's thread; success path returns the scratch to the
            // pool at the end. Failure paths cancel pending futures (so their
            // slices stop being filled) and still return the scratch.
            try {
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
                    // memcpy when both buffers are direct). The src slice
                    // remains tied to the scratch parent — releasing the scratch
                    // happens once all slices have been drained, in the finally
                    // block below.
                    ByteBuffer dst = output.asByteBuffer(destOffset, len);
                    dst.put(src);
                }
            } finally {
                // All slices have been drained (or their owning futures
                // cancelled). The scratch parent can be safely reused.
                releaseScratch(scratch);
            }
        }
    }
}
