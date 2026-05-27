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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.function.IntFunction;

/**
 * {@link RapidsInputFile} backed by the Hadoop FileSystem API.
 *
 * <p>Vectored read dispatches to {@link FSDataInputStream#readVectored} so the
 * underlying FS driver's parallel + range-merged path is used (gcs-connector
 * {@code VectoredIOImpl} for {@code gs://}, S3A vectored read for
 * {@code s3a://}, etc.). The API was added in HADOOP-18103 (Hadoop 3.3.5);
 * on older runtimes or drivers without vectored support the call throws
 * {@link UnsupportedOperationException} or {@link NoSuchMethodError} and we
 * fall back to the sequential default inherited from {@link RapidsInputFile}.
 */
public class HadoopInputFile implements RapidsInputFile {

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
     * Vectored read via {@link FSDataInputStream#readVectored}.
     *
     * <p>Each range allocates its own pageable {@link HostMemoryBuffer}
     * ({@code preferPinned=false}) inside the {@code IntFunction}. Hadoop's
     * worker threads fill the buffers asynchronously; the drain loop memcpys
     * each filled buffer into {@code output} at the offset stashed in
     * {@link FileRange#getReference()}. After drain, every per-range HMB is
     * closed in {@code finally} — deterministic release, no pool, no
     * long-lived retention.
     */
    @Override
    public void readVectored(HostMemoryBuffer output, List<RapidsInputFile.CopyRange> copyRanges)
            throws IOException {
        if (copyRanges.isEmpty()) {
            return;
        }

        // CopyRange.length is long; FileRange takes int. Split ranges that
        // exceed Integer.MAX_VALUE into chunks. Each chunk's destination
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

        // ConcurrentLinkedQueue because Hadoop may invoke the IntFunction
        // from internal worker threads.
        final Queue<HostMemoryBuffer> perRangeBuffers = new ConcurrentLinkedQueue<>();
        IntFunction<ByteBuffer> allocate = length -> {
            HostMemoryBuffer hmb = HostMemoryBuffer.allocate(length, false);
            perRangeBuffers.add(hmb);
            return hmb.asByteBuffer(0L, length);
        };

        try (FSDataInputStream stream = fs.open(filePath)) {
            try {
                stream.readVectored(fileRanges, allocate);
            } catch (UnsupportedOperationException | NoSuchMethodError uoe) {
                closeAll(perRangeBuffers);
                RapidsInputFile.super.readVectored(output, copyRanges);
                return;
            }

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
                    output.asByteBuffer(destOffset, len).put(src);
                }
            } finally {
                closeAll(perRangeBuffers);
            }
        }
    }

    private static void cancelRemainingFutures(List<FileRange> ranges, int fromIdx) {
        for (int i = fromIdx; i < ranges.size(); i++) {
            try {
                ranges.get(i).getData().cancel(true);
            } catch (Throwable ignored) {
                // best effort
            }
        }
    }

    private static void closeAll(Queue<HostMemoryBuffer> buffers) {
        HostMemoryBuffer hmb;
        while ((hmb = buffers.poll()) != null) {
            try {
                hmb.close();
            } catch (Throwable ignored) {
                // best effort
            }
        }
    }
}
