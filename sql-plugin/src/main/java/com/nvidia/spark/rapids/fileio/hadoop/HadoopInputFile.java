/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.function.IntFunction;

/**
 * Implementation of {@link RapidsInputFile} using the Hadoop file system.
 * <br/>
 * This class provides methods to get the length of the file and to open a seekable input stream
 * for reading the file.
 */
public class HadoopInputFile implements RapidsInputFile {
    private final Path filePath;
    private final FileSystem fs;

    public static HadoopInputFile create(Path filePath, Configuration conf) throws IOException {
        Objects.requireNonNull(filePath, "filePath can't be null!");
        Objects.requireNonNull(conf, "Hadoop conf can't be null");
        FileSystem fs = filePath.getFileSystem(conf);
        return new HadoopInputFile(filePath, fs);
    }

    private HadoopInputFile(Path filePath, FileSystem fs) {
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
     * Vectored read via Hadoop's FSDataInputStream.readVectored. Activates the
     * parallel + range-merged read path of the underlying filesystem driver
     * (e.g. gcs-connector 3.1.9 VectoredIOImpl for gs://, S3A vectored read
     * for s3a://). On filesystems that don't implement vectored I/O, the
     * FSDataInputStream throws UnsupportedOperationException and we fall
     * through to the inherited sequential default.
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

        // CopyRange.length is long; FileRange takes int. Split ranges that exceed
        // Integer.MAX_VALUE into chunks. Parquet column chunks rarely exceed
        // 2 GiB but split keeps the contract safe. Stash destination offset
        // in the FileRange reference for the drain phase.
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

        // Direct ByteBuffer allocator: avoids an extra heap-to-native copy on the
        // FS driver side and enables gRPC zero-copy on gcs-connector.
        IntFunction<ByteBuffer> allocate = ByteBuffer::allocateDirect;

        try (FSDataInputStream stream = fs.open(filePath)) {
            try {
                stream.readVectored(fileRanges, allocate);
            } catch (UnsupportedOperationException uoe) {
                // FS implementation doesn't support vectored I/O. Fall back to
                // the inherited sequential default.
                RapidsInputFile.super.readVectored(output, copyRanges);
                return;
            }

            // Drain each future and copy into the destination at the offset
            // stashed in FileRange.reference.
            for (FileRange r : fileRanges) {
                ByteBuffer src;
                try {
                    src = r.getData().get();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "readVectored interrupted at offset " + r.getOffset(), ie);
                } catch (ExecutionException ee) {
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
            }
        }
    }
}
