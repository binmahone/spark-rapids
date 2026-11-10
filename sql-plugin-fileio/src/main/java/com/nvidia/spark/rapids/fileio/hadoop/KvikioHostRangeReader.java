/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile.CopyRange;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Experimental POSIX-to-host KvikIO bridge; it does not alter Hybrid decoding or H2D. */
final class KvikioHostRangeReader {
    private static final int THREADS = Integer.getInteger("rapids.test.kvikioThreads", 32);
    private static final int WINDOW = Integer.getInteger("rapids.test.kvikioRangeWindow", 1);
    private static final long TASK_BYTES = Long.getLong("rapids.test.kvikioTaskBytes", 4194304L);

    static {
        String library = System.getProperty("rapids.test.kvikioLibrary");
        if (library == null || !library.startsWith("/")) {
            throw new IllegalArgumentException("Set an absolute diagnostic KvikIO library path");
        }
        if (THREADS < 1 || THREADS > 128 || WINDOW < 1 || WINDOW > 128 || TASK_BYTES < 1) {
            throw new IllegalArgumentException("Invalid diagnostic KvikIO limits");
        }
        System.load(library);
        initializeNative(THREADS);
        System.err.println("KVIKIO_HOST_BRIDGE_LOADED threads=" + THREADS
                + " window=" + WINDOW + " taskBytes=" + TASK_BYTES + " library=" + library
                + " backend=" + backendNative());
    }

    static void read(URI uri, HostMemoryBuffer output, List<CopyRange> ranges) throws IOException {
        Objects.requireNonNull(output, "output can't be null");
        Objects.requireNonNull(ranges, "ranges can't be null");
        String scheme = uri.getScheme();
        if ((scheme != null && !"file".equals(scheme))
                || (uri.getAuthority() != null && !uri.getAuthority().isEmpty())
                || uri.getPath() == null || !uri.getPath().startsWith("/")) {
            throw new IOException("Diagnostic KvikIO reads require an absolute local file");
        }
        long[] offsets = new long[ranges.size()];
        long[] lengths = new long[ranges.size()];
        long[] destinations = new long[ranges.size()];
        long expected = 0;
        for (int i = 0; i < ranges.size(); i++) {
            CopyRange range = Objects.requireNonNull(ranges.get(i), "range can't be null");
            long length = range.getLength();
            long offset = range.getOutputOffset();
            if (length <= 0 || offset < 0 || offset > output.getLength()
                    || length > output.getLength() - offset || range.getInputOffset() < 0
                    || range.getInputOffset() > Long.MAX_VALUE - length) {
                throw new IllegalArgumentException("Invalid diagnostic KvikIO range");
            }
            offsets[i] = range.getInputOffset();
            lengths[i] = length;
            destinations[i] = offset;
            expected = Math.addExact(expected, length);
        }
        if (WINDOW > 1) {
            List<CopyRange> ordered = new ArrayList<>(ranges);
            ordered.sort(Comparator.comparingLong(CopyRange::getOutputOffset));
            long end = 0;
            for (CopyRange range : ordered) {
                if (range.getOutputOffset() < end) {
                    throw new IllegalArgumentException("Parallel KvikIO output ranges overlap");
                }
                end = range.getOutputOffset() + range.getLength();
            }
        }
        long started = System.nanoTime();
        boolean completed = false;
        try {
            long count = readNative(uri.getPath().getBytes(StandardCharsets.UTF_8),
                    output.getAddress(), output.getLength(), offsets, lengths, destinations,
                    TASK_BYTES, WINDOW);
            if (count != expected) {
                throw new IOException("KvikIO returned " + count + " of " + expected + " bytes");
            }
            completed = true;
        } finally {
            System.err.println("KVIKIO_HOST_RANGE bytes=" + expected + " ranges=" + ranges.size()
                    + " threads=" + THREADS + " window=" + WINDOW + " taskBytes=" + TASK_BYTES
                    + " totalNs=" + (System.nanoTime() - started) + " completed=" + completed
                    + " path=" + uri.getPath());
        }
    }

    private static native void initializeNative(int threads);
    private static native String backendNative();
    private static native long readNative(byte[] path, long address, long capacity,
            long[] offsets, long[] lengths, long[] destinations, long taskBytes, int window)
            throws IOException;

    private KvikioHostRangeReader() {
    }
}
