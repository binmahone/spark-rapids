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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Checks real native-host bytes, unaligned slices, EOF draining, bounds, and reuse after failure. */
public final class KvikioRangeReadProbe {
    public static void main(String[] args) throws Exception {
        final int mib = 1024 * 1024;
        java.nio.file.Path file = Files.createTempFile("kvikio-range-probe-", ".bin");
        try {
            byte[] source = new byte[12 * mib + 103];
            for (int i = 0; i < source.length; i++) {
                source[i] = (byte) (i * 31 + i / 13);
            }
            Files.write(file, source);
            HadoopInputFile input = HadoopInputFile.create(new Path(file.toUri()),
                    new Configuration());
            List<CopyRange> ranges = Arrays.asList(new CopyRange(6 * mib, 5 * mib, 5 * mib + 1024),
                    new CopyRange(mib / 2 + 1, 5 * mib, 0));
            byte[] expected = new byte[10 * mib + 4096];
            Arrays.fill(expected, (byte) 0x5a);
            System.arraycopy(source, 6 * mib, expected, 5 * mib + 1024, 5 * mib);
            System.arraycopy(source, mib / 2 + 1, expected, 0, 5 * mib);
            try (HostMemoryBuffer output = HostMemoryBuffer.allocate(expected.length)) {
                for (int attempt = 0; attempt < 3; attempt++) {
                    byte[] initial = new byte[expected.length];
                    Arrays.fill(initial, (byte) 0x5a);
                    output.setBytes(0, initial, 0, initial.length);
                    input.readVectored(output, ranges);
                    byte[] actual = new byte[expected.length];
                    output.getBytes(actual, 0, 0, actual.length);
                    if (!Arrays.equals(expected, actual)) {
                        throw new AssertionError("Wrong bytes or overwritten destination guard");
                    }
                    input.readVectored(output, Collections.emptyList());
                    try {
                        input.readVectored(output, Arrays.asList(new CopyRange(0, mib, 0),
                                new CopyRange(source.length - 10, 100, 2 * mib)));
                        throw new AssertionError("Expected EOF after a valid submitted range");
                    } catch (EOFException expectedFailure) {
                        // A subsequent iteration must still read correctly after all work drains.
                    }
                }
                try {
                    input.readVectored(output, Collections.singletonList(
                            new CopyRange(0, 100, output.getLength() - 1)));
                    throw new AssertionError("Expected destination-bound rejection");
                } catch (IllegalArgumentException expectedFailure) {
                    // Native IO must not write outside the supplied buffer.
                }
                int window = Integer.getInteger(Boolean.getBoolean("rapids.test.kvikioHostRead")
                        ? "rapids.test.kvikioRangeWindow" : "rapids.test.rangeReadConcurrency", 1);
                if (window > 1) {
                    try {
                        input.readVectored(output, Arrays.asList(new CopyRange(0, 100, 0),
                                new CopyRange(100, 100, 50)));
                        throw new AssertionError("Expected overlapping-output rejection");
                    } catch (IllegalArgumentException expectedFailure) {
                        // Concurrent requests may not overwrite one another.
                    }
                }
                HadoopInputFile missing = HadoopInputFile.create(
                        new Path(file.toString() + ".missing"), new Configuration());
                try {
                    missing.readVectored(output, Collections.singletonList(new CopyRange(0, 32, 0)));
                    throw new AssertionError("Expected missing-file failure");
                } catch (IOException expectedFailure) {
                    // An unavailable native source must not silently switch readers.
                }
            }
            System.out.println("KVIKIO_RANGE_READ_PROBE_PASS bytes=true guards=true eof=true reuse=true");
        } finally {
            Files.delete(file);
        }
    }
}
