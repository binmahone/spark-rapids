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
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile.CopyRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.EOFException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;

/** Actual native host-buffer reads with sparse input ranges and reordered output. */
public class DirectRangeReadProbe {
    public static void main(String[] args) throws Exception {
        java.nio.file.Path file = Files.createTempFile("parallel-range-probe-", ".bin");
        try {
            byte[] source = new byte[131071];
            for (int i = 0; i < source.length; i++) {
                source[i] = (byte) (i * 31 + i / 13);
            }
            Files.write(file, source);
            Configuration conf = new Configuration();
            conf.setInt("parquet.read.allocation.size", 4096);
            HadoopInputFile input = HadoopInputFile.create(new Path(file.toUri()), conf);
            System.setProperty("rapids.test.directHostRead", "true");
            System.setProperty("rapids.test.rangeIoTiming", "true");
            for (int lanes : new int[]{1, 2, 4, 8}) {
                System.setProperty("rapids.test.rangeReadConcurrency", Integer.toString(lanes));
                try (HostMemoryBuffer output = HostMemoryBuffer.allocate(30000)) {
                    input.readVectored(output, Arrays.asList(new CopyRange(70000, 12000, 18000),
                            new CopyRange(0, 0, 0), new CopyRange(10000, 18000, 0)));
                    byte[] actual = new byte[30000];
                    output.getBytes(actual, 0, 0, actual.length);
                    byte[] expected = new byte[30000];
                    System.arraycopy(source, 10000, expected, 0, 18000);
                    System.arraycopy(source, 70000, expected, 18000, 12000);
                    if (!Arrays.equals(expected, actual)) {
                        throw new AssertionError("Wrong bytes for concurrency " + lanes);
                    }
                    try {
                        input.readVectored(output, Arrays.asList(new CopyRange(0, 8000, 0),
                                new CopyRange(source.length - 10, 100, 10000)));
                        throw new AssertionError("Expected short-read failure");
                    } catch (EOFException expectedFailure) {
                        // The reader must drain workers before the output buffer can close.
                    }
                    if (lanes > 1) {
                        try {
                            input.readVectored(output, Arrays.asList(new CopyRange(0, 20, 0),
                                    new CopyRange(100, 20, 10)));
                            throw new AssertionError("Expected overlapping-output rejection");
                        } catch (IllegalArgumentException expectedFailure) {
                            // Parallel writers must not share an output interval.
                        }
                    }
                    input.readVectored(output, Collections.emptyList());
                }
            }
            System.out.println("DIRECT_RANGE_READ_PROBE_PASS lanes=1,2,4,8 correctBytes=true eof=true");
        } finally {
            Files.delete(file);
        }
    }
}
