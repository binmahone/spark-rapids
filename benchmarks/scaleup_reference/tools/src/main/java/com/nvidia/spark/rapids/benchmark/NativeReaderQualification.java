/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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
package com.nvidia.spark.rapids.benchmark;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.DeviceMemoryBuffer;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.Rmm;
import com.nvidia.spark.rapids.jni.NativeFileSource;

/** Validates the native reader against a complete CPU byte reference. */
public final class NativeReaderQualification {
  public static void main(String[] args) throws Exception {
    boolean deviceMode = args[0].equals("gds");
    if (!deviceMode && !args[0].equals("host")) throw new IllegalArgumentException("Unknown mode");
    if (deviceMode && (!Files.isReadable(Paths.get("/run/udev")) ||
        !"false".equals(System.getenv("CUFILE_ALLOW_COMPAT_MODE")))) {
      throw new IllegalStateException("GDS requires readable udev and strict cuFile");
    }
    NativeFileSource.setUpKvikio(deviceMode ? NativeFileSource.CompatMode.AUTO :
        NativeFileSource.CompatMode.ON, 32);
    Cuda.setDevice(0);
    int bytes = 16 * 1024 * 1024;
    byte[] expected = new byte[bytes];
    try (FileChannel reference = FileChannel.open(Paths.get(args[1]), StandardOpenOption.READ)) {
      ByteBuffer destination = ByteBuffer.wrap(expected);
      while (destination.hasRemaining()) {
        if (reference.read(destination) <= 0) throw new IllegalStateException("Incomplete reference");
      }
    }
    Rmm.initialize(0, null, 0);
    try (NativeFileSource source = NativeFileSource.open(args[1]);
         HostMemoryBuffer host = HostMemoryBuffer.allocate(bytes)) {
      if (deviceMode) {
        if (!source.isDeviceReadDirect(bytes)) throw new IllegalStateException("No direct device path");
        try (DeviceMemoryBuffer device = DeviceMemoryBuffer.allocate(bytes)) {
          source.readToDevice(new long[]{0}, new long[]{bytes}, device, new long[]{0},
              Cuda.DEFAULT_STREAM);
          Cuda.deviceSynchronize();
          host.copyFromDeviceBuffer(device);
        }
      } else {
        source.readToHost(new long[]{0, bytes / 2}, new long[]{bytes / 2, bytes / 2},
            host, new long[]{0, bytes / 2});
      }
      byte[] actual = new byte[bytes];
      host.getBytes(actual, 0, 0, bytes);
      if (!Arrays.equals(expected, actual)) throw new IllegalStateException("Native bytes differ");
      Files.write(Paths.get(args[2]), Files.readAllBytes(Paths.get("/proc/self/maps")));
      System.out.println("NATIVE_READER_BYTES_VALIDATED mode=" + args[0] + " bytes=" + bytes);
    } finally {
      Rmm.shutdown();
    }
  }

  private NativeReaderQualification() {}
}
