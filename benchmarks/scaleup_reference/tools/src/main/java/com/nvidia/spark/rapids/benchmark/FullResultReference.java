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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import org.apache.spark.sql.Row;

/** Complete Q11 key/value comparison against a separately retained baseline execution. */
public final class FullResultReference {
  private static final int MAGIC = 0x51313101;

  private static String hex(byte[] bytes) {
    StringBuilder result = new StringBuilder();
    for (byte value : bytes) result.append(String.format("%02x", value & 255));
    return result.toString();
  }

  public static String write(Row[] rows, String filename) throws Exception {
    Path target = Paths.get(filename);
    if (Files.exists(target)) throw new IOException("Reference already exists");
    Path temporary = target.resolveSibling(target.getFileName() + ".staging");
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(
        new DigestOutputStream(Files.newOutputStream(temporary), digest), 4 * 1024 * 1024))) {
      output.writeInt(MAGIC);
      output.writeLong(rows.length);
      double previous = Double.POSITIVE_INFINITY;
      for (Row row : rows) {
        if (row.isNullAt(0) || row.isNullAt(1)) throw new IOException("Unexpected NULL");
        long key = ((Number) row.get(0)).longValue();
        double value = ((Number) row.get(1)).doubleValue();
        if (key <= 0 || !Double.isFinite(value) || value > previous) {
          throw new IOException("Invalid Q11 key, value or descending order");
        }
        output.writeLong(key);
        output.writeDouble(value);
        previous = value;
      }
    }
    Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
    return hex(digest.digest());
  }

  private static int slot(long key, int mask) {
    long value = key * 0x9e3779b97f4a7c15L;
    return (int) (value ^ (value >>> 32)) & mask;
  }

  public static String compare(Row[] rows, String filename, String expectedHash) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    long[] keys;
    double[] values;
    long count;
    try (DataInputStream input = new DataInputStream(new BufferedInputStream(
        new DigestInputStream(Files.newInputStream(Paths.get(filename)), digest), 4 * 1024 * 1024))) {
      if (input.readInt() != MAGIC) throw new IOException("Unknown reference format");
      count = input.readLong();
      if (count != rows.length || count < 0 || count > (1 << 28)) {
        throw new IOException("Complete row count differs or exceeds supported bounds");
      }
      int capacity = 8;
      while (capacity < count * 2) capacity *= 2;
      keys = new long[capacity];
      values = new double[capacity];
      int mask = capacity - 1;
      for (long index = 0; index < count; index++) {
        long key = input.readLong();
        double value = input.readDouble();
        if (key <= 0 || !Double.isFinite(value)) throw new IOException("Invalid reference row");
        int position = slot(key, mask);
        while (keys[position] != 0 && keys[position] != key) position = (position + 1) & mask;
        if (keys[position] == key) throw new IOException("Duplicate reference key");
        keys[position] = key;
        values[position] = value;
      }
      if (input.read() != -1) throw new IOException("Unexpected trailing reference bytes");
    }
    String actualHash = hex(digest.digest());
    if (!actualHash.equals(expectedHash)) throw new IOException("Reference SHA-256 differs");
    long matched = 0;
    long mismatches = 0;
    double previous = Double.POSITIVE_INFINITY;
    int mask = keys.length - 1;
    for (Row row : rows) {
      if (row.isNullAt(0) || row.isNullAt(1)) throw new IOException("Unexpected NULL");
      long key = ((Number) row.get(0)).longValue();
      double value = ((Number) row.get(1)).doubleValue();
      if (key <= 0 || !Double.isFinite(value) || value > previous) {
        throw new IOException("Invalid Q11 key, value or descending order");
      }
      previous = value;
      int position = slot(key, mask);
      while (keys[position] != 0 && keys[position] != key) position = (position + 1) & mask;
      if (keys[position] != key) {
        mismatches++;
        continue;
      }
      double reference = values[position];
      double tolerance = Math.max(0.01, 1e-8 * Math.max(Math.abs(value), Math.abs(reference)));
      if (Math.abs(value - reference) > tolerance) mismatches++;
      keys[position] = -1;
      matched++;
    }
    if (matched != count || mismatches != 0) {
      throw new IOException("Complete Q11 mismatch: matched=" + matched + ", expected="
          + count + ", mismatches=" + mismatches);
    }
    return "{\"status\":\"passed\",\"rows\":" + count + ",\"matched_keys\":" + matched
        + ",\"mismatches\":0,\"reference_sha256\":\"" + actualHash + "\"}";
  }

  private FullResultReference() {}
}
