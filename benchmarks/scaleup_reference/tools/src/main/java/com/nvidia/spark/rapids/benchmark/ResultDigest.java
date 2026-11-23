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

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.Decimal;

/** Computes ordered and order-independent digests without copying rows through Py4J. */
public final class ResultDigest {
  private static final int SHA256_BYTES = 32;
  private static final MathContext NORMALIZED_DECIMAL_CONTEXT =
      new MathContext(12, RoundingMode.HALF_EVEN);

  private ResultDigest() {
  }

  public static String[] compute(Row[] rows) {
    try {
      MessageDigest ordered = MessageDigest.getInstance("SHA-256");
      MessageDigest rowDigest = MessageDigest.getInstance("SHA-256");
      byte[] sum = new byte[SHA256_BYTES];
      byte[] xor = new byte[SHA256_BYTES];
      MessageDigest normalizedOrdered = MessageDigest.getInstance("SHA-256");
      byte[] normalizedSum = new byte[SHA256_BYTES];
      byte[] normalizedXor = new byte[SHA256_BYTES];
      byte[] length = new byte[Integer.BYTES];
      byte[] rowHash = new byte[SHA256_BYTES];
      for (Row row : rows) {
        byte[] bytes = String.valueOf(row).getBytes(StandardCharsets.UTF_8);
        encodeLength(bytes.length, length);
        ordered.update(length);
        ordered.update(bytes);
        rowDigest.reset();
        rowDigest.update(length);
        rowDigest.update(bytes);
        rowDigest.digest(rowHash, 0, rowHash.length);
        addModulo256(sum, rowHash);
        for (int index = 0; index < SHA256_BYTES; index++) {
          xor[index] ^= rowHash[index];
        }

        byte[] normalizedBytes = renderNormalized(row).getBytes(StandardCharsets.UTF_8);
        encodeLength(normalizedBytes.length, length);
        normalizedOrdered.update(length);
        normalizedOrdered.update(normalizedBytes);
        rowDigest.reset();
        rowDigest.update(length);
        rowDigest.update(normalizedBytes);
        rowDigest.digest(rowHash, 0, rowHash.length);
        addModulo256(normalizedSum, rowHash);
        for (int index = 0; index < SHA256_BYTES; index++) {
          normalizedXor[index] ^= rowHash[index];
        }
      }
      return new String[] {
          Integer.toString(rows.length),
          toHex(ordered.digest()),
          toHex(sum),
          toHex(xor),
          toHex(normalizedOrdered.digest()),
          toHex(normalizedSum),
          toHex(normalizedXor)
      };
    } catch (NoSuchAlgorithmException | DigestException exception) {
      throw new IllegalStateException("SHA-256 is unavailable", exception);
    }
  }

  private static String renderNormalized(Row row) {
    StringBuilder builder = new StringBuilder();
    builder.append('[');
    for (int index = 0; index < row.size(); index++) {
      if (index > 0) {
        builder.append(',');
      }
      builder.append(normalizeValue(row.get(index)));
    }
    builder.append(']');
    return builder.toString();
  }

  private static String normalizeValue(Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof BigDecimal) {
      return normalizeDecimal((BigDecimal) value);
    }
    if (value instanceof Decimal) {
      return normalizeDecimal(((Decimal) value).toJavaBigDecimal());
    }
    if (value instanceof Double) {
      double number = (Double) value;
      return Double.isFinite(number)
          ? normalizeDecimal(BigDecimal.valueOf(number))
          : Double.toString(number);
    }
    if (value instanceof Float) {
      float number = (Float) value;
      return Float.isFinite(number)
          ? normalizeDecimal(new BigDecimal(Float.toString(number)))
          : Float.toString(number);
    }
    return String.valueOf(value);
  }

  private static String normalizeDecimal(BigDecimal value) {
    BigDecimal normalized = value.round(NORMALIZED_DECIMAL_CONTEXT).stripTrailingZeros();
    return normalized.signum() == 0 ? "0" : normalized.toEngineeringString();
  }

  private static void encodeLength(int value, byte[] output) {
    output[0] = (byte) (value >>> 24);
    output[1] = (byte) (value >>> 16);
    output[2] = (byte) (value >>> 8);
    output[3] = (byte) value;
  }

  private static void addModulo256(byte[] accumulator, byte[] value) {
    int carry = 0;
    for (int index = SHA256_BYTES - 1; index >= 0; index--) {
      int total = (accumulator[index] & 0xff) + (value[index] & 0xff) + carry;
      accumulator[index] = (byte) total;
      carry = total >>> 8;
    }
  }

  private static String toHex(byte[] bytes) {
    StringBuilder builder = new StringBuilder(bytes.length * 2);
    for (byte value : bytes) {
      builder.append(String.format("%02x", value & 0xff));
    }
    return builder.toString();
  }
}
