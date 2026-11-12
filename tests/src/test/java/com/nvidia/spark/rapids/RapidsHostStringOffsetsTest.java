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
package com.nvidia.spark.rapids;

import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVector;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RapidsHostStringOffsetsTest {
  private static final int CHUNK = 1 << 20;
  private static final HostColumnVector.DataType STRING =
      new HostColumnVector.BasicType(true, DType.STRING);

  @Test void promotionPreservesValuesAndRollbackBeyondInt32() {
    byte[] value = new byte[CHUNK];
    Arrays.fill(value, (byte) 'Z');
    try (RapidsHostColumnBuilder builder = new RapidsHostColumnBuilder(STRING, 1)) {
      for (int row = 0; row < 2047; row++) builder.appendUTF8String(value);
      builder.appendUTF8String(value, 0, CHUNK - 1);
      try (HostColumnVector host = builder.build()) {
        assertEquals(DType.INT32, host.getOffsetsType());
        assertEquals(Integer.MAX_VALUE, host.getEndListOffset(2047));
      }
      builder.appendUTF8String(value, 0, 1);
      builder.appendNull();
      RapidsHostColumnBuilder.BuilderSnapshot snapshot = builder.captureState();
      builder.appendNull();
      builder.appendUTF8String(value);
      builder.restoreState(snapshot);
      builder.appendUTF8String(new byte[]{'a', 'b'});
      try (HostColumnVector host = builder.build()) {
        assertEquals(DType.INT64, host.getOffsetsType());
        assertEquals(2051, host.getRowCount());
        assertEquals(1, host.getNullCount());
        assertTrue(host.isNull(2049));
        assertFalse(host.isNull(2050));
        assertEquals(1L << 31, host.getStartListOffset(2050));
        assertEquals((1L << 31) + 2, host.getEndListOffset(2050));
        assertArrayEquals(new byte[]{'a', 'b'}, host.getUTF8(2050));
        assertArrayEquals(value, host.getUTF8(0));
      }
    }
  }

  @Test void smallWorkloadRetainsInt32() {
    try (RapidsHostColumnBuilder builder = new RapidsHostColumnBuilder(STRING, 1)) {
      builder.append("abc");
      builder.appendNull();
      builder.append("def");
      try (HostColumnVector host = builder.build()) {
        assertEquals(DType.INT32, host.getOffsetsType());
        assertEquals("def", host.getJavaString(2));
        assertTrue(host.isNull(1));
      }
    }
  }

  @Test void explicitTestLimitStillRejectsBeforeWrite() {
    RapidsHostColumnBuilder.withTestLimits(new RapidsHostColumnBuilder.Limits(4, 10, 10, 10),
        () -> {
          try (RapidsHostColumnBuilder builder = new RapidsHostColumnBuilder(STRING, 1)) {
            builder.append("abcd");
            assertThrows(ColumnLimitExceededException.class, () -> builder.append("e"));
            try (HostColumnVector host = builder.build()) {
              assertEquals(1, host.getRowCount());
              assertEquals("abcd", host.getJavaString(0));
              assertEquals(DType.INT32, host.getOffsetsType());
            }
          }
          return null;
        });
  }
}
