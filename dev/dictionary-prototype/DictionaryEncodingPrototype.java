import ai.rapids.cudf.BinaryOp;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.OrderByArg;
import ai.rapids.cudf.Rmm;
import ai.rapids.cudf.RmmAllocationMode;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;

import java.util.Arrays;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * Measures a physical keys-plus-indices representation using only public cudf-java APIs.
 * This is an economic feasibility prototype, not a DICTIONARY32 implementation.
 */
public final class DictionaryEncodingPrototype {
  private static final int DEFAULT_ROWS = 10_000_000;
  private static final int DEFAULT_REPEATS = 3;

  private static final class Encoded implements AutoCloseable {
    private final Table keys;
    private final ColumnVector indices;

    private Encoded(Table keys, ColumnVector indices) {
      this.keys = keys;
      this.indices = indices;
    }

    private long deviceBytes() {
      return keys.getColumn(0).getDeviceMemorySize() + indices.getDeviceMemorySize();
    }

    private Table decode() {
      return keys.gather(indices);
    }

    @Override
    public void close() {
      indices.close();
      keys.close();
    }
  }

  private static Encoded encode(ColumnVector input) {
    try (Table inputTable = new Table(input);
         Table unsortedKeys = inputTable.dropDuplicates(
             new int[] {0}, Table.DuplicateKeepOption.KEEP_ANY, true)) {
      Table sortedKeys = unsortedKeys.orderBy(OrderByArg.asc(0));
      boolean succeeded = false;
      try {
        ColumnVector indices = sortedKeys.lowerBound(inputTable, OrderByArg.asc(0));
        succeeded = true;
        return new Encoded(sortedKeys, indices);
      } finally {
        if (!succeeded) {
          sortedKeys.close();
        }
      }
    }
  }

  private static void verify(ColumnVector input, Table decoded) {
    if (decoded.getRowCount() != input.getRowCount()) {
      throw new IllegalStateException("Decoded row count mismatch");
    }
    try (ColumnVector equal = input.binaryOp(
             BinaryOp.EQUAL, decoded.getColumn(0), DType.BOOL8);
         Scalar allEqual = equal.all()) {
      if (!allEqual.isValid() || !allEqual.getBoolean()) {
        throw new IllegalStateException("Decoded values differ from input");
      }
    }
  }

  private static double median(long[] values) {
    long[] copy = values.clone();
    Arrays.sort(copy);
    int middle = copy.length / 2;
    if ((copy.length & 1) == 1) {
      return copy[middle] / 1_000_000.0;
    }
    return (copy[middle - 1] + copy[middle]) / 2_000_000.0;
  }

  private static void benchmark(
      String name, Supplier<ColumnVector> factory, int repeats) {
    try (ColumnVector input = factory.get()) {
      long rawBytes = input.getDeviceMemorySize();

      // Warm up all operators and native library paths once.
      try (Encoded warm = encode(input); Table decoded = warm.decode()) {
        Cuda.DEFAULT_STREAM.sync();
        verify(input, decoded);
      }
      Cuda.DEFAULT_STREAM.sync();

      long[] encodeNs = new long[repeats];
      long[] decodeNs = new long[repeats];
      long encodedBytes = -1;
      long keyCount = -1;
      for (int i = 0; i < repeats; ++i) {
        Cuda.DEFAULT_STREAM.sync();
        long encodeStart = System.nanoTime();
        try (Encoded encoded = encode(input)) {
          Cuda.DEFAULT_STREAM.sync();
          encodeNs[i] = System.nanoTime() - encodeStart;
          encodedBytes = encoded.deviceBytes();
          keyCount = encoded.keys.getRowCount();

          long decodeStart = System.nanoTime();
          try (Table decoded = encoded.decode()) {
            Cuda.DEFAULT_STREAM.sync();
            decodeNs[i] = System.nanoTime() - decodeStart;
            verify(input, decoded);
          }
        }
      }

      double ratio = encodedBytes / (double) rawBytes;
      System.out.printf(Locale.ROOT,
          "RESULT,%s,%s,%d,%d,%d,%d,%.6f,%.3f,%.3f%n",
          name, input.getType(), input.getRowCount(), keyCount, rawBytes, encodedBytes,
          ratio, median(encodeNs), median(decodeNs));
    }
  }

  private static ColumnVector sequenceInt32(int rows) {
    try (Scalar start = Scalar.fromInt(0); Scalar step = Scalar.fromInt(1)) {
      return ColumnVector.sequence(start, step, rows);
    }
  }

  private static ColumnVector sequenceInt64(int rows) {
    try (Scalar start = Scalar.fromLong(0); Scalar step = Scalar.fromLong(1)) {
      return ColumnVector.sequence(start, step, rows);
    }
  }

  private static ColumnVector moduloInt32(int rows, int cardinality) {
    try (ColumnVector sequence = sequenceInt32(rows);
         Scalar divisor = Scalar.fromInt(cardinality)) {
      return sequence.binaryOp(BinaryOp.MOD, divisor, DType.INT32);
    }
  }

  private static ColumnVector repeatedInt64(int rows, long repetitions) {
    try (ColumnVector sequence = sequenceInt64(rows);
         Scalar divisor = Scalar.fromLong(repetitions)) {
      return sequence.binaryOp(BinaryOp.DIV, divisor, DType.INT64);
    }
  }

  private static ColumnVector repeatedStrings(int rows, String[] keys) {
    try (ColumnVector keyColumn = ColumnVector.fromStrings(keys);
         Table keyTable = new Table(keyColumn);
         ColumnVector indices = moduloInt32(rows, keys.length);
         Table gathered = keyTable.gather(indices)) {
      return gathered.getColumn(0).incRefCount();
    }
  }

  private static String[] partTypes(int cardinality) {
    String[] values = new String[cardinality];
    for (int i = 0; i < cardinality; ++i) {
      values[i] = String.format(Locale.ROOT, "TYPE-%03d-STANDARD-POLISHED", i);
    }
    return values;
  }

  public static void main(String[] args) {
    int rows = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_ROWS;
    int repeats = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_REPEATS;
    if (rows <= 0 || repeats <= 0) {
      throw new IllegalArgumentException("rows and repeats must be positive");
    }

    Cuda.setDevice(0);
    Rmm.initialize(RmmAllocationMode.CUDA_ASYNC, null, 64L * 1024 * 1024 * 1024);
    try {
      System.out.println("name,type,rows,key_count,raw_bytes,encoded_bytes,ratio," +
          "encode_median_ms,decode_median_ms");
      benchmark("shippriority_constant_int32", () -> {
        try (Scalar zero = Scalar.fromInt(0)) {
          return ColumnVector.fromScalar(zero, rows);
        }
      }, repeats);
      benchmark("orderdate_2400_int32", () -> moduloInt32(rows, 2400), repeats);
      benchmark("orderkey_repeat4_int64", () -> repeatedInt64(rows, 4), repeats);
      benchmark("unique_int64", () -> sequenceInt64(rows), repeats);
      benchmark("marketsegment_5_string", () -> repeatedStrings(rows, new String[] {
          "AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"
      }), repeats);
      benchmark("nation_25_string", () -> repeatedStrings(rows, new String[] {
          "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA",
          "FRANCE", "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN",
          "JORDAN", "KENYA", "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA",
          "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES"
      }), repeats);
      benchmark("parttype_150_string", () -> repeatedStrings(rows, partTypes(150)), repeats);
    } finally {
      Rmm.shutdown();
    }
  }
}
