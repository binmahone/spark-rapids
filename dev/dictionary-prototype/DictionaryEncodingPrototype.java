import ai.rapids.cudf.BinaryOp;
import ai.rapids.cudf.BaseDeviceMemoryBuffer;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ContiguousTable;
import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.DeviceMemoryBuffer;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.OrderByArg;
import ai.rapids.cudf.ParquetChunkedReader;
import ai.rapids.cudf.ParquetOptions;
import ai.rapids.cudf.Rmm;
import ai.rapids.cudf.RmmAllocationMode;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import ai.rapids.cudf.nvcomp.BatchedCompressor;
import ai.rapids.cudf.nvcomp.BatchedDecompressor;
import ai.rapids.cudf.nvcomp.BatchedLZ4Compressor;
import ai.rapids.cudf.nvcomp.BatchedLZ4Decompressor;
import ai.rapids.cudf.nvcomp.BatchedZstdCompressor;
import ai.rapids.cudf.nvcomp.BatchedZstdDecompressor;

import java.io.File;
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
  private static final long NVCOMP_CHUNK_SIZE = 64L * 1024;
  private static final long NVCOMP_MAX_INTERMEDIATE_SIZE = 1024L * 1024 * 1024;

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

    private DType indexType() {
      return indices.getType();
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

  private static DType chooseIndexType(long keyCount) {
    if (keyCount <= Byte.MAX_VALUE + 1L) {
      return DType.INT8;
    }
    if (keyCount <= Short.MAX_VALUE + 1L) {
      return DType.INT16;
    }
    return DType.INT32;
  }

  private static Encoded encode(ColumnVector input) {
    try (Table inputTable = new Table(input);
         Table unsortedKeys = inputTable.dropDuplicates(
             new int[] {0}, Table.DuplicateKeepOption.KEEP_ANY, true)) {
      Table sortedKeys = unsortedKeys.orderBy(OrderByArg.asc(0));
      boolean succeeded = false;
      try {
        try (ColumnVector int32Indices =
                 sortedKeys.lowerBound(inputTable, OrderByArg.asc(0))) {
          DType indexType = chooseIndexType(sortedKeys.getRowCount());
          ColumnVector indices = int32Indices.castTo(indexType);
          succeeded = true;
          return new Encoded(sortedKeys, indices);
        }
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
      DType indexType = null;
      for (int i = 0; i < repeats; ++i) {
        Cuda.DEFAULT_STREAM.sync();
        long encodeStart = System.nanoTime();
        try (Encoded encoded = encode(input)) {
          Cuda.DEFAULT_STREAM.sync();
          encodeNs[i] = System.nanoTime() - encodeStart;
          encodedBytes = encoded.deviceBytes();
          keyCount = encoded.keys.getRowCount();
          indexType = encoded.indexType();

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
          "RESULT,%s,%s,%s,%d,%d,%d,%d,%.6f,%.3f,%.3f%n",
          name, input.getType(), indexType, input.getRowCount(), keyCount, rawBytes,
          encodedBytes, ratio, median(encodeNs), median(decodeNs));

      benchmarkCodec(name, input, "lz4", repeats);
      benchmarkCodec(name, input, "zstd", repeats);
    }
  }

  private static DeviceMemoryBuffer compress(
      BatchedCompressor compressor, DeviceMemoryBuffer input) {
    input.incRefCount();
    DeviceMemoryBuffer[] outputs = compressor.compress(
        new BaseDeviceMemoryBuffer[] {input}, Cuda.DEFAULT_STREAM);
    if (outputs.length != 1) {
      for (DeviceMemoryBuffer output : outputs) {
        output.close();
      }
      throw new IllegalStateException("Expected one compressed buffer");
    }
    return outputs[0];
  }

  private static void verifyBuffers(DeviceMemoryBuffer expected, DeviceMemoryBuffer actual) {
    if (expected.getLength() != actual.getLength()) {
      throw new IllegalStateException("Decompressed buffer length mismatch");
    }
    if (expected.getLength() > Integer.MAX_VALUE) {
      throw new IllegalStateException("Prototype verification buffer exceeds Java array limit");
    }
    int size = (int) expected.getLength();
    byte[] expectedBytes = new byte[size];
    byte[] actualBytes = new byte[size];
    try (HostMemoryBuffer expectedHost = HostMemoryBuffer.allocate(size);
         HostMemoryBuffer actualHost = HostMemoryBuffer.allocate(size)) {
      expectedHost.copyFromDeviceBuffer(expected);
      actualHost.copyFromDeviceBuffer(actual);
      expectedHost.getBytes(expectedBytes, 0, 0, size);
      actualHost.getBytes(actualBytes, 0, 0, size);
    }
    if (!Arrays.equals(expectedBytes, actualBytes)) {
      throw new IllegalStateException("Decompressed bytes differ from packed input");
    }
  }

  private static void benchmarkCodec(
      String name, ColumnVector input, String codec, int repeats) {
    BatchedCompressor compressor;
    BatchedDecompressor decompressor;
    if ("lz4".equals(codec)) {
      compressor = new BatchedLZ4Compressor(
          NVCOMP_CHUNK_SIZE, NVCOMP_MAX_INTERMEDIATE_SIZE);
      decompressor = new BatchedLZ4Decompressor(NVCOMP_CHUNK_SIZE);
    } else if ("zstd".equals(codec)) {
      compressor = new BatchedZstdCompressor(
          NVCOMP_CHUNK_SIZE, NVCOMP_MAX_INTERMEDIATE_SIZE);
      decompressor = new BatchedZstdDecompressor(NVCOMP_CHUNK_SIZE);
    } else {
      throw new IllegalArgumentException("Unsupported codec: " + codec);
    }

    try (Table inputTable = new Table(input)) {
      ContiguousTable[] packedTables = inputTable.contiguousSplit();
      if (packedTables.length != 1) {
        for (ContiguousTable packed : packedTables) {
          packed.close();
        }
        throw new IllegalStateException("Expected one packed table");
      }
      try (ContiguousTable packed = packedTables[0]) {
        DeviceMemoryBuffer packedBuffer = packed.getBuffer();
        long packedBytes = packedBuffer.getLength();
        long[] compressNs = new long[repeats];
        long[] decompressNs = new long[repeats];
        long compressedBytes = -1;

        for (int i = -1; i < repeats; ++i) {
          Cuda.DEFAULT_STREAM.sync();
          long compressStart = System.nanoTime();
          try (DeviceMemoryBuffer compressed = compress(compressor, packedBuffer)) {
            Cuda.DEFAULT_STREAM.sync();
            long currentCompressNs = System.nanoTime() - compressStart;
            compressedBytes = compressed.getLength();

            try (DeviceMemoryBuffer output = DeviceMemoryBuffer.allocate(packedBytes)) {
              compressed.incRefCount();
              long decompressStart = System.nanoTime();
              decompressor.decompressAsync(
                  new BaseDeviceMemoryBuffer[] {compressed},
                  new BaseDeviceMemoryBuffer[] {output}, Cuda.DEFAULT_STREAM);
              Cuda.DEFAULT_STREAM.sync();
              long currentDecompressNs = System.nanoTime() - decompressStart;
              verifyBuffers(packedBuffer, output);
              if (i >= 0) {
                compressNs[i] = currentCompressNs;
                decompressNs[i] = currentDecompressNs;
              }
            }
          }
        }

        System.out.printf(Locale.ROOT,
            "CODEC_RESULT,%s,%s,%d,%d,%d,%.6f,%.3f,%.3f%n",
            name, codec, input.getRowCount(), packedBytes, compressedBytes,
            compressedBytes / (double) packedBytes, median(compressNs), median(decompressNs));
      }
    }
  }

  private static ColumnVector readFirstRowGroup(String file, String column) {
    ParquetOptions options = ParquetOptions.builder()
        .includeColumn(column)
        .withRowGroups(new int[] {0})
        .build();
    long chunkLimit = 512L * 1024 * 1024;
    try (ParquetChunkedReader reader =
             new ParquetChunkedReader(chunkLimit, options, new File(file))) {
      if (!reader.hasNext()) {
        throw new IllegalStateException("No Parquet data for " + file + ":" + column);
      }
      try (Table chunk = reader.readChunk()) {
        if (chunk.getNumberOfColumns() != 1) {
          throw new IllegalStateException("Expected one column from " + file + ":" + column);
        }
        return chunk.getColumn(0).incRefCount();
      }
    }
  }

  private static void benchmarkRealTpch(String root, int repeats) {
    String customer = root + "/customer/part.0.parquet";
    String lineitem = root + "/lineitem/part.0.parquet";
    String nation = root + "/nation/part.0.parquet";
    String orders = root + "/orders/part.0.parquet";
    String part = root + "/part/part.0.parquet";

    benchmark("real_customer_c_mktsegment", () ->
        readFirstRowGroup(customer, "c_mktsegment"), repeats);
    benchmark("real_customer_c_nationkey", () ->
        readFirstRowGroup(customer, "c_nationkey"), repeats);
    benchmark("real_customer_c_custkey", () ->
        readFirstRowGroup(customer, "c_custkey"), repeats);
    benchmark("real_orders_o_orderdate", () ->
        readFirstRowGroup(orders, "o_orderdate"), repeats);
    benchmark("real_orders_o_shippriority", () ->
        readFirstRowGroup(orders, "o_shippriority"), repeats);
    benchmark("real_orders_o_orderpriority", () ->
        readFirstRowGroup(orders, "o_orderpriority"), repeats);
    benchmark("real_lineitem_l_orderkey", () ->
        readFirstRowGroup(lineitem, "l_orderkey"), repeats);
    benchmark("real_lineitem_l_shipmode", () ->
        readFirstRowGroup(lineitem, "l_shipmode"), repeats);
    benchmark("real_lineitem_l_returnflag", () ->
        readFirstRowGroup(lineitem, "l_returnflag"), repeats);
    benchmark("real_lineitem_l_linestatus", () ->
        readFirstRowGroup(lineitem, "l_linestatus"), repeats);
    benchmark("real_nation_n_name", () ->
        readFirstRowGroup(nation, "n_name"), repeats);
    benchmark("real_part_p_mfgr", () ->
        readFirstRowGroup(part, "p_mfgr"), repeats);
    benchmark("real_part_p_brand", () ->
        readFirstRowGroup(part, "p_brand"), repeats);
    benchmark("real_part_p_type", () ->
        readFirstRowGroup(part, "p_type"), repeats);
    benchmark("real_part_p_container", () ->
        readFirstRowGroup(part, "p_container"), repeats);
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
      System.out.println("name,type,index_type,rows,key_count,raw_bytes,encoded_bytes,ratio," +
          "encode_median_ms,decode_median_ms");
      System.out.println("codec_name,codec,rows,packed_bytes,compressed_bytes,ratio," +
          "compress_median_ms,decompress_median_ms");
      if (args.length > 2) {
        benchmarkRealTpch(args[2], repeats);
        return;
      }
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
