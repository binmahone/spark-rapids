# Kudo Serialization Format: Design Philosophy and Technical Details

## Background: Spark Shuffle on GPU

In the NVIDIA spark-rapids plugin, when a GPU shuffle exchange occurs, the map side needs to:

1. Partition a columnar batch by hash (or other partitioning scheme) using cudf's `table.partition()`, producing a single reordered table plus an array of partition boundary indices.
2. Slice this table into per-partition pieces.
3. Serialize each piece into a byte stream and hand it to Spark's shuffle writer, which may apply CPU compression (e.g., lz4) and write it to disk or transfer it over the network.

On the reduce side, Spark's shuffle reader retrieves the serialized bytes for each reduce partition, decompresses them, and passes them to the deserializer. The deserialized results are typically many small batches (one per map-side partition slice). Since GPU vectorized execution performs best with large batches, a coalesce step (`GpuShuffleCoalesceExec`) always follows the shuffle read, concatenating these small batches into larger ones before placing them on the GPU.

## The Problem with JCudfSerialization

The original serializer, `ai.rapids.cudf.JCudfSerialization`, treats each serialized slice as a self-contained unit. When serializing a row range `[start, end)` from the original table, it performs the following computations:

### Validity Buffer

The null bitmask in cudf is bit-packed (8 rows per byte). If the slice starts at row 3, the validity bits for that slice start at bit 3 of byte 0. JCudfSerialization shifts the bits so the output validity buffer is aligned to bit 0 -- i.e., it computes an exact, self-contained validity buffer for the slice. This involves bit-level shifting across all validity bytes for every column.

### Offset Buffer

For variable-width columns (e.g., strings), cudf stores an offset array where `offset[i]` is the byte position in the data buffer where row `i`'s data begins. If we are slicing rows `[3, 9)`, the raw offsets might be `[100, 115, 130, 180, 195, 210, 250]`. JCudfSerialization normalizes these by subtracting the base offset (100), producing `[0, 15, 30, 80, 95, 110, 150]`, so the serialized slice's data buffer can start from byte 0. This requires iterating over all offset values for every variable-width column.

### Header

The header is self-descriptive -- it embeds the full schema information (data type, null count, data size, validity size, offset size for every column). This makes each serialized blob independently deserializable without any external context, but increases header size proportionally to the number of columns.

### Cost

These per-slice computations (bit shifting, offset normalization) happen on the CPU and add measurable overhead, especially when the number of partitions is large (e.g., 200 partitions means 200 slices per input batch).

## Kudo's Key Observations

The Kudo format is designed around two observations:

1. **The format does not need to be self-descriptive.** In Spark shuffle, the reduce side already knows the schema of the data (it is determined at query planning time and propagated through the shuffle dependency). There is no need to embed schema information in every serialized slice. By removing schema from the header and requiring it to be provided externally, the header shrinks to a small fixed size (a few integers plus a compact bitset indicating which columns have non-null validity buffers).

2. **Write-time normalization is wasted work when read-time concatenation is inevitable.** Since the reduce side almost always concatenates many small slices into a larger batch (via `GpuShuffleCoalesceExec`), the concatenation logic must already handle merging validity buffers and recomputing offset buffers across multiple inputs. If the concatenation step already does this work, then doing it also at write time is redundant. By deferring these computations to the read-side merge, the write path becomes almost a pure memory copy (`memcpy`).

## How Kudo Implements This

### Write Path (Serialization)

- **Validity buffer**: Instead of bit-shifting the validity bits to align the slice to bit 0, Kudo simply copies the relevant bytes as-is. For a slice starting at row 3, it copies the bytes that contain bits `[3, end)`, without any shifting. The row offset (3) is recorded in the header, so the reader knows that valid data starts at bit 3.

- **Offset buffer**: Instead of subtracting the base offset value, Kudo copies the raw offset values directly. For the example above, it writes `[100, 115, 130, 180, 195, 210, 250]` as-is. The reader, knowing the row offset and the structure, can adjust these during the merge phase.

- **Data buffer**: Copied directly, same as JCudfSerialization.

- The result is that serialization becomes essentially a sequence of `memcpy` operations with minimal computation, significantly reducing write-side CPU cost.

### Header Format

The Kudo header contains only:

| Field | Size | Description |
|---|---|---|
| Magic number | 4 bytes | ASCII "KUD0" |
| Row offset | 4 bytes | Starting row in original table |
| Number of rows | 4 bytes | Row count for this slice |
| Validity buffer length | 4 bytes | Total validity section size |
| Offset buffer length | 4 bytes | Total offset section size |
| Total body length | 4 bytes | Total body size |
| Number of columns | 4 bytes | Flattened column count |
| hasValidityBuffer | (numCols+7)/8 bytes | Bitset: which columns have validity |

No per-column type or size information is stored.

### Body Layout

Unlike JCudfSerialization which interleaves buffers per-column (`col0-validity, col0-data, col1-validity, col1-data, ...`), Kudo groups buffers by type:

1. All validity buffers (4-byte aligned)
2. All offset buffers (inherently 4-byte aligned)
3. All data buffers (4-byte aligned)

This grouping may improve memory access patterns during the read-side merge, as the merge logic processes all validity buffers together, then all offset buffers, then all data buffers.

### Read Path (Deserialization / Merge)

On the reduce side, multiple Kudo-serialized slices for the same partition are merged. During this merge, the reader:

- Combines validity buffers from multiple slices, handling the bit offset recorded in each header to correctly align the bits.
- Recomputes offset buffers by adjusting raw offset values from each slice, accounting for the cumulative data size from previously merged slices.
- Concatenates data buffers.

This work is essentially the same as what the coalesce step would have done anyway -- the difference is that with JCudfSerialization, the normalize-then-merge pattern does the normalization work twice (once at write, once conceptually at merge), whereas Kudo does it only once at merge time.

## Tradeoffs

| Aspect | JCudfSerialization | Kudo |
|---|---|---|
| Write performance | Per-element computation (bit shift, offset subtract) | Near-`memcpy` speed |
| Read performance | Slices are self-contained, simpler merge | Slightly more work per slice during merge (bit alignment, offset adjustment), but amortized by batch-level concatenation |
| Header size | Large (proportional to column count, embeds full schema) | Small and fixed (no schema) |
| Schema dependency | None (self-descriptive) | Requires schema provided externally |
| Use case | General-purpose columnar serialization | Optimized specifically for Spark shuffle where schema is known and read-side coalesce is guaranteed |
