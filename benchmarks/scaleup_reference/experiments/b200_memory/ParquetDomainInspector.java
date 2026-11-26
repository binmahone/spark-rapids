# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

package com.nvidia.spark.rapids.benchmark;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

/** Read all footer fields within the JVM, without per-row-group gateway round trips. */
public final class ParquetDomainInspector {
  private ParquetDomainInspector() {}

  public static String inspect(String folder, List<String> columns) throws IOException {
    File[] files = new File(folder).listFiles((dir, name) -> name.endsWith(".parquet"));
    if (files == null || files.length == 0) {
      throw new IllegalArgumentException("No Parquet files for domain proof");
    }
    Arrays.sort(files);
    Map<String, Map<String, Object>> bounds = new LinkedHashMap<>();
    for (String column : columns) {
      Map<String, Object> value = new LinkedHashMap<>();
      value.put("minimum", null);
      value.put("maximum", null);
      value.put("row_groups", 0L);
      bounds.put(column, value);
    }
    List<Map<String, Object>> inventory = new ArrayList<>();
    Configuration conf = new Configuration();
    for (File file : files) {
      long size = file.length();
      long modified = Files.getLastModifiedTime(file.toPath()).to(TimeUnit.NANOSECONDS);
      ParquetMetadata footer = ParquetFileReader.readFooter(conf, new Path(file.toURI()));
      for (BlockMetaData block : footer.getBlocks()) {
        Set<String> found = new HashSet<>();
        for (ColumnChunkMetaData chunk : block.getColumns()) {
          String name = chunk.getPath().toDotString();
          if (!bounds.containsKey(name)) {
            continue;
          }
          found.add(name);
          if (chunk.getPrimitiveType().getPrimitiveTypeName() != PrimitiveTypeName.INT64) {
            throw new IllegalArgumentException("Requires physical INT64: " + name);
          }
          Statistics<?> statistics = chunk.getStatistics();
          Map<String, Object> value = bounds.get(name);
          value.put("row_groups", (Long) value.get("row_groups") + 1);
          if (statistics.isNumNullsSet() && statistics.getNumNulls() == block.getRowCount()) {
            continue;
          }
          if (!statistics.hasNonNullValue()) {
            throw new IllegalArgumentException("Missing integer bounds: " + file + ":" + name);
          }
          long low = ((Number) statistics.genericGetMin()).longValue();
          long high = ((Number) statistics.genericGetMax()).longValue();
          if (low < Integer.MIN_VALUE || high > Integer.MAX_VALUE || low > high) {
            throw new IllegalArgumentException("Outside INT32: " + name + ":" + low + ":" + high);
          }
          Long previousLow = (Long) value.get("minimum");
          Long previousHigh = (Long) value.get("maximum");
          value.put("minimum", previousLow == null ? low : Math.min(previousLow, low));
          value.put("maximum", previousHigh == null ? high : Math.max(previousHigh, high));
        }
        if (!found.equals(bounds.keySet())) {
          throw new IllegalArgumentException("Missing columns in row-group metadata: " + file);
        }
      }
      if (size != file.length() || modified !=
          Files.getLastModifiedTime(file.toPath()).to(TimeUnit.NANOSECONDS)) {
        throw new IllegalArgumentException("File changed during domain inspection: " + file);
      }
      Map<String, Object> info = new LinkedHashMap<>();
      info.put("path", file.getAbsolutePath());
      info.put("size", size);
      info.put("mtime_ns", modified);
      inventory.add(info);
    }
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("columns", bounds);
    result.put("files", inventory);
    return new ObjectMapper().writeValueAsString(result);
  }
}
