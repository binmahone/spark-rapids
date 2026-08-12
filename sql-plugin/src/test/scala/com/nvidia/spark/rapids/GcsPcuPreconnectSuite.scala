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

package com.nvidia.spark.rapids

import java.io.IOException
import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf

class GcsPcuPreconnectSuite extends AnyFunSuite {
  test("disabled preconnect does not require a root") {
    GcsPcuPreconnect.run(new SparkConf(false), new Configuration(false), "1")
  }

  test("enabled preconnect requires a root") {
    val conf = new SparkConf(false).set(GcsPcuPreconnect.ENABLED_KEY, "true")
    val error = intercept[IllegalArgumentException] {
      GcsPcuPreconnect.run(conf, new Configuration(), "1")
    }
    assert(error.getMessage.contains(GcsPcuPreconnect.ROOT_KEY))
  }

  test("enabled preconnect writes one executor-scoped object and fails on reuse") {
    val root = Files.createTempDirectory("gcs-pcu-preconnect").toUri.toString
    val hadoopConf = new Configuration()
    val conf = new SparkConf(false)
      .set(GcsPcuPreconnect.ENABLED_KEY, "true")
      .set(GcsPcuPreconnect.ROOT_KEY, root)
      .set(GcsPcuPreconnect.BYTES_KEY, "17")
    val expected = new Path(root, "executor-2/preconnect.bin")

    try {
      GcsPcuPreconnect.run(conf, hadoopConf, "2")
      val fs = expected.getFileSystem(hadoopConf)
      assert(fs.exists(expected))
      assert(fs.getFileStatus(expected).getLen === 17L)
      intercept[IOException] {
        GcsPcuPreconnect.run(conf, hadoopConf, "2")
      }
    } finally {
      expected.getFileSystem(hadoopConf).delete(new Path(root), true)
    }
  }
}
