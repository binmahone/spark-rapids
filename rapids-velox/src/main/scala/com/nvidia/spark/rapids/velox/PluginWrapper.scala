/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.velox

import io.glutenproject.{GlutenConfig, GlutenPlugin}

import org.apache.spark.{SPARK_VERSION, SparkContext, TaskFailedReason}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.StaticSQLConf

/**
 * The light-weight wrapper over the GlutenPlugin. We cannot load GlutenPlugin directly because
 * GlutenPlugin will affect the query building of Spark SQL as a SparkPlugin, such as override
 * SparkPlans as what Spark Rapids does. However, we would like to GlutenPlugin to work as an
 * embedded engine, which is encapsulated in certain physical plans, without affecting the query
 * processing.
 *
 * The GlutenPluginWrapper is registered as an extra plugin of spark-rapids, just as
 * AwsStoragePlugin and OptimizerPlugin.
 */
class PluginWrapper extends SparkPlugin {

  private val glutenPlugin = new GlutenPlugin()

  override def driverPlugin(): DriverPlugin = {
    new DriverPluginWrapper(glutenPlugin.driverPlugin())
  }

  override def executorPlugin(): ExecutorPlugin = {
    new ExecutorPluginWrapper(glutenPlugin.executorPlugin())
  }
}

private class DriverPluginWrapper(wrapped: DriverPlugin) extends DriverPlugin with Logging {

  override def init(sc: SparkContext,
                    pluginContext: PluginContext): java.util.Map[String, String] = {
    val conf = pluginContext.conf()
    // NOTE: default value of RapidsConf does NOT affect `pluginContext`
    if (!conf.getBoolean(PluginWrapper.LOAD_BACKEND_KEY, defaultValue = false)) {
      return new java.util.HashMap[String, String]()
    }

    // check if Spark version Gluten built against is correct
    PluginWrapper.checkGlutenJarVersion(SPARK_VERSION)

    // Enable the Gluten to initialize GlutenPlugin and Velox as the Gluten Backend.
    // conf.set(GlutenConfig.GLUTEN_ENABLE_KEY, "true")
    val ret = wrapped.init(sc, pluginContext)
    // Unregister GlutenSessionExtensions by removing it from spark.sql.extensions.
    conf.set(
      StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
      conf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key)
        .split(",")
        .filter(_ != PluginWrapper.GLUTEN_SESSION_EXTENSION_NAME)
        .mkString(",")
    )
    // Disable the Gluten plugin in case it affects the query processing of Spark SQL unexpectedly
    conf.set(GlutenConfig.GLUTEN_ENABLE_KEY, "false")

    // load NativeDeps of rapids-velox
    PluginWrapper.loadNativeDeps()

    ret
  }

  override def registerMetrics(appId: String, pluginContext: PluginContext): Unit = {
    wrapped.registerMetrics(appId, pluginContext)
  }

  override def shutdown(): Unit = {
    wrapped.shutdown()
  }
}

private class ExecutorPluginWrapper(wrapped: ExecutorPlugin) extends ExecutorPlugin with Logging {

  override def init(ctx: PluginContext, extraConf: java.util.Map[String, String]): Unit = {
    val conf = ctx.conf()
    if (conf.getBoolean(PluginWrapper.LOAD_BACKEND_KEY, defaultValue = false)) {
      // check if Spark version Gluten built against is correct
      PluginWrapper.checkGlutenJarVersion(SPARK_VERSION)

      // Firstly, enable the Gluten to initialize GlutenPlugin and Velox as the Gluten Backend.
      // Then, disable it as a plugin in case it affects the query processing unexpectedly.
      // conf.set(GlutenConfig.GLUTEN_ENABLE_KEY, "true")
      wrapped.init(ctx, extraConf)
      conf.set(GlutenConfig.GLUTEN_ENABLE_KEY, "false")

      // load NativeDeps of rapids-velox
      PluginWrapper.loadNativeDeps()

      // Initialize the context of VeloxBackendApis for each executor
      VeloxBackendApis.init(conf)
    }
  }

  override def shutdown(): Unit = {
    wrapped.shutdown()
  }

  override def onTaskStart(): Unit = {
    wrapped.onTaskStart()
  }

  override def onTaskSucceeded(): Unit = {
    wrapped.onTaskSucceeded()
  }

  override def onTaskFailed(failureReason: TaskFailedReason): Unit = {
    wrapped.onTaskFailed(failureReason)
  }
}

object PluginWrapper {
  private[velox] val GLUTEN_SESSION_EXTENSION_NAME = "io.glutenproject.GlutenSessionExtensions"
  private[velox] val LOAD_BACKEND_KEY = "spark.rapids.sql.hybrid.loadBackend"

  // Leaves belows emptys method to maximize the alignment to the main branch
  private[velox] def loadNativeDeps(): Unit = {}

  /**
   * Throws exception if Gluten jar version is not the same as Spark version
   */
  private[velox] def checkGlutenJarVersion(sparkVersion: String): Unit = {}
}
