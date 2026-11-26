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

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import scala.Function1;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;

/** Spark 3.5.3 experiment using the original Dataset action and deserialization paths. */
public final class EarlyShuffleCollector {
  private EarlyShuffleCollector() {
  }

  private static Object invoke(Method method, Object instance, Object... arguments) {
    try {
      return method.invoke(instance, arguments);
    } catch (InvocationTargetException error) {
      Throwable cause = error.getCause();
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new RuntimeException(cause);
    } catch (ReflectiveOperationException error) {
      throw new IllegalStateException(error);
    }
  }

  private static boolean discover(SparkPlan plan, Set<SparkPlan> seen,
      List<SparkPlan> leaves) {
    boolean descendantExchange = false;
    Iterator<SparkPlan> children = plan.children().iterator();
    while (children.hasNext()) {
      descendantExchange |= discover(children.next(), seen, leaves);
    }
    boolean exchange = plan.nodeName().equals("GpuColumnarExchange");
    if (exchange && !descendantExchange &&
        plan.outputPartitioning() instanceof HashPartitioning &&
        seen.add(plan)) {
      leaves.add(plan);
    }
    return exchange || descendantExchange;
  }

  public static Row[] collect(Dataset<Row> frame, boolean eager, String receipt) {
    if (!frame.sparkSession().version().equals("3.5.3")) {
      throw new IllegalArgumentException("This experiment requires Spark 3.5.3");
    }
    List<Map<String, Object>> submissions = new ArrayList<>();
    Map<String, Object> record = new LinkedHashMap<>();
    record.put("enabled", eager);
    record.put("submissions", submissions);
    long started = System.nanoTime();
    try {
      Row[] rows;
      if (!eager) {
        rows = (Row[]) frame.collect();
      } else {
        Method action = Dataset.class.getDeclaredMethod(
            "withAction", String.class, QueryExecution.class, Function1.class);
        Method collect = Dataset.class.getDeclaredMethod("collectFromPlan", SparkPlan.class);
        action.setAccessible(true);
        collect.setAccessible(true);
        rows = (Row[]) invoke(action, frame, "collect", frame.queryExecution(),
            new AbstractFunction1<SparkPlan, Object>() {
              @Override
              public Object apply(SparkPlan plan) {
                // Dataset.withAction resets metrics before this callback and owns SQL execution.
                plan.prepare();
                List<SparkPlan> leaves = new ArrayList<>();
                Set<SparkPlan> seen = Collections.newSetFromMap(new IdentityHashMap<>());
                discover(plan, seen, leaves);
                for (SparkPlan leaf : leaves) {
                  Map<String, Object> submission = new LinkedHashMap<>();
                  submission.put("node_id", leaf.id());
                  submission.put("partitioning", leaf.outputPartitioning().toString());
                  submission.put("start_after_action_seconds",
                      (System.nanoTime() - started) / 1.0e9);
                  try {
                    // The lazy dependency is shared with the final collect, so completed maps
                    // are consumed by the query rather than recomputed into a separate cache.
                    Object future = invoke(leaf.getClass().getMethod(
                        "mapOutputStatisticsFuture"), leaf);
                    Object dependency = invoke(leaf.getClass().getMethod(
                        "shuffleDependencyColumnar"), leaf);
                    submission.put("shuffle_id", invoke(dependency.getClass().getMethod(
                        "shuffleId"), dependency));
                    submission.put("future_class", future.getClass().getName());
                  } catch (NoSuchMethodException error) {
                    throw new IllegalStateException("Unsupported GPU exchange API", error);
                  }
                  submission.put("return_after_action_seconds",
                      (System.nanoTime() - started) / 1.0e9);
                  submissions.add(submission);
                }
                return invoke(collect, frame, plan);
              }
            });
      }
      record.put("status", "passed");
      return rows;
    } catch (ReflectiveOperationException error) {
      throw new IllegalStateException("Spark 3.5.3 Dataset action API differs", error);
    } catch (RuntimeException | Error error) {
      record.put("status", "failed");
      record.put("error", error.toString());
      String group = frame.sparkSession().sparkContext().getLocalProperty("spark.jobGroup.id");
      if (group != null) {
        frame.sparkSession().sparkContext().cancelJobGroup(group);
      }
      throw error;
    } finally {
      record.put("elapsed_seconds", (System.nanoTime() - started) / 1.0e9);
      try {
        new ObjectMapper().writeValue(new File(receipt), record);
      } catch (Exception error) {
        throw new IllegalStateException("Cannot retain execution evidence", error);
      }
    }
  }
}
