/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 */
#include "cudf_jni_apis.hpp"
#include <cudf/concatenate.hpp>
#include <cudf/strings/detail/concatenate.hpp>
#include <cudf/table/table.hpp>
#include <algorithm>
#include <atomic>
#include <cstdio>

namespace cudf::strings::offset_experiment {
std::unique_ptr<column> concatenate(host_span<column_view const>, cuda::stream_ref,
                                  cudf::memory_resources);
}

namespace {
std::atomic<bool> use_candidate{false};
std::atomic<int64_t> calls{0}, string_columns{0}, input_tables{0};

jlongArray concatenate(JNIEnv* env, jclass, jlongArray handles)
{
  JNI_NULL_CHECK(env, handles, "input tables are null", nullptr);
  JNI_TRY {
    cudf::jni::auto_set_device(env);
    cudf::jni::native_jpointerArray<cudf::table_view> wrapped(env, handles);
    auto const tables = wrapped.get_dereferenced();
    bool const candidate = use_candidate.load(std::memory_order_relaxed);
    if (calls.fetch_add(1, std::memory_order_relaxed) == 0) {
      std::fprintf(stderr, "CONCAT_OFFSET_PROBE_FIRST_CALL mode=%s\n",
                   candidate ? "candidate" : "control");
    }
    input_tables.fetch_add(tables.size(), std::memory_order_relaxed);
    if (!candidate || tables.empty()) {
      return cudf::jni::convert_table_for_return(env, cudf::concatenate(tables));
    }
    auto const width = tables.front().num_columns();
    CUDF_EXPECTS(std::all_of(tables.begin(), tables.end(), [width](auto const& t) {
      return t.num_columns() == width;
    }), "All tables must have the same number of columns");
    std::vector<std::unique_ptr<cudf::column>> output;
    output.reserve(width);
    for (int i = 0; i < width; ++i) {
      std::vector<cudf::column_view> columns;
      columns.reserve(tables.size());
      for (auto const& table : tables) { columns.push_back(table.column(i)); }
      if (std::all_of(columns.begin(), columns.end(), [](auto const& column) {
        return column.type().id() == cudf::type_id::STRING;
      })) {
        if (string_columns.fetch_add(1, std::memory_order_relaxed) == 0) {
          std::fprintf(stderr, "CONCAT_OFFSET_PROBE_FIRST_STRING mode=candidate\n");
        }
        output.push_back(cudf::strings::offset_experiment::concatenate(
          columns, cudf::get_default_stream(), cudf::get_current_device_resource_ref()));
      } else {
        output.push_back(cudf::concatenate(columns));
      }
    }
    return cudf::jni::convert_table_for_return(
      env, std::make_unique<cudf::table>(std::move(output)));
  }
  JNI_CATCH(env, nullptr);
}
}

extern "C" JNIEXPORT void JNICALL
Java_com_nvidia_spark_rapids_experiments_ConcatOffsetProbe_install(
  JNIEnv* env, jclass, jclass table, jboolean candidate)
{
  use_candidate.store(candidate, std::memory_order_relaxed);
  JNINativeMethod method{const_cast<char*>("concatenate"), const_cast<char*>("([J)[J"),
                         reinterpret_cast<void*>(&concatenate)};
  if (env->RegisterNatives(table, &method, 1) != JNI_OK && !env->ExceptionCheck()) {
    auto error = env->FindClass("java/lang/UnsatisfiedLinkError");
    if (error != nullptr) { env->ThrowNew(error, "Unable to bind native concatenate probe"); }
  }
}

extern "C" JNIEXPORT jlongArray JNICALL
Java_com_nvidia_spark_rapids_experiments_ConcatOffsetProbe_counters(JNIEnv* env, jclass)
{
  jlong const values[]{calls.load(), string_columns.load(), input_tables.load()};
  auto result = env->NewLongArray(3);
  if (result != nullptr) { env->SetLongArrayRegion(result, 0, 3, values); }
  return result;
}
