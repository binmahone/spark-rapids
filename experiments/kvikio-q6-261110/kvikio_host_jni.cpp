/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

#include <jni.h>
#include <kvikio/defaults.hpp>
#include <kvikio/detail/posix_io.hpp>
#include <kvikio/file_handle.hpp>
#include <kvikio/threadpool_wrapper.hpp>
#include <kvikio/utils.hpp>

#include <dlfcn.h>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
std::unique_ptr<kvikio::ThreadPool> pool;
std::mutex initialization_mutex;

using Clock = std::chrono::steady_clock;

long long elapsed_ns(Clock::time_point start)
{
  return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count();
}

int file_handle_limit()
{
  auto const* value = std::getenv("RAPIDS_TEST_KVIKIO_FILE_HANDLES");
  if (value == nullptr) { return 1; }
  std::string text(value);
  size_t consumed = 0;
  auto limit = std::stoi(text, &consumed);
  if (consumed != text.size() || limit < 1 || limit > 4) {
    throw std::invalid_argument("Diagnostic file handle limit must be between one and four");
  }
  return limit;
}

class EndOfFile : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

void report(JNIEnv* env, char const* type, std::exception const& error)
{
  if (!env->ExceptionCheck()) {
    auto klass = env->FindClass(type);
    if (klass != nullptr) { env->ThrowNew(klass, error.what()); }
  }
}

std::vector<jlong> values(JNIEnv* env, jlongArray array)
{
  if (array == nullptr) { throw std::invalid_argument("Null range array"); }
  std::vector<jlong> result(env->GetArrayLength(array));
  env->GetLongArrayRegion(array, 0, result.size(), result.data());
  if (env->ExceptionCheck()) { throw std::runtime_error("Could not copy JNI range array"); }
  return result;
}
}  // namespace

extern "C" JNIEXPORT void JNICALL
Java_com_nvidia_spark_rapids_fileio_hadoop_KvikioHostRangeReader_initializeNative(
  JNIEnv* env, jclass, jint threads)
{
  try {
    if (threads < 1 || threads > 128) { throw std::invalid_argument("Invalid KvikIO thread limit"); }
    std::lock_guard<std::mutex> guard(initialization_mutex);
    if (pool) { throw std::logic_error("Diagnostic KvikIO pool already initialized"); }
    kvikio::defaults::set_compat_mode(kvikio::CompatMode::ON);
    kvikio::defaults::set_auto_direct_io_read(false);
    pool = std::make_unique<kvikio::ThreadPool>(threads);
  } catch (std::exception const& error) {
    report(env, "java/io/IOException", error);
  }
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_nvidia_spark_rapids_fileio_hadoop_KvikioHostRangeReader_backendNative(JNIEnv* env, jclass)
{
  Dl_info info{};
  if (!dladdr(reinterpret_cast<void*>(&kvikio::defaults::thread_pool_nthreads), &info)) {
    return env->NewStringUTF("unresolved");
  }
  return env->NewStringUTF(info.dli_fname);
}

extern "C" JNIEXPORT jlong JNICALL
Java_com_nvidia_spark_rapids_fileio_hadoop_KvikioHostRangeReader_readNative(
  JNIEnv* env, jclass, jbyteArray path, jlong address, jlong capacity, jlongArray offsets_array,
  jlongArray lengths_array, jlongArray destinations_array, jlong task_bytes, jint window)
{
  try {
    if (!pool || path == nullptr || address == 0 || capacity < 0 || task_bytes < 1 || window < 1) {
      throw std::invalid_argument("Invalid diagnostic KvikIO read arguments");
    }
    std::string filename(env->GetArrayLength(path), '\0');
    env->GetByteArrayRegion(path, 0, filename.size(), reinterpret_cast<jbyte*>(filename.data()));
    if (env->ExceptionCheck()) { return 0; }
    if (filename.empty() || filename.front() != '/' || filename.find('\0') != std::string::npos) {
      throw std::invalid_argument("Invalid local filename");
    }
    auto const offsets      = values(env, offsets_array);
    auto const lengths      = values(env, lengths_array);
    auto const destinations = values(env, destinations_array);
    if (offsets.size() != lengths.size() || offsets.size() != destinations.size()) {
      throw std::invalid_argument("Mismatched range arrays");
    }
    if (offsets.empty()) { return 0; }
    auto* buffer = reinterpret_cast<std::uint8_t*>(static_cast<std::uintptr_t>(address));
    if (!kvikio::is_host_memory(buffer)) { throw std::invalid_argument("Expected native host memory"); }
    for (size_t i = 0; i < offsets.size(); ++i) {
      if (offsets[i] < 0 || lengths[i] < 1 || destinations[i] < 0 || destinations[i] > capacity ||
          lengths[i] > capacity - destinations[i]) {
        throw std::invalid_argument("Range is outside destination memory");
      }
    }
    auto const started = Clock::now();
    auto const* lane_setting = std::getenv("RAPIDS_TEST_KVIKIO_LANE_TASKS");
    bool const lane_tasks = lane_setting != nullptr && std::string(lane_setting) == "4";
    if (lane_setting != nullptr && !lane_tasks && std::string(lane_setting) != "0") {
      throw std::invalid_argument("Diagnostic lane tasks must be zero or four");
    }
    auto const handle_count = std::min({file_handle_limit(), window, static_cast<int>(offsets.size())});
    std::vector<std::unique_ptr<kvikio::FileHandle>> sources;
    sources.reserve(handle_count);
    for (int i = 0; i < handle_count; ++i) {
      sources.push_back(std::make_unique<kvikio::FileHandle>(
        filename, "r", kvikio::FileHandle::m644, kvikio::CompatMode::ON));
    }
    auto const open_ns = elapsed_ns(started);
    long long submit_ns = 0;
    long long wait_ns = 0;
    auto const file_bytes = sources.front()->nbytes();
    // Allocate future slots before submitting work; destination memory and the file outlive all IO.
    auto const work_items = lane_tasks ? std::min<size_t>(4, offsets.size()) : offsets.size();
    std::vector<std::future<std::size_t>> futures(work_items);
    std::vector<std::size_t> expected(work_items, 0);
    for (size_t i = 0; i < offsets.size(); ++i) {
      expected[lane_tasks ? i % work_items : i] += lengths[i];
    }
    size_t submitted = 0;
    size_t consumed  = 0;
    jlong total      = 0;
    std::exception_ptr failure;
    auto consume = [&] {
      auto index = consumed++;
      auto const wait_started = Clock::now();
      auto count = futures[index].get();
      wait_ns += elapsed_ns(wait_started);
      if (count != expected[index]) {
        throw EndOfFile("Short diagnostic KvikIO read");
      }
      total += count;
    };
    try {
      if (lane_tasks) {
        // Coarse tasks use KvikIO's pool and POSIX implementation without nested pool submissions.
        // Each task completes one strided lane, matching the FileChannel diagnostic's granularity.
        for (size_t lane = 0; lane < work_items; ++lane) {
          auto const submit_started = Clock::now();
          futures[lane] = pool->submit_task([&, lane]() -> std::size_t {
            auto const& source = *sources[lane % sources.size()];
            std::size_t result = 0;
            for (size_t i = lane; i < offsets.size(); i += work_items) {
              auto const offset = static_cast<std::size_t>(offsets[i]);
              auto const length = static_cast<std::size_t>(lengths[i]);
              if (offset > file_bytes || length > file_bytes - offset) {
                throw EndOfFile("Requested lane range exceeds the file length");
              }
              auto const count = kvikio::detail::posix_host_read<kvikio::detail::PartialIO::NO>(
                source.fd(false), buffer + destinations[i], length, offset);
              if (count != length) { throw EndOfFile("Short diagnostic KvikIO lane read"); }
              result += count;
            }
            return result;
          });
          submit_ns += elapsed_ns(submit_started);
          ++submitted;
        }
      } else {
        for (size_t i = 0; i < offsets.size(); ++i) {
          auto offset = static_cast<std::size_t>(offsets[i]);
          auto length = static_cast<std::size_t>(lengths[i]);
          if (offset > file_bytes || length > file_bytes - offset) {
            throw EndOfFile("Requested range exceeds the file length");
          }
          auto const submit_started = Clock::now();
          futures[i] = sources[i % sources.size()]->pread(
            buffer + destinations[i], length, offset, task_bytes, 0, false, pool.get());
          submit_ns += elapsed_ns(submit_started);
          ++submitted;
          if (submitted - consumed >= static_cast<std::size_t>(window)) { consume(); }
        }
      }
    } catch (...) {
      failure = std::current_exception();
    }
    // Drain on success, error, or cancellation before the JNI caller may release its host buffer.
    while (consumed < submitted) {
      try {
        consume();
      } catch (...) {
        if (!failure) { failure = std::current_exception(); }
      }
    }
    std::fprintf(stderr,
      "KVIKIO_NATIVE_TIMING handles=%d ranges=%zu bytes=%lld openNs=%lld submitNs=%lld "
      "waitNs=%lld totalNs=%lld completed=%s scheduling=%s path=%s\n",
      handle_count, offsets.size(), static_cast<long long>(total), open_ns, submit_ns,
      wait_ns, elapsed_ns(started), failure ? "false" : "true",
      lane_tasks ? "strided-lanes" : "per-range", filename.c_str());
    if (failure) { std::rethrow_exception(failure); }
    return total;
  } catch (EndOfFile const& error) {
    report(env, "java/io/EOFException", error);
  } catch (std::exception const& error) {
    report(env, "java/io/IOException", error);
  }
  return 0;
}
