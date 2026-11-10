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
#include <kvikio/file_handle.hpp>
#include <kvikio/threadpool_wrapper.hpp>
#include <kvikio/utils.hpp>

#include <dlfcn.h>
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
    auto* buffer = reinterpret_cast<std::uint8_t*>(static_cast<std::uintptr_t>(address));
    if (!kvikio::is_host_memory(buffer)) { throw std::invalid_argument("Expected native host memory"); }
    for (size_t i = 0; i < offsets.size(); ++i) {
      if (offsets[i] < 0 || lengths[i] < 1 || destinations[i] < 0 || destinations[i] > capacity ||
          lengths[i] > capacity - destinations[i]) {
        throw std::invalid_argument("Range is outside destination memory");
      }
    }
    kvikio::FileHandle source(filename, "r", kvikio::FileHandle::m644, kvikio::CompatMode::ON);
    // Allocate future slots before submitting work; destination memory and the file outlive all IO.
    std::vector<std::future<std::size_t>> futures(offsets.size());
    size_t submitted = 0;
    size_t consumed  = 0;
    jlong total      = 0;
    std::exception_ptr failure;
    auto consume = [&] {
      auto index = consumed++;
      auto count = futures[index].get();
      if (count != static_cast<std::size_t>(lengths[index])) {
        throw EndOfFile("Short diagnostic KvikIO read");
      }
      total += count;
    };
    try {
      for (size_t i = 0; i < offsets.size(); ++i) {
        auto offset = static_cast<std::size_t>(offsets[i]);
        auto length = static_cast<std::size_t>(lengths[i]);
        if (offset > source.nbytes() || length > source.nbytes() - offset) {
          throw EndOfFile("Requested range exceeds the file length");
        }
        futures[i] = source.pread(buffer + destinations[i], length, offset, task_bytes,
                                  0, false, pool.get());
        ++submitted;
        if (submitted - consumed >= static_cast<std::size_t>(window)) { consume(); }
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
    if (failure) { std::rethrow_exception(failure); }
    return total;
  } catch (EndOfFile const& error) {
    report(env, "java/io/EOFException", error);
  } catch (std::exception const& error) {
    report(env, "java/io/IOException", error);
  }
  return 0;
}
