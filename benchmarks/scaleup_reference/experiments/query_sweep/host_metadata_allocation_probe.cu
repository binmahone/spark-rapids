/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 */
#include "cudf_jni_apis.hpp"
#include "probe_allocator.hpp"
#include <cudf/detail/utilities/vector_factories.hpp>
#include <cudf/utilities/pinned_memory.hpp>
#include <chrono>
#include <cstdint>
#include <sstream>

__global__ void delayed_write(uint64_t* output, uint64_t value)
{
  uint64_t begin, current;
  asm volatile("mov.u64 %0, %%globaltimer;" : "=l"(begin));
  do {
    asm volatile("mov.u64 %0, %%globaltimer;" : "=l"(current));
  } while (current - begin < 40000000);
  if (output != nullptr) { *output = value; }
}

namespace {
using clock_type = std::chrono::steady_clock;
double milliseconds(clock_type::time_point start)
{
  return std::chrono::duration<double, std::milli>(clock_type::now() - start).count();
}
struct owned_stream {
  cudaStream_t stream{};
  owned_stream() { CUDF_CUDA_TRY(cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking)); }
  ~owned_stream()
  {
    cudaStreamSynchronize(stream);
    cudaStreamDestroy(stream);
  }
  owned_stream(owned_stream const&) = delete;
  owned_stream& operator=(owned_stream const&) = delete;
  cudaStream_t value() const { return stream; }
  void synchronize() const { CUDF_CUDA_TRY(cudaStreamSynchronize(stream)); }
};
struct streams {
  owned_stream compute;
  owned_stream allocation;
  ~streams()
  {
    cudaStreamSynchronize(compute.value());
    cudaStreamSynchronize(allocation.value());
  }
};
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_nvidia_spark_rapids_experiments_HostMetadataAllocationProbe_run(
  JNIEnv* env, jclass, jlong pool_bytes, jboolean unsafe_mutation)
{
  JNI_TRY {
    cudf::jni::auto_set_device(env);
    verify_shared_probe_allocator();
    CUDF_EXPECTS(pool_bytes == 64 * 1024 * 1024, "Unexpected pinned test pool size");
    streams s;
    auto resource = cudf::get_pinned_memory_resource();
    // Load and finish the kernel before measuring CUDA module initialization effects.
    delayed_write<<<1, 1, 0, s.compute.value()>>>(nullptr, 0);
    s.compute.synchronize();
    double control_ms, isolated_ms, reused_ms;
    delayed_write<<<1, 1, 0, s.compute.value()>>>(nullptr, 0);
    auto begin = clock_type::now();
    {
      auto host = cudf::detail::make_pinned_vector_async<uint64_t>(1024, cuda::stream_ref{s.compute.value()});
      control_ms = milliseconds(begin);
      CUDF_EXPECTS(cudaStreamQuery(s.compute.value()) == cudaSuccess,
                   "Control allocation did not synchronize the compute stream");
      host[0] = 23;
    }
    s.compute.synchronize();
    delayed_write<<<1, 1, 0, s.compute.value()>>>(nullptr, 0);
    begin = clock_type::now();
    void* isolated = resource.allocate(cuda::stream_ref{s.allocation.value()}, 8192, 256);
    s.allocation.synchronize();
    isolated_ms = milliseconds(begin);
    auto compute_pending = cudaStreamQuery(s.compute.value()) == cudaErrorNotReady;
    static_cast<uint64_t*>(isolated)[0] = 29;
    resource.deallocate(cuda::stream_ref{s.compute.value()}, isolated, 8192, 256);
    s.compute.synchronize();
    CUDF_EXPECTS(compute_pending, "Isolated allocation waited for unrelated compute work");

    // Exhaust the real pinned pool so allocation must reuse a buffer with a pending GPU writer.
    void* prior = resource.allocate(cuda::stream_ref{s.allocation.value()}, pool_bytes, 256);
    s.allocation.synchronize();
    auto* data = static_cast<uint64_t*>(prior);
    *data = 0;
    constexpr uint64_t expected = 0x123456789abcdef0ULL;
    delayed_write<<<1, 1, 0, s.compute.value()>>>(data, expected);
    resource.deallocate(cuda::stream_ref{s.compute.value()}, prior, pool_bytes, 256);
    begin = clock_type::now();
    void* reused = resource.allocate(cuda::stream_ref{s.allocation.value()}, pool_bytes, 256);
    if (!unsafe_mutation) { s.allocation.synchronize(); }
    reused_ms = milliseconds(begin);
    auto ready = cudaStreamQuery(s.compute.value()) == cudaSuccess;
    // Synchronize before inspecting or freeing the returned buffer, including the negative test.
    s.compute.synchronize();
    s.allocation.synchronize();
    auto const same = reused == prior;
    auto const value = *static_cast<uint64_t*>(reused);
    resource.deallocate(cuda::stream_ref{s.compute.value()}, reused, pool_bytes, 256);
    s.compute.synchronize();
    CUDF_EXPECTS(same && value == expected, "Pinned allocation did not reuse the live test buffer");
    CUDF_EXPECTS(ready, "HOST_METADATA_REUSE_GUARD: allocation returned before the GPU writer completed");
    std::ostringstream out;
    out << "{\"control_wait_ms\":" << control_ms
        << ",\"isolated_wait_ms\":" << isolated_ms
        << ",\"required_reuse_wait_ms\":" << reused_ms
        << ",\"pending_buffer_reused_safely\":true,\"scope\":\"allocation mechanism only\"}";
    return env->NewStringUTF(out.str().c_str());
  }
  JNI_CATCH(env, nullptr);
}
