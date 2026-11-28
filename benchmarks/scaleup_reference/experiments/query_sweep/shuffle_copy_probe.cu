/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 */
#include <cuda_runtime.h>
#include <jni.h>
#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstdint>

namespace {
__global__ void shuffle_copy_bytes(unsigned char* dst, unsigned char const* src, size_t bytes)
{
  size_t const index = size_t{blockIdx.x} * blockDim.x + threadIdx.x;
  size_t const stride = size_t{gridDim.x} * blockDim.x;
  if (((reinterpret_cast<uintptr_t>(dst) | reinterpret_cast<uintptr_t>(src)) & 15) == 0) {
    auto vectors_dst = reinterpret_cast<uint4*>(dst);
    auto vectors_src = reinterpret_cast<uint4 const*>(src);
    for (size_t i = index; i < bytes / 16; i += stride) vectors_dst[i] = vectors_src[i];
    for (size_t i = (bytes / 16) * 16 + index; i < bytes; i += stride) dst[i] = src[i];
  } else {
    for (size_t i = index; i < bytes; i += stride) dst[i] = src[i];
  }
}
}

extern "C" JNIEXPORT void JNICALL
Java_com_nvidia_spark_rapids_experiments_ShuffleCopyProbe_copy(
    JNIEnv* env, jclass, jlong destination, jlong source, jlong bytes, jlong stream)
{
  if (bytes <= 0 || !destination || !source) {
    env->ThrowNew(env->FindClass("java/lang/IllegalArgumentException"), "Invalid device copy");
    return;
  }
#ifdef SHUFFLE_COPY_PROBE_TEST_NOOP
  return;
#endif
  auto dst = reinterpret_cast<unsigned char*>(destination);
  auto src = reinterpret_cast<unsigned char const*>(source);
  auto cuda_stream = reinterpret_cast<cudaStream_t>(stream);
  bool const aligned = ((destination | source) & 15) == 0;
  size_t const elements = aligned ? (size_t(bytes) + 15) / 16 : size_t(bytes);
  unsigned int const blocks = static_cast<unsigned int>(std::min<size_t>((elements + 255) / 256, 376));
  shuffle_copy_bytes<<<blocks, 256, 0, cuda_stream>>>(dst, src, size_t(bytes));
  auto const result = cudaGetLastError();
  if (result != cudaSuccess) {
    env->ThrowNew(env->FindClass("java/lang/RuntimeException"), cudaGetErrorString(result));
  } else {
    static std::atomic<bool> reported{false};
    if (!reported.exchange(true)) {
      std::fprintf(stderr, "SHUFFLE_COPY_KERNEL_SUBMITTED bytes=%lld\n", static_cast<long long>(bytes));
    }
  }
}
