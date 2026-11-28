/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 */
#pragma once
#include "copy_admission.hpp"
#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cuda/stream>
#include <cuda_runtime_api.h>
#include <cudf/utilities/error.hpp>
#include <cudf/detail/utilities/cuda_memcpy.hpp>
#include <memory>
#include <mutex>

namespace scan_copy_experiment {
extern int admission_mode;
extern std::string admission_path;
inline std::atomic<bool> entered{false}, admitted{false}, completed{false};
inline std::mutex executor_copy_mutex;
inline std::atomic<bool> serial_acquired{false}, serial_contended{false};

class copy_admission {
public:
  copy_admission(cuda::stream_ref stream, size_t bytes) : stream_(stream) {
    if (!entered.exchange(true)) {
      std::fprintf(stderr, "SCAN_COPY_NATIVE_ENTER mode=%d\n", admission_mode);
    }
    active_ = (admission_mode == 1 || admission_mode == 2) && bytes >= 32 * 1024 * 1024;
    if (!active_) {
      return;
    }
    // Finish preceding GPU dependencies before taking an interprocess copy
    // lease.
    CUDF_CUDA_TRY(cudaStreamSynchronize(stream_.get()));
    if (admission_mode == 2) {
      lease_ = std::make_unique<file_copy_lease>(admission_path);
    }
    if (!admitted.exchange(true)) {
      std::fprintf(stderr, "SCAN_COPY_ADMITTED mode=%d bytes=%zu path=%s\n",
                   admission_mode, bytes, admission_path.c_str());
    }
  }
  void finish() {
    if (active_) {
      CUDF_CUDA_TRY(cudaStreamSynchronize(stream_.get()));
      if (!completed.exchange(true)) {
        std::fprintf(stderr, "SCAN_COPY_COMPLETED mode=%d\n", admission_mode);
      }
    }
  }

private:
  cuda::stream_ref stream_;
  bool active_ = false;
  std::unique_ptr<file_copy_lease> lease_;
};

inline cudaError_t submit_copies(void* const* destinations,
                                 void const* const* sources,
                                 size_t const* sizes,
                                 size_t count,
                                 cuda::stream_ref stream) {
  if (admission_mode < 3) {
    return cudf::detail::memcpy_batch_async(destinations, sources, sizes, count, stream);
  }
  constexpr size_t quantum = 32 * 1024 * 1024;
  size_t total = 0, chunks = 0;
  for (size_t i = 0; i < count; ++i) total += sizes[i];
  if (total <= quantum) {
    return cudf::detail::memcpy_batch_async(destinations, sources, sizes, count, stream);
  }
  std::unique_lock<std::mutex> owner(executor_copy_mutex, std::defer_lock);
  if (admission_mode == 5) {
    static bool const single_device = [] {
      int count = 0;
      CUDF_CUDA_TRY(cudaGetDeviceCount(&count));
      CUDF_EXPECTS(count == 1, "Serial copy experiment requires one visible GPU per process");
      return true;
    }();
    (void)single_device;
    if (!owner.try_lock()) {
      if (!serial_contended.exchange(true)) {
        std::fprintf(stderr, "SCAN_COPY_SERIAL_CONTENDED mode=5\n");
      }
      owner.lock();
    }
    if (!serial_acquired.exchange(true)) {
      std::fprintf(stderr, "SCAN_COPY_SERIAL_ACQUIRED mode=5\n");
    }
  }
  for (size_t i = 0; i < count; ++i) {
    for (size_t offset = 0; offset < sizes[i]; offset += quantum) {
      auto const bytes = std::min(quantum, sizes[i] - offset);
      CUDF_CUDA_TRY(cudaMemcpyAsync(static_cast<char*>(destinations[i]) + offset,
                                   static_cast<char const*>(sources[i]) + offset,
                                   bytes, cudaMemcpyHostToDevice, stream.get()));
      if (admission_mode == 3 || admission_mode == 5) {
        // Bound queued copy work so another task can submit decode metadata transfers.
        CUDF_CUDA_TRY(cudaStreamSynchronize(stream.get()));
      }
      ++chunks;
    }
  }
  if (!completed.exchange(true)) {
    std::fprintf(stderr, "SCAN_COPY_PACED_SUBMITTED mode=%d bytes=%zu chunks=%zu\n",
                 admission_mode, total, chunks);
  }
  return cudaSuccess;
}
} // namespace scan_copy_experiment
