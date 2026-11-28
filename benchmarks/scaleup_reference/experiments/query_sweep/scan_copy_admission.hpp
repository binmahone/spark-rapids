/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 */
#pragma once
#include "copy_admission.hpp"
#include <atomic>
#include <cstdio>
#include <cuda/stream>
#include <cuda_runtime_api.h>
#include <cudf/utilities/error.hpp>
#include <memory>

namespace scan_copy_experiment {
extern int admission_mode;
extern std::string admission_path;
inline std::atomic<bool> entered{false}, admitted{false}, completed{false};

class copy_admission {
public:
  copy_admission(cuda::stream_ref stream, size_t bytes) : stream_(stream) {
    if (!entered.exchange(true)) {
      std::fprintf(stderr, "SCAN_COPY_NATIVE_ENTER mode=%d\n", admission_mode);
    }
    active_ = admission_mode != 0 && bytes >= 32 * 1024 * 1024;
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
} // namespace scan_copy_experiment
