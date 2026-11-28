/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 */
#pragma once

#include <cudf/detail/utilities/cuda_memcpy.hpp>
#include <cudf/utilities/error.hpp>
#include <rmm/mr/per_device_resource.hpp>
#include <cstdio>
#include <dlfcn.h>

inline void verify_shared_probe_allocator()
{
  Dl_info library{};
  CUDF_EXPECTS(dladdr(reinterpret_cast<void*>(&cudf::detail::memcpy_batch_async), &library),
               "Unable to identify loaded libcudf");
  auto* handle = dlopen(library.dli_fname, RTLD_NOW | RTLD_NOLOAD);
  CUDF_EXPECTS(handle != nullptr, "Loaded libcudf handle is unavailable");
  // The diagnostic SDK pins the RMM 26.12 ABI. Its registry must retain process-wide visibility.
  auto* process_registry = dlsym(handle,
    "_ZZN3rmm10_RMM_26_122mr6detail11get_ref_mapEvE21device_id_to_resource");
  dlclose(handle);
  auto* local_registry = static_cast<void*>(&rmm::mr::detail::get_ref_map());
  CUDF_EXPECTS(process_registry != nullptr && process_registry == local_registry,
               "Native probe has a private RMM registry and cannot run a valid comparison");
  std::fprintf(stderr, "NATIVE_PROBE_RMM_SHARED registry=%p library=%s\n",
               local_registry, library.dli_fname);
}
