/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
#include <cuda.h>
#include <cuda_runtime.h>
#include <ucp/api/ucp.h>
#include <cstdio>
#include <cstring>

#define CHECK(call) do { if ((call) != 0) { std::fprintf(stderr, "FAILED %s\n", #call); return 1; } } while (0)
int main(int argc, char** argv)
{
  if (argc != 2 || (std::strcmp(argv[1], "clobber") && std::strcmp(argv[1], "preserve"))) { return 2; }
  CHECK(cuInit(0));
  CHECK(cuDevicePrimaryCtxSetFlags(0, CU_CTX_SCHED_BLOCKING_SYNC));
  CHECK(cudaSetDevice(0));
  void* device;
  CHECK(cudaMalloc(&device, 65536));
  unsigned initial, after_init, after_map;
  CHECK(cuCtxGetFlags(&initial));
  if ((initial & CU_CTX_SCHED_MASK) != CU_CTX_SCHED_BLOCKING_SYNC) { return 3; }
  ucp_config_t* config;
  CHECK(ucp_config_read(nullptr, nullptr, &config));
  ucp_params_t parameters{};
  parameters.field_mask = UCP_PARAM_FIELD_FEATURES;
  parameters.features = UCP_FEATURE_TAG | UCP_FEATURE_RMA;
  ucp_context_h context;
  CHECK(ucp_init(&parameters, config, &context));
  ucp_config_release(config);
  CHECK(cuCtxGetFlags(&after_init));
  ucp_mem_map_params_t map{};
  map.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH;
  map.address = device;
  map.length = 65536;
  ucp_mem_h memory;
  CHECK(ucp_mem_map(context, &map, &memory));
  CHECK(cuCtxGetFlags(&after_map));
  CHECK(ucp_mem_unmap(context, memory));
  ucp_cleanup(context);
  CHECK(cudaFree(device));
  std::printf("{\"initial\":%u,\"after_ucp_init\":%u,\"after_ucp_mem_map\":%u}\n",
              initial, after_init, after_map);
  auto expected = std::strcmp(argv[1], "preserve") == 0 ? CU_CTX_SCHED_BLOCKING_SYNC : CU_CTX_SCHED_AUTO;
  if ((after_map & CU_CTX_SCHED_MASK) != expected || !(after_map & CU_CTX_SYNC_MEMOPS)) { return 4; }
}
