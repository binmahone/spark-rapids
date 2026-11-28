/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
#include <cuda.h>
#include <cuda_runtime.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>

#define CHECK(call) do { if ((call) != 0) { std::fprintf(stderr, "FAILED %s\n", #call); return 1; } } while (0)
__global__ void delay(unsigned long long* output)
{
  unsigned long long begin, current;
  asm volatile("mov.u64 %0, %%globaltimer;" : "=l"(begin));
  do { asm volatile("mov.u64 %0, %%globaltimer;" : "=l"(current)); }
  while (current - begin < 40000000);
  *output = 123456789;
}
double seconds(clockid_t clock)
{
  timespec value;
  if (clock_gettime(clock, &value)) { std::abort(); }
  return value.tv_sec + value.tv_nsec * 1e-9;
}
int main(int argc, char** argv)
{
  if (argc != 2 || (std::strcmp(argv[1], "auto") && std::strcmp(argv[1], "blocking"))) { return 2; }
  auto desired = std::strcmp(argv[1], "blocking") == 0 ? CU_CTX_SCHED_BLOCKING_SYNC : CU_CTX_SCHED_AUTO;
  CHECK(cuInit(0));
  CHECK(cuDevicePrimaryCtxSetFlags(0, desired));
  CHECK(cudaSetDevice(0));
  unsigned flags;
  CHECK(cuCtxGetFlags(&flags));
  if ((flags & CU_CTX_SCHED_MASK) != desired) { return 3; }
  unsigned long long *device, value;
  CHECK(cudaMalloc(&device, sizeof(value)));
  cudaStream_t stream;
  CHECK(cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking));
  delay<<<1,1,0,stream>>>(device);
  CHECK(cudaGetLastError());
  CHECK(cudaStreamSynchronize(stream));
  double cpu = 0, wall = 0;
  for (int i = 0; i < 5; ++i) {
    delay<<<1,1,0,stream>>>(device);
    CHECK(cudaGetLastError());
    auto c = seconds(CLOCK_THREAD_CPUTIME_ID), w = seconds(CLOCK_MONOTONIC);
    CHECK(cudaStreamSynchronize(stream));
    wall += seconds(CLOCK_MONOTONIC) - w;
    cpu += seconds(CLOCK_THREAD_CPUTIME_ID) - c;
    CHECK(cudaMemcpy(&value, device, sizeof(value), cudaMemcpyDeviceToHost));
    if (value != 123456789) { return 4; }
  }
  CHECK(cudaStreamDestroy(stream));CHECK(cudaFree(device));
  std::printf("{\"mode\":\"%s\",\"flags\":%u,\"cpu_seconds\":%.9f,\"wall_seconds\":%.9f,\"iterations\":5,\"value_correct\":true}\n",
              argv[1],flags,cpu,wall);
}
