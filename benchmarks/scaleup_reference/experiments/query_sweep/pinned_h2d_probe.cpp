/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 */
#include <cuda_runtime.h>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <unistd.h>

void check(cudaError_t status)
{
  if (status != cudaSuccess) { throw std::runtime_error(cudaGetErrorString(status)); }
}

int main(int argc, char** argv)
{
  try {
    if (argc != 3) { throw std::runtime_error("Expected GPU index and barrier directory"); }
    int const gpu = std::stoi(argv[1]);
    std::filesystem::path const directory(argv[2]);
    check(cudaSetDevice(gpu));
    cudaStream_t stream;
    cudaEvent_t begin, end;
    check(cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking));
    check(cudaEventCreate(&begin));
    check(cudaEventCreate(&end));
    size_t const capacity = size_t{1} << 30;
    void *host = nullptr, *device = nullptr;
    check(cudaMallocHost(&host, capacity));
    check(cudaMalloc(&device, capacity));
    std::memset(host, 0x5a, capacity);
    {
      std::ifstream maps("/proc/self/numa_maps");
      std::ofstream saved(directory / ("numa-" + std::to_string(gpu) + ".txt"));
      saved << maps.rdbuf();
    }
    for (size_t bytes : {size_t{2} << 20, size_t{64} << 20, capacity}) {
      int const copies = static_cast<int>((size_t{8} << 30) / bytes);
      check(cudaMemcpyAsync(device, host, bytes, cudaMemcpyHostToDevice, stream));
      check(cudaStreamSynchronize(stream));
      for (int repetition = 0; repetition < 3; ++repetition) {
        auto const token = std::to_string(bytes) + "-" + std::to_string(repetition);
        std::ofstream(directory / (token + "-ready-" + std::to_string(gpu))) << getpid();
        auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (!std::filesystem::exists(directory / (token + "-go"))) {
          if (std::chrono::steady_clock::now() > deadline) {
            throw std::runtime_error("Barrier timed out");
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        auto const wall_begin = std::chrono::steady_clock::now();
        check(cudaEventRecord(begin, stream));
        for (int i = 0; i < copies; ++i) {
          check(cudaMemcpyAsync(device, host, bytes, cudaMemcpyHostToDevice, stream));
        }
        check(cudaEventRecord(end, stream));
        check(cudaEventSynchronize(end));
        auto const wall = std::chrono::duration<double>(
          std::chrono::steady_clock::now() - wall_begin).count();
        float milliseconds;
        check(cudaEventElapsedTime(&milliseconds, begin, end));
        unsigned char received = 0;
        check(cudaMemcpy(&received, static_cast<char*>(device) + bytes - 1, 1,
                         cudaMemcpyDeviceToHost));
        if (received != 0x5a) { throw std::runtime_error("Copied byte differs"); }
        std::cout << "{\"gpu\":" << gpu << ",\"bytes_per_copy\":" << bytes
          << ",\"copies\":" << copies << ",\"repetition\":" << repetition
          << ",\"cuda_seconds\":" << milliseconds / 1000.0
          << ",\"wall_seconds\":" << wall << ",\"validation\":\"passed\"}" << std::endl;
      }
    }
    check(cudaFree(device));
    check(cudaFreeHost(host));
    check(cudaEventDestroy(begin));
    check(cudaEventDestroy(end));
    check(cudaStreamDestroy(stream));
  } catch (std::exception const& error) {
    std::cerr << error.what() << std::endl;
    return 1;
  }
}
