/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 */
#include <cuda_runtime.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <sched.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {
constexpr int devices = 8;
constexpr size_t capacity = size_t{128} << 20;
constexpr int rounds = 16;
void check(cudaError_t status)
{
  if (status != cudaSuccess) throw std::runtime_error(cudaGetErrorString(status));
}
struct barrier {
  std::mutex mutex;
  std::condition_variable condition;
  int arrived = 0;
  int generation = 0;
  void wait()
  {
    std::unique_lock<std::mutex> lock(mutex);
    int const current = generation;
    if (++arrived == devices + 1) {
      arrived = 0;
      ++generation;
      condition.notify_all();
    } else {
      condition.wait(lock, [&] { return generation != current; });
    }
  }
};
struct buffers {
  unsigned char* source{};
  std::array<unsigned char*, devices> destination{};
  std::array<cudaStream_t, devices> streams{};
  unsigned int* errors{};
};
std::array<int, devices> numbers(char const* text)
{
  std::array<int, devices> values{};
  std::stringstream input(text);
  std::string part;
  for (int i = 0; i < devices; ++i) {
    if (!std::getline(input, part, ',')) throw std::runtime_error("Expected eight entries");
    values[i] = std::stoi(part);
  }
  if (std::getline(input, part, ',')) throw std::runtime_error("Too many entries");
  return values;
}
__global__ void verify(unsigned char const* data, size_t bytes,
                       unsigned char expected, unsigned int* errors)
{
  for (size_t i = size_t{blockIdx.x} * blockDim.x + threadIdx.x;
       i < bytes; i += size_t{blockDim.x} * gridDim.x) {
    if (data[i] != expected) atomicOr(errors, 1U);
  }
}
}

int main(int argc, char** argv)
{
  try {
    if (argc != 3) throw std::runtime_error("Expected GPU NUMA nodes and submission CPUs");
    auto nodes = numbers(argv[1]);
    auto cpus = numbers(argv[2]);
    int count = 0;
    check(cudaGetDeviceCount(&count));
    if (count != devices) throw std::runtime_error("This probe requires eight visible GPUs");
    std::array<buffers, devices> memory{};
    for (int gpu = 0; gpu < devices; ++gpu) {
      check(cudaSetDevice(gpu));
      cudaDeviceProp property;
      check(cudaGetDeviceProperties(&property, gpu));
      std::cout << "{\"kind\":\"device\",\"gpu\":" << gpu
                << ",\"pci_domain\":" << property.pciDomainID
                << ",\"pci_bus\":" << property.pciBusID
                << ",\"pci_device\":" << property.pciDeviceID << "}" << std::endl;
      for (int peer = 0; peer < devices; ++peer) {
        if (peer == gpu) continue;
        int accessible = 0;
        check(cudaDeviceCanAccessPeer(&accessible, gpu, peer));
        if (!accessible) throw std::runtime_error("A directed P2P pair is unavailable");
        auto status = cudaDeviceEnablePeerAccess(peer, 0);
        if (status == cudaErrorPeerAccessAlreadyEnabled) { (void)cudaGetLastError(); }
        else check(status);
        check(cudaStreamCreateWithFlags(&memory[gpu].streams[peer], cudaStreamNonBlocking));
        check(cudaMalloc(&memory[gpu].destination[peer], capacity));
      }
      check(cudaMalloc(&memory[gpu].source, capacity));
      check(cudaMalloc(&memory[gpu].errors, sizeof(unsigned int)));
    }
    int local_peer = -1, remote_peer = -1;
    for (int peer = 1; peer < devices; ++peer) {
      if (nodes[peer] == nodes[0] && local_peer == -1) local_peer = peer;
      if (nodes[peer] != nodes[0] && remote_peer == -1) remote_peer = peer;
    }
    if (local_peer < 0 || remote_peer < 0) throw std::runtime_error("Expected two NUMA groups");
    for (std::string mode : {"one-local", "one-remote", "within-numa", "across-numa", "all-to-all"}) {
      std::vector<std::pair<int, int>> edges;
      for (int src = 0; src < devices; ++src) {
        for (int dst = 0; dst < devices; ++dst) {
          if (src == dst) continue;
          if ((mode == "one-local" && src == 0 && dst == local_peer) ||
              (mode == "one-remote" && src == 0 && dst == remote_peer) ||
              (mode == "within-numa" && nodes[src] == nodes[dst]) ||
              (mode == "across-numa" && nodes[src] != nodes[dst]) || mode == "all-to-all") {
            edges.emplace_back(src, dst);
          }
        }
      }
      for (bool pull : {false, true}) {
        for (size_t chunk : {size_t{4} << 20, capacity}) {
          for (int repetition = -1; repetition < 3; ++repetition) {
            barrier rendezvous;
            std::vector<std::thread> workers;
            for (int gpu = 0; gpu < devices; ++gpu) {
              workers.emplace_back([&, gpu] {
                try {
                  cpu_set_t affinity;
                  CPU_ZERO(&affinity);
                  CPU_SET(cpus[gpu], &affinity);
                  if (sched_setaffinity(0, sizeof(affinity), &affinity) != 0)
                    throw std::runtime_error("CPU binding failed");
                  check(cudaSetDevice(gpu));
                  check(cudaMemset(memory[gpu].source, 1 + gpu + 16 * (repetition + 1), capacity));
                  for (auto [src, dst] : edges) {
                    if (dst == gpu) check(cudaMemset(memory[dst].destination[src], 0, capacity));
                  }
                  check(cudaDeviceSynchronize());
                  rendezvous.wait();
                  rendezvous.wait();
                  for (int round = 0; round < rounds; ++round) {
                    for (size_t offset = 0; offset < capacity; offset += chunk) {
                      for (auto [src, dst] : edges) {
                        if (gpu != (pull ? dst : src)) continue;
                        auto stream = memory[gpu].streams[pull ? src : dst];
                        check(cudaMemcpyPeerAsync(memory[dst].destination[src] + offset, dst,
                            memory[src].source + offset, src, chunk, stream));
                      }
                    }
                  }
                  for (int peer = 0; peer < devices; ++peer) {
                    if (peer != gpu) check(cudaStreamSynchronize(memory[gpu].streams[peer]));
                  }
                  rendezvous.wait();
                  check(cudaMemset(memory[gpu].errors, 0, sizeof(unsigned int)));
                  for (auto [src, dst] : edges) {
                    if (dst == gpu) {
                      verify<<<1024, 256>>>(memory[dst].destination[src], capacity,
                          static_cast<unsigned char>(1 + src + 16 * (repetition + 1)), memory[gpu].errors);
                      check(cudaGetLastError());
                    }
                  }
                  unsigned int errors = 0;
                  check(cudaMemcpy(&errors, memory[gpu].errors, sizeof(errors), cudaMemcpyDeviceToHost));
                  if (errors) throw std::runtime_error("Full destination buffer validation failed");
                } catch (std::exception const& error) {
                  std::cerr << "GPU " << gpu << ": " << error.what() << std::endl;
                  std::_Exit(2);
                }
              });
            }
            rendezvous.wait();
            auto const begin = std::chrono::steady_clock::now();
            rendezvous.wait();
            rendezvous.wait();
            double const seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
            for (auto& worker : workers) worker.join();
            if (repetition >= 0) {
              auto const bytes = capacity * rounds * edges.size();
              std::cout << "{\"kind\":\"measurement\",\"mode\":\"" << mode
                << "\",\"initiator\":\"" << (pull ? "receiver" : "sender")
                << "\",\"chunk_bytes\":" << chunk << ",\"edges\":" << edges.size()
                << ",\"repetition\":" << repetition << ",\"bytes\":" << bytes
                << ",\"wall_seconds\":" << seconds << ",\"aggregate_GBps\":"
                << bytes / seconds / 1e9 << ",\"validation\":\"passed\"}" << std::endl;
            }
          }
        }
      }
    }
    for (int gpu = 0; gpu < devices; ++gpu) {
      check(cudaSetDevice(gpu));
      check(cudaDeviceSynchronize());
      for (int peer = 0; peer < devices; ++peer) {
        if (peer == gpu) continue;
        check(cudaFree(memory[gpu].destination[peer]));
        check(cudaStreamDestroy(memory[gpu].streams[peer]));
      }
      check(cudaFree(memory[gpu].source));
      check(cudaFree(memory[gpu].errors));
    }
  } catch (std::exception const& error) {
    std::cerr << error.what() << std::endl;
    return 1;
  }
}
