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

// Diagnostic preload observer: records in memory and writes only during process shutdown.
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <fcntl.h>
#include <new>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

namespace {
struct Record {
  uint64_t kind, call, task, tid, fd, offset, bytes, result, buffer, begin, end, cpu, flags, file;
};
static_assert(sizeof(Record) == 112);
constexpr uint64_t capacity = 262144;
std::atomic<uint64_t> next{0};
std::atomic<uint64_t> calls{1};
Record* records = nullptr;
std::atomic<bool> selected{false};
thread_local bool inside = false;
thread_local uint64_t active_call = 0, active_task = 0;
char hostname[256]{};
uint64_t base_mono = 0, base_real = 0;

uint64_t nanos(clockid_t clock = CLOCK_MONOTONIC)
{
  timespec value{};
  clock_gettime(clock, &value);
  return uint64_t(value.tv_sec) * 1000000000ULL + value.tv_nsec;
}

void append(Record record)
{
  if (!selected || records == nullptr) { return; }
  auto const index = next.fetch_add(1, std::memory_order_relaxed);
  if (index >= capacity) { return; }
  auto const kind = record.kind;
  record.kind = 0;
  records[index] = record;
  std::atomic_ref<uint64_t>(records[index].kind).store(kind, std::memory_order_release);
}

__attribute__((constructor)) void initialize()
{
  auto const* host = std::getenv("RAPIDS_IO_TRACE_HOST");
  auto const* gpu = std::getenv("CUDA_VISIBLE_DEVICES");
  auto const* wanted_gpu = std::getenv("RAPIDS_IO_TRACE_GPU");
  if (!wanted_gpu) { wanted_gpu = "0"; }
  auto const* directory = std::getenv("RAPIDS_IO_TRACE_DIR");
  gethostname(hostname, sizeof(hostname) - 1);
  if (!host || !gpu || !directory || std::strcmp(host, hostname) || std::strcmp(gpu, wanted_gpu)) { return; }
  auto* allocation = mmap(nullptr, capacity * sizeof(Record), PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (allocation == MAP_FAILED) { return; }
  records = new (allocation) Record[capacity];
  base_mono = nanos();
  base_real = nanos(CLOCK_REALTIME);
  selected = true;
}

__attribute__((destructor)) void finish()
{
  if (!selected || records == nullptr || next.load() == 0) { return; }
  selected = false;
  char path[4096];
  std::snprintf(path, sizeof(path), "%s/%s-%d.bin", std::getenv("RAPIDS_IO_TRACE_DIR"), hostname, getpid());
  auto* output = std::fopen(path, "wb");
  if (!output) { return; }
  auto const count = next.load();
  uint64_t header[] = {1, 0, count > capacity ? count - capacity : 0, sizeof(Record), base_mono, base_real};
  std::fwrite("RAPIDIO1", 8, 1, output);
  std::fwrite(header, sizeof(header), 1, output);
  // Keep the mapping alive until process exit so late teardown cannot access freed storage.
  for (uint64_t i = 0; i < count && i < capacity; ++i) {
    if (std::atomic_ref<uint64_t>(records[i].kind).load(std::memory_order_acquire) == 0) { continue; }
    std::fwrite(records + i, sizeof(Record), 1, output);
    ++header[1];
  }
  std::fseek(output, 16, SEEK_SET);
  std::fwrite(header + 1, sizeof(uint64_t), 1, output);
  std::fclose(output);
}

template <typename Function, typename Offset>
ssize_t observe(Function function, int fd, void* buffer, size_t bytes, Offset offset)
{
  if (!selected || inside) { return function(fd, buffer, bytes, offset); }
  auto const original_errno = errno;
  inside = true;
  char link[64], path[4096];
  std::snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
  auto const length = readlink(link, path, sizeof(path) - 1);
  auto const* prefix = std::getenv("RAPIDS_IO_TRACE_PREFIX");
  if (length < 0 || prefix == nullptr) {
    inside = false;
    errno = original_errno;
    return function(fd, buffer, bytes, offset);
  }
  path[length] = '\0';
  if (std::strncmp(path, prefix, std::strlen(prefix))) {
    inside = false;
    errno = original_errno;
    return function(fd, buffer, bytes, offset);
  }
  auto const flags = fcntl(fd, F_GETFL);
  auto const file = std::strtoull(path + std::strlen(prefix), nullptr, 10);
  errno = original_errno;
  auto const cpu_start = nanos(CLOCK_THREAD_CPUTIME_ID);
  auto const start = nanos();
  auto const result = function(fd, buffer, bytes, offset);
  auto const saved_errno = errno;
  auto const end = nanos();
  auto const cpu = nanos(CLOCK_THREAD_CPUTIME_ID) - cpu_start;
  append({6, active_call, active_task, uint64_t(syscall(SYS_gettid)), uint64_t(fd), uint64_t(offset),
          bytes, uint64_t(result), uint64_t(buffer), start, end, cpu, uint64_t(flags), file});
  inside = false;
  errno = saved_errno;
  return result;
}
}  // namespace

extern "C" uint64_t rapids_io_trace_note(uint64_t kind, uint64_t call, uint64_t task, int fd,
                                        void const* buffer, uint64_t offset, uint64_t bytes)
{
  if (!selected) { return 0; }
  if (kind == 1) { call = calls.fetch_add(1); }
  if (kind == 3) { active_call = call; active_task = task; }
  auto const now = nanos();
  append({kind, call, task, uint64_t(syscall(SYS_gettid)), uint64_t(fd), offset, bytes, 0,
          uint64_t(buffer), now, now, 0, 0, 0});
  if (kind == 4) { active_call = active_task = 0; }
  return call;
}

extern "C" ssize_t pread(int fd, void* buffer, size_t bytes, off_t offset)
{
  using Function = ssize_t (*)(int, void*, size_t, off_t);
  static auto real = reinterpret_cast<Function>(dlsym(RTLD_NEXT, "pread"));
  return observe(real, fd, buffer, bytes, offset);
}

extern "C" ssize_t pread64(int fd, void* buffer, size_t bytes, off64_t offset)
{
  using Function = ssize_t (*)(int, void*, size_t, off64_t);
  static auto real = reinterpret_cast<Function>(dlsym(RTLD_NEXT, "pread64"));
  return observe(real, fd, buffer, bytes, offset);
}
