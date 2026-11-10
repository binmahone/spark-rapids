// Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Diagnostic-only preload observer. It does not schedule, delay, or alter reads.
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <fcntl.h>
#include <limits.h>
#include <mutex>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

namespace {
std::mutex output_mutex;
std::atomic<unsigned long> sequence{0};
thread_local bool observing = false;

long long nanos()
{
  timespec ts{};
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<long long>(ts.tv_sec) * 1000000000LL + ts.tv_nsec;
}

bool matches(int fd)
{
  auto const* prefix = std::getenv("KVIKIO_TRACE_PREFIX");
  if (prefix == nullptr) { return false; }
  char link[64];
  char path[PATH_MAX];
  std::snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
  auto n = readlink(link, path, sizeof(path) - 1);
  if (n < 0) { return false; }
  path[n] = '\0';
  return std::strncmp(path, prefix, std::strlen(prefix)) == 0;
}

template <typename Offset, typename Function>
ssize_t observe(Function function, int fd, void* buffer, size_t count, Offset offset)
{
  if (observing || !matches(fd)) { return function(fd, buffer, count, offset); }
  observing = true;
  auto start = nanos();
  auto result = function(fd, buffer, count, offset);
  auto end = nanos();
  auto saved_errno = errno;
  if (auto const* output = std::getenv("KVIKIO_TRACE_OUTPUT")) {
    std::lock_guard<std::mutex> guard(output_mutex);
    int log = open(output, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0600);
    if (log >= 0) {
      char line[512];
      auto length = std::snprintf(
        line, sizeof(line),
        "{\"sequence\":%lu,\"pid\":%d,\"tid\":%ld,\"fd\":%d,\"start_ns\":%lld,"
        "\"end_ns\":%lld,\"offset\":%lld,\"requested\":%zu,\"returned\":%zd}\n",
        sequence.fetch_add(1), getpid(), syscall(SYS_gettid), fd, start, end,
        static_cast<long long>(offset), count, result);
      if (length > 0 && static_cast<size_t>(length) < sizeof(line)) {
        auto ignored = write(log, line, length);
        (void)ignored;
      }
      close(log);
    }
  }
  observing = false;
  errno = saved_errno;
  return result;
}
}  // namespace

extern "C" ssize_t pread(int fd, void* buffer, size_t count, off_t offset)
{
  using Function = ssize_t (*)(int, void*, size_t, off_t);
  static auto real = reinterpret_cast<Function>(dlsym(RTLD_NEXT, "pread"));
  return observe(real, fd, buffer, count, offset);
}

extern "C" ssize_t pread64(int fd, void* buffer, size_t count, off64_t offset)
{
  using Function = ssize_t (*)(int, void*, size_t, off64_t);
  static auto real = reinterpret_cast<Function>(dlsym(RTLD_NEXT, "pread64"));
  return observe(real, fd, buffer, count, offset);
}
