/* Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 * Experimental Linux host-read adapter for immutable local benchmark data.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <dlfcn.h>
#include <fcntl.h>
#include <limits>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

namespace {
constexpr size_t max_request = 8 * 1024 * 1024;
constexpr size_t min_request = 1024 * 1024;
enum Counter { ELIGIBLE, BUFFERED, DIRECT, DIRECT_BYTES, COLD, RESIDENT,
               MIXED, INSPECT_FAIL, DIRECT_FAIL, REQUEST_BYTES, MIXED_DIRECT,
               PRESSURE_ACTIVE, PRESSURE_INACTIVE, PRESSURE_INVALID, COUNTERS };
uint64_t* counters = nullptr;
const char* prefix = nullptr;
bool enabled = false;
bool pressure_mode = false;
const uint64_t* pressure_word = nullptr;
long page_size = 0;
using Read = ssize_t (*)(int, void*, size_t, off64_t);

void count(Counter key, uint64_t value = 1)
{
  if (counters) __atomic_fetch_add(counters + key, value, __ATOMIC_RELAXED);
}

__attribute__((constructor)) void initialize()
{
  prefix = getenv("SCALEUP_COLD_READ_ROOT");
  const char* directory = getenv("SCALEUP_COLD_READ_STATS");
  const char* mode = getenv("SCALEUP_COLD_READ_MODE");
  if (!prefix || !directory || !mode || prefix[0] != '/' ||
      (strcmp(mode, "buffered") && strcmp(mode, "cold-direct") &&
       strcmp(mode, "pressure-direct"))) {
    prefix = nullptr;
    return;
  }
  pressure_mode = !strcmp(mode, "pressure-direct");
  enabled = !strcmp(mode, "cold-direct") || pressure_mode;
  if (pressure_mode) {
    const char* control = getenv("SCALEUP_COLD_READ_PRESSURE");
    const int control_fd = control ? open(control, O_RDONLY | O_CLOEXEC) : -1;
    struct stat control_info {};
    if (control_fd >= 0) {
      if (!fstat(control_fd, &control_info) && S_ISREG(control_info.st_mode) &&
          control_info.st_uid == geteuid() && control_info.st_size == static_cast<off_t>(sizeof(uint64_t))) {
        void* mapped = mmap(nullptr, sizeof(uint64_t), PROT_READ, MAP_SHARED, control_fd, 0);
        if (mapped != MAP_FAILED) pressure_word = static_cast<const uint64_t*>(mapped);
      }
      close(control_fd);
    }
  }
  page_size = sysconf(_SC_PAGESIZE);
  if (page_size != 4096) { prefix = nullptr; return; }
  char path[4096];
  const int n = snprintf(path, sizeof(path), "%s/%ld.bin", directory, long(getpid()));
  if (n < 0 || size_t(n) >= sizeof(path)) { prefix = nullptr; return; }
  const int fd = open(path, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) { prefix = nullptr; return; }
  if (ftruncate(fd, COUNTERS * sizeof(uint64_t)) == 0) {
    void* p = mmap(nullptr, COUNTERS * sizeof(uint64_t), PROT_READ | PROT_WRITE,
                   MAP_SHARED, fd, 0);
    if (p != MAP_FAILED) counters = static_cast<uint64_t*>(p);
  }
  close(fd);
  if (!counters) { prefix = nullptr; return; }
  fprintf(stderr, "SCALEUP_COLD_READ mode=%s pid=%ld root=%s\n", mode, long(getpid()), prefix);
}

bool pressure_allows_direct()
{
  struct timespec now {};
  if (!pressure_word || clock_gettime(CLOCK_MONOTONIC, &now)) {
    count(PRESSURE_INVALID);
    return false;
  }
  const uint64_t word = __atomic_load_n(pressure_word, __ATOMIC_ACQUIRE);
  const uint64_t stamp = word >> 1;
  const uint64_t now_ms = uint64_t(now.tv_sec) * 1000 + now.tv_nsec / 1000000;
  // An absent, future, or stale heartbeat preserves the original buffered path.
  if (!stamp || stamp > now_ms || now_ms - stamp > 2000) {
    count(PRESSURE_INVALID);
    return false;
  }
  const bool active = word & 1;
  count(active ? PRESSURE_ACTIVE : PRESSURE_INACTIVE);
  return active;
}

struct Bounce {
  void* address = MAP_FAILED;
  size_t capacity = 0;
  ~Bounce() { if (address != MAP_FAILED) munmap(address, capacity); }
  bool reserve(size_t size)
  {
    if (capacity >= size) return true;
    if (address != MAP_FAILED) munmap(address, capacity);
    // Shared anonymous memory avoids the private-buffer direct-I/O/fork restriction.
    address = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    capacity = address == MAP_FAILED ? 0 : size;
    return capacity != 0;
  }
};

bool eligible(int fd, size_t bytes, off64_t offset, char* fd_path, struct stat& info)
{
  if (!prefix || bytes < min_request || bytes > max_request || offset < 0) return false;
  char name[16] = {};
  if (prctl(PR_GET_NAME, name) || strncmp(name, "kvikio-", 7)) return false;
  snprintf(fd_path, 64, "/proc/self/fd/%d", fd);
  char path[4096];
  const ssize_t n = readlink(fd_path, path, sizeof(path) - 1);
  if (n < 0 || size_t(n) >= sizeof(path) - 1) return false;
  path[n] = 0;
  const size_t root_length = strlen(prefix);
  if (strncmp(path, prefix, root_length) || path[root_length] != '/' ||
      n < 8 || strcmp(path + n - 8, ".parquet")) return false;
  const int flags = fcntl(fd, F_GETFL);
  if (flags < 0 || (flags & O_ACCMODE) != O_RDONLY || (flags & O_DIRECT)) return false;
  if (fstat(fd, &info) || !S_ISREG(info.st_mode) || info.st_uid != geteuid() ||
      offset > info.st_size || bytes > uint64_t(info.st_size - offset)) return false;
  return true;
}

ssize_t read_request(Read real, int fd, void* dst, size_t bytes, off64_t offset)
{
  const int incoming_errno = errno;
  char fd_path[64];
  struct stat info {};
  if (!eligible(fd, bytes, offset, fd_path, info)) {
    errno = incoming_errno;
    return real(fd, dst, bytes, offset);
  }
  count(ELIGIBLE); count(REQUEST_BYTES, bytes);
  auto buffered = [&]() {
    count(BUFFERED);
    errno = incoming_errno;
    return real(fd, dst, bytes, offset);
  };
  if (!enabled) return buffered();
  if (pressure_mode && !pressure_allows_direct()) return buffered();
  const off64_t map_offset = offset / page_size * page_size;
  const size_t map_length = size_t(offset - map_offset) + bytes;
  unsigned char residency[max_request / 4096 + 2];
  void* mapping = mmap(nullptr, map_length, PROT_NONE, MAP_SHARED, fd, map_offset);
  if (mapping == MAP_FAILED) { count(INSPECT_FAIL); return buffered(); }
  const int status = mincore(mapping, map_length, residency);
  munmap(mapping, map_length);
  if (status) { count(INSPECT_FAIL); return buffered(); }
  const size_t pages = (map_length + page_size - 1) / page_size;
  size_t present = 0;
  for (size_t i = 0; i < pages; ++i) present += residency[i] & 1;
  if (present == pages) {
    count(RESIDENT);
    return buffered();
  }
  count(present ? MIXED : COLD);
  struct Range { size_t begin, end; } ranges[max_request / min_request + 1];
  size_t range_count = 0;
  const size_t request_begin = size_t(offset - map_offset);
  const size_t request_end = request_begin + bytes;
  // Read large absent spans directly. Small holes share the buffered runs between them.
  for (size_t i = 0; i < pages;) {
    if (residency[i] & 1) { ++i; continue; }
    const size_t start = i;
    while (i < pages && !(residency[i] & 1)) ++i;
    const size_t begin = std::max(start * size_t(page_size), request_begin);
    const size_t end = std::min(i * size_t(page_size), request_end);
    if (end - begin >= min_request) ranges[range_count++] = {begin, end};
  }
  if (!range_count) return buffered();
  // A residency snapshot controls cost only; immutable file contents define correctness.
  struct statx alignment {};
  if (statx(fd, "", AT_EMPTY_PATH, STATX_DIOALIGN, &alignment) ||
      !(alignment.stx_mask & STATX_DIOALIGN) || !alignment.stx_dio_offset_align ||
      !alignment.stx_dio_mem_align || alignment.stx_dio_mem_align > size_t(page_size) ||
      page_size % alignment.stx_dio_mem_align ||
      alignment.stx_dio_offset_align > size_t(page_size) ||
      page_size % alignment.stx_dio_offset_align) {
    count(DIRECT_FAIL); return buffered();
  }
  const size_t length = pages * size_t(page_size);
  thread_local Bounce bounce;
  if (!bounce.reserve(length)) { count(DIRECT_FAIL); return buffered(); }
  // Reopen the descriptor's inode, not the pathname which could have been replaced.
  const int direct_fd = open(fd_path, O_RDONLY | O_DIRECT | O_CLOEXEC);
  if (direct_fd < 0) { count(DIRECT_FAIL); return buffered(); }
  struct stat direct_info {};
  if (fstat(direct_fd, &direct_info) || direct_info.st_ino != info.st_ino ||
      direct_info.st_dev != info.st_dev) {
    close(direct_fd); count(DIRECT_FAIL); return buffered();
  }
  bool succeeded = true;
  size_t direct_bytes = 0;
  for (size_t i = 0; i < range_count && succeeded; ++i) {
    const size_t begin = ranges[i].begin / page_size * page_size;
    const size_t end = (ranges[i].end + page_size - 1) / page_size * page_size;
    const ssize_t got = real(direct_fd, static_cast<char*>(bounce.address) + begin,
                             end - begin, map_offset + begin);
    succeeded = got >= 0 && size_t(got) >= ranges[i].end - begin;
    direct_bytes += ranges[i].end - ranges[i].begin;
  }
  close(direct_fd);
  // Do not expose a partial direct read. The original read handles EOF and errors.
  if (!succeeded) { count(DIRECT_FAIL); return buffered(); }
  size_t cursor = request_begin;
  for (size_t i = 0; i <= range_count; ++i) {
    const size_t end = i == range_count ? request_end : ranges[i].begin;
    if (end > cursor) {
      const ssize_t got = real(fd, static_cast<char*>(bounce.address) + cursor,
                               end - cursor, map_offset + cursor);
      if (got < 0 || size_t(got) != end - cursor) { count(DIRECT_FAIL); return buffered(); }
    }
    if (i < range_count) cursor = ranges[i].end;
  }
  memcpy(dst, static_cast<char*>(bounce.address) + request_begin, bytes);
  count(DIRECT); count(DIRECT_BYTES, direct_bytes);
  if (present) count(MIXED_DIRECT);
  errno = incoming_errno;
  return bytes;
}
}  // namespace

extern "C" ssize_t pread64(int fd, void* dst, size_t bytes, off64_t offset)
{
  static const auto real = reinterpret_cast<Read>(dlsym(RTLD_NEXT, "pread64"));
  if (!real) { errno = ENOSYS; return -1; }
  return read_request(real, fd, dst, bytes, offset);
}

extern "C" ssize_t pread(int fd, void* dst, size_t bytes, off_t offset)
{
  static_assert(sizeof(off_t) == sizeof(off64_t));
  static const auto real = reinterpret_cast<Read>(dlsym(RTLD_NEXT, "pread"));
  if (!real) { errno = ENOSYS; return -1; }
  return read_request(real, fd, dst, bytes, offset);
}
