/* Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 * Test-only direct-read error injection; excluded from benchmark deployment.
 */
#include <cerrno>
#include <cstdlib>
#include <dlfcn.h>
#include <fcntl.h>
#include <unistd.h>

extern "C" ssize_t pread64(int fd, void* dst, size_t bytes, off64_t offset)
{
  static const auto real = reinterpret_cast<decltype(&pread64)>(dlsym(RTLD_NEXT, "pread64"));
  const int flags = fcntl(fd, F_GETFL);
  if (flags >= 0 && (flags & O_DIRECT)) {
    if (getenv("SCALEUP_TEST_SHORT_READ")) return 0;
    errno = EIO;
    return -1;
  }
  return real(fd, dst, bytes, offset);
}
