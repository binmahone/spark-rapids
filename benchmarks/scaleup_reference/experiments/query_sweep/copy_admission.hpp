/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 */
#pragma once

#include <cerrno>
#include <fcntl.h>
#include <string>
#include <sys/file.h>
#include <system_error>
#include <unistd.h>

class file_copy_lease {
public:
  explicit file_copy_lease(std::string const &path) {
    descriptor_ = open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
    if (descriptor_ < 0) {
      throw std::system_error(errno, std::generic_category());
    }
    while (flock(descriptor_, LOCK_EX) != 0) {
      if (errno == EINTR) {
        continue;
      }
      auto const error = errno;
      close(descriptor_);
      descriptor_ = -1;
      throw std::system_error(error, std::generic_category());
    }
  }
  ~file_copy_lease() {
    if (descriptor_ >= 0) {
      close(descriptor_);
    }
  }
  file_copy_lease(file_copy_lease const &) = delete;
  file_copy_lease &operator=(file_copy_lease const &) = delete;

private:
  int descriptor_ = -1;
};
