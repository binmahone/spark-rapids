/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 */
#include "copy_admission.hpp"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>

int main(int argc, char **argv) {
  try {
    if (argc != 5) {
      throw std::runtime_error("Expected lock, ready, release, and mode");
    }
    file_copy_lease lease(argv[1]);
    std::ofstream(argv[2]) << "acquired\n";
    if (std::string(argv[4]) == "throw") {
      throw std::runtime_error("injected failure");
    }
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!std::filesystem::exists(argv[3])) {
      if (std::chrono::steady_clock::now() > deadline) {
        throw std::runtime_error("release timeout");
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  } catch (std::exception const &error) {
    std::cerr << error.what() << std::endl;
    if (argc == 5 && std::string(argv[4]) == "throw") {
      std::ofstream(std::string(argv[2]) + ".unwound") << "unwound\n";
      auto deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(10);
      while (!std::filesystem::exists(argv[3]) &&
             std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }
    }
    return 1;
  }
}
