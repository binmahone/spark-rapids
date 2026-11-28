/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 */
#include <cudf/column/column_factories.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/copying.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/strings/strings_column_view.hpp>
#include <cudf/utilities/error.hpp>
#include <cudf/utilities/memory_resource.hpp>
#include <rmm/device_buffer.hpp>
#include <cuda/buffer>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <vector>

namespace cudf::strings::offset_experiment {
std::unique_ptr<column> concatenate(host_span<column_view const>, cuda::stream_ref,
                                  cudf::memory_resources);
}

namespace {
template <typename T>
std::vector<T> download(T const* pointer, size_t size, cuda::stream_ref stream)
{
  std::vector<T> result(size);
  if (size) {
    CUDF_CUDA_TRY(cudaMemcpyAsync(result.data(), pointer, size * sizeof(T),
                                 cudaMemcpyDeviceToHost, stream.get()));
  }
  stream.sync();
  return result;
}

void check_equal(cudf::column_view left, cudf::column_view right, cuda::stream_ref stream)
{
  if (left.size() != right.size() || left.null_count() != right.null_count()) {
    throw std::runtime_error("Result size or null count differs");
  }
  cudf::strings_column_view a(left), b(right);
  auto const offsets_a = a.offsets();
  auto const offsets_b = b.offsets();
  auto offset_values = [&](cudf::column_view col) {
    if (col.type().id() == cudf::type_id::INT64) {
      return download(col.head<int64_t>(), col.size(), stream);
    }
    auto const small = download(col.head<int32_t>(), col.size(), stream);
    return std::vector<int64_t>(small.begin(), small.end());
  };
  auto const oa = offset_values(offsets_a);
  auto const ob = offset_values(offsets_b);
  if (oa != ob) { throw std::runtime_error("String offsets differ"); }
  if (download(left.head<char>(), oa.back(), stream) !=
      download(right.head<char>(), ob.back(), stream)) {
    throw std::runtime_error("String contents differ");
  }
  if (left.has_nulls()) {
    auto const ma = download(left.null_mask(), (left.size() + 31) / 32, stream);
    auto const mb = download(right.null_mask(), (right.size() + 31) / 32, stream);
    for (int row = 0; row < left.size(); ++row) {
      if (((ma[row / 32] ^ mb[row / 32]) >> (row % 32)) & 1) {
        throw std::runtime_error("Null mask differs");
      }
    }
  }
}

template <typename Offset>
std::unique_ptr<cudf::column> make_input(size_t bytes, int seed, bool nulls,
                                        cuda::stream_ref stream)
{
  int const rows = static_cast<int>(bytes / 97) + 2;
  std::vector<Offset> offsets{0};
  std::vector<char> chars;
  std::vector<cudf::bitmask_type> mask((rows + 31) / 32, ~cudf::bitmask_type{0});
  int null_count = 0;
  for (int row = 0; row < rows; ++row) {
    bool const valid = !nulls || row % 13 != 5;
    if (!valid) { mask[row / 32] &= ~(cudf::bitmask_type{1} << (row % 32)); ++null_count; }
    int const length = valid && row % 17 != 3 ? 95 + (row + seed) % 5 : 0;
    chars.insert(chars.end(), length, static_cast<char>('a' + (row + seed) % 26));
    offsets.push_back(static_cast<Offset>(chars.size()));
  }
  auto offset_column = cudf::make_numeric_column(
    cudf::data_type{sizeof(Offset) == 8 ? cudf::type_id::INT64 : cudf::type_id::INT32},
    rows + 1, cudf::mask_state::UNALLOCATED, stream);
  CUDF_CUDA_TRY(cudaMemcpyAsync(offset_column->mutable_view().data<Offset>(), offsets.data(),
    offsets.size() * sizeof(Offset), cudaMemcpyHostToDevice, stream.get()));
  rmm::device_buffer data(chars.data(), chars.size(), stream);
  auto validity = cuda::device_buffer<cudf::bitmask_type>(stream,
    cudf::get_current_device_resource_ref(), nulls ? mask.size() : 0, cuda::no_init);
  if (nulls) {
    CUDF_CUDA_TRY(cudaMemcpyAsync(validity.data(), mask.data(), mask.size() * sizeof(mask[0]),
                                 cudaMemcpyHostToDevice, stream.get()));
  }
  stream.sync();
  return cudf::make_strings_column(rows, std::move(offset_column), std::move(data),
                                   null_count, std::move(validity));
}

template <typename Offset>
void run_case(int count, size_t bytes, bool nulls)
{
  auto const stream = cudf::get_default_stream();
  std::vector<std::unique_ptr<cudf::column>> inputs;
  std::vector<cudf::column_view> views;
  auto empty = cudf::make_empty_column(cudf::type_id::STRING);
  for (int i = 0; i < count; ++i) {
    inputs.push_back(make_input<Offset>(bytes + i * 37, i, nulls, stream));
    auto sliced = cudf::slice(inputs.back()->view(), {1, inputs.back()->size() - 1}, stream);
    views.push_back(sliced[0]);
    if (i == count / 2) { views.push_back(empty->view()); }
  }
  auto baseline = cudf::concatenate(views, stream);
  auto candidate = cudf::strings::offset_experiment::concatenate(
    views, stream, cudf::get_current_device_resource_ref());
  stream.sync();
  check_equal(baseline->view(), candidate->view(), stream);
  baseline.reset(); candidate.reset();
  for (int iteration = 0; iteration < 6; ++iteration) {
    for (int turn = 0; turn < 2; ++turn) {
      bool const changed = (iteration + turn) % 2 == 1;
      stream.sync();
      auto const begin = std::chrono::steady_clock::now();
      auto output = changed ? cudf::strings::offset_experiment::concatenate(
        views, stream, cudf::get_current_device_resource_ref()) : cudf::concatenate(views, stream);
      stream.sync();
      auto const seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
      std::cout << "{\"inputs\":" << count << ",\"input_bytes_target\":" << bytes
        << ",\"offset_bits\":" << sizeof(Offset) * 8 << ",\"nulls\":" << nulls
        << ",\"candidate\":" << changed << ",\"iteration\":" << iteration
        << ",\"seconds\":" << seconds << ",\"validation\":\"passed\"}" << std::endl;
    }
  }
}
}

int main()
{
  try {
    CUDF_CUDA_TRY(cudaSetDevice(0));
    run_case<int32_t>(8, 512 * 1024, false);
    run_case<int32_t>(8, 2 * 1024 * 1024, true);
    run_case<int64_t>(8, 512 * 1024, false);
    run_case<int32_t>(64, 2 * 1024 * 1024, false);
    run_case<int32_t>(256, 2 * 1024 * 1024, false);
  } catch (std::exception const& error) {
    std::cerr << error.what() << std::endl;
    return 1;
  }
}
