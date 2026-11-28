/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
#include <cuda.h>
#include <jni.h>
#include <jvmti.h>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>

namespace {
using sync_method = void(JNICALL*)(JNIEnv*, jclass, jlong);
std::atomic<sync_method> original{nullptr};
std::atomic<unsigned long> calls{0};
unsigned expected_flags;

void require(bool condition, char const* message)
{
  if (!condition) {
    std::fprintf(stderr, "CUDA_WAIT_POLICY_FAILED %s\n", message);
    std::fflush(stderr);
    std::abort();
  }
}

void check(CUresult status)
{
  if (status != CUDA_SUCCESS) {
    char const* name = nullptr;
    cuGetErrorName(status, &name);
    require(false, name == nullptr ? "CUDA error" : name);
  }
}

void JNICALL checked_sync(JNIEnv* env, jclass cls, jlong stream)
{
  auto const function = original.load();
  require(function != nullptr, "Missing original sync method");
  function(env, cls, stream);
  if (env->ExceptionCheck()) { return; }
  unsigned flags;
  CUcontext context;
  check(cuCtxGetCurrent(&context));
  require(context != nullptr, "Synchronization returned without a current context");
  check(cuCtxGetFlags(&flags));
  require((flags & CU_CTX_SCHED_MASK) == expected_flags, "Executed context flags differ");
  if (calls.fetch_add(1) == 0) {
    std::fprintf(stderr, "CUDA_WAIT_POLICY_EXECUTED flags=%u context=%p stream=%lld\n",
                 flags, static_cast<void*>(context), static_cast<long long>(stream));
    std::fflush(stderr);
  }
}

void JNICALL bound(jvmtiEnv* jvmti, JNIEnv*, jthread, jmethodID method,
                   void* address, void** replacement)
{
  char *name = nullptr, *signature = nullptr, *class_name = nullptr;
  if (jvmti->GetMethodName(method, &name, &signature, nullptr) != JVMTI_ERROR_NONE) { return; }
  bool const matches = std::strcmp(name, "streamSynchronize") == 0 &&
                       std::strcmp(signature, "(J)V") == 0;
  jvmti->Deallocate(reinterpret_cast<unsigned char*>(name));
  jvmti->Deallocate(reinterpret_cast<unsigned char*>(signature));
  if (!matches) { return; }
  jclass declaring;
  require(jvmti->GetMethodDeclaringClass(method, &declaring) == JVMTI_ERROR_NONE,
          "Cannot identify native method class");
  require(jvmti->GetClassSignature(declaring, &class_name, nullptr) == JVMTI_ERROR_NONE,
          "Cannot identify class signature");
  bool const target = std::strcmp(class_name, "Lai/rapids/cudf/Cuda;") == 0;
  jvmti->Deallocate(reinterpret_cast<unsigned char*>(class_name));
  if (!target) { return; }
  require(original.load() == nullptr, "Repeated sync method binding");
  original.store(reinterpret_cast<sync_method>(address));
  *replacement = reinterpret_cast<void*>(&checked_sync);
  std::fprintf(stderr, "CUDA_WAIT_POLICY_BOUND ai/rapids/cudf/Cuda.streamSynchronize(J)V\n");
}
}  // namespace

extern "C" JNIEXPORT jint JNICALL Agent_OnLoad(JavaVM* vm, char* options, void*)
{
  std::string mode = options == nullptr ? "" : options;
  bool const smoke = mode.rfind("smoke-", 0) == 0;
  if (smoke) { mode.erase(0, 6); }
  require(mode == "auto" || mode == "blocking", "Unknown wait policy");
  std::ifstream command("/proc/self/cmdline", std::ios::binary);
  std::string argument;
  bool executor = false;
  while (std::getline(command, argument, '\0')) {
    executor |= argument == "org.apache.spark.executor.CoarseGrainedExecutorBackend";
  }
  if (!executor && !smoke) {
    std::fprintf(stderr, "CUDA_WAIT_POLICY_SKIPPED non-executor\n");
    return JNI_OK;
  }
  check(cuInit(0));
  int count;
  check(cuDeviceGetCount(&count));
  require(count == 1, "Expected exactly one visible GPU");
  CUdevice device;
  check(cuDeviceGet(&device, 0));
  unsigned before, after;
  int active;
  check(cuDevicePrimaryCtxGetState(device, &before, &active));
  require(active == 0 && (before & CU_CTX_SCHED_MASK) == CU_CTX_SCHED_AUTO,
          "Expected an inactive primary context with default scheduling");
  expected_flags = mode == "blocking" ? CU_CTX_SCHED_BLOCKING_SYNC : CU_CTX_SCHED_AUTO;
  check(cuDevicePrimaryCtxSetFlags(device, (before & ~CU_CTX_SCHED_MASK) | expected_flags));
  check(cuDevicePrimaryCtxGetState(device, &after, &active));
  require(active == 0 && (after & CU_CTX_SCHED_MASK) == expected_flags,
          "Primary context configuration differs");
  jvmtiEnv* jvmti = nullptr;
  require(vm->GetEnv(reinterpret_cast<void**>(&jvmti), JVMTI_VERSION_1_2) == JNI_OK,
          "JVMTI unavailable");
  jvmtiCapabilities capabilities{};
  capabilities.can_generate_native_method_bind_events = 1;
  require(jvmti->AddCapabilities(&capabilities) == JVMTI_ERROR_NONE, "Native binding unavailable");
  jvmtiEventCallbacks callbacks{};
  callbacks.NativeMethodBind = bound;
  require(jvmti->SetEventCallbacks(&callbacks, sizeof(callbacks)) == JVMTI_ERROR_NONE,
          "Cannot install binding callback");
  require(jvmti->SetEventNotificationMode(JVMTI_ENABLE, JVMTI_EVENT_NATIVE_METHOD_BIND, nullptr) ==
          JVMTI_ERROR_NONE, "Cannot enable binding callback");
  std::fprintf(stderr, "CUDA_WAIT_POLICY_CONFIGURED mode=%s before=%u after=%u active=%d\n",
               mode.c_str(), before, after, active);
  std::fflush(stderr);
  return JNI_OK;
}

extern "C" JNIEXPORT void JNICALL Agent_OnUnload(JavaVM*)
{
  std::fprintf(stderr, "CUDA_WAIT_POLICY_COUNTS checks=%lu\n", calls.load());
}
