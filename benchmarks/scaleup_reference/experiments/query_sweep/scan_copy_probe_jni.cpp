/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 */
#include "cudf_jni_apis.hpp"
#include "probe_allocator.hpp"
#include "scan_copy_admission.hpp"

namespace scan_copy_experiment {
int admission_mode = 0;
std::string admission_path;
} // namespace scan_copy_experiment

extern "C" {
jlongArray Java_com_nvidia_spark_rapids_jni_PackedHostToDeviceCopy_copyAsync(
    JNIEnv *, jclass, jlongArray, jlongArray, jlong, jlong);
void Java_com_nvidia_spark_rapids_jni_PackedHostToDeviceCopy_setFreeStream(
    JNIEnv *, jclass, jlongArray, jlong);
void Java_com_nvidia_spark_rapids_jni_PackedHostToDeviceCopy_freePack(JNIEnv *,
                                                                      jclass,
                                                                      jlong);

JNIEXPORT void JNICALL
Java_com_nvidia_spark_rapids_experiments_ScanCopyProbe_install(
    JNIEnv *env, jclass, jclass type, jint mode, jstring path) {
  JNI_NULL_CHECK(env, path, "lease path is null", );
  JNI_TRY {
    verify_shared_probe_allocator();
    CUDF_EXPECTS(mode >= 0 && mode <= 6, "Unknown copy admission mode");
    cudf::jni::native_jstring lease_path(env, path);
    scan_copy_experiment::admission_path = lease_path.get();
    scan_copy_experiment::admission_mode = mode;
    JNINativeMethod methods[]{
        {const_cast<char *>("copyAsync"), const_cast<char *>("([J[JJJ)[J"),
         reinterpret_cast<void *>(
             &Java_com_nvidia_spark_rapids_jni_PackedHostToDeviceCopy_copyAsync)},
        {const_cast<char *>("setFreeStream"), const_cast<char *>("([JJ)V"),
         reinterpret_cast<void *>(
             &Java_com_nvidia_spark_rapids_jni_PackedHostToDeviceCopy_setFreeStream)},
        {const_cast<char *>("freePack"), const_cast<char *>("(J)V"),
         reinterpret_cast<void *>(
             &Java_com_nvidia_spark_rapids_jni_PackedHostToDeviceCopy_freePack)}};
    CUDF_EXPECTS(env->RegisterNatives(type, methods, 3) == JNI_OK,
                 "Unable to register scan-copy probe");
  }
  JNI_CATCH(env, );
}
}
