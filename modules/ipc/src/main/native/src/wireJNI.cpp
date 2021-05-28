/*
 * Copyright 2019-2021 tsurugi project.
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

#include <jni.h>
#include "com_nautilus_technologies_tsubakuro_impl_low_sql_WireImpl.h"
#include "wire.h"

using namespace tsubakuro::common;

JNIEXPORT jlong JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_WireImpl_openNative
(JNIEnv *env, [[maybe_unused]] jclass thisObj, jstring name)
{
    const char* name_ = env->GetStringUTFChars(name, NULL);
    if (name_ == NULL) return 0;
    jsize len_ = env->GetStringUTFLength(name);

    session_wire_container* container = new session_wire_container(std::string_view(name_, len_));
    env->ReleaseStringUTFChars(name, name_);
    return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(container));
}

JNIEXPORT jlong JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_WireImpl_sendNative
(JNIEnv *env, [[maybe_unused]] jclass thisObj, jlong handle, jbyteArray srcj)
{
    session_wire_container* container = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));

    jbyte *src = env->GetByteArrayElements(srcj, 0);
    jsize capacity = env->GetArrayLength(srcj);

    if (src == nullptr) {
        std::abort();
    }
    container->get_request_wire().write(src, capacity);
    env->ReleaseByteArrayElements(srcj, src, 0);
    return 0;  // FIXME
}


JNIEXPORT jbyteArray JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_WireImpl_recvNative
(JNIEnv *env, [[maybe_unused]] jclass thisObj, jlong handle)
{
    session_wire_container* container = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));
    auto& wire = container->get_response_wire();
    std::size_t length = wire.length();
    jbyteArray dstj = env->NewByteArray(length);
    if (dstj == NULL) {
        return NULL;
    }
    jbyte* dst = env->GetByteArrayElements(dstj, NULL);
    if (dst == NULL) {
        return NULL;
    }

    wire.read(dst, length);
    env->ReleaseByteArrayElements(dstj, dst, 0);
    return dstj;
}

JNIEXPORT jboolean JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_WireImpl_closeNative
([[maybe_unused]] JNIEnv *env, [[maybe_unused]] jclass thisObj, jlong handle)
{
    session_wire_container* container = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));

    delete container;
    return static_cast<jboolean>(true);
}
