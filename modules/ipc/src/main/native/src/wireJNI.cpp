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
#include "com_nautilus_technologies_tsubakuro_impl_low_sql_SessionWireImpl.h"
#include "com_nautilus_technologies_tsubakuro_impl_low_sql_ResultSetWireImpl.h"
#include "udf_wires.h"

using namespace tsubakuro::common::wire;

JNIEXPORT jlong JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_SessionWireImpl_openNative
(JNIEnv *env, jclass, jstring name)
{
    const char* name_ = env->GetStringUTFChars(name, NULL);
    if (name_ == NULL) return 0;
    jsize len_ = env->GetStringUTFLength(name);

    session_wire_container* container = new session_wire_container(std::string_view(name_, len_));
    env->ReleaseStringUTFChars(name, name_);
    return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(container));
}

JNIEXPORT jlong JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_SessionWireImpl_sendNative
(JNIEnv *env, jclass, jlong handle, jbyteArray srcj)
{
    session_wire_container* container = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));

    jbyte *src = env->GetByteArrayElements(srcj, 0);
    jsize length = env->GetArrayLength(srcj);

    if (src == nullptr) {
        std::abort();
    }
    session_wire_container::response *r = container->write(src, length);
    env->ReleaseByteArrayElements(srcj, src, 0);
    return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(r));
}

JNIEXPORT jbyteArray JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_SessionWireImpl_recvNative
(JNIEnv *env, jclass, jlong handle)
{
    session_wire_container::response *r = reinterpret_cast<session_wire_container::response*>(static_cast<std::uintptr_t>(handle));
    signed char* msg = r->read();

    if (msg == NULL) {
        return NULL;
    }

    std::size_t length = r->get_length();
    jbyteArray dstj = env->NewByteArray(length);
    if (dstj == NULL) {
        return NULL;
    }
    jbyte* dst = env->GetByteArrayElements(dstj, NULL);
    if (dst == NULL) {
        return NULL;
    }

    memcpy(dst, msg, length);
    env->ReleaseByteArrayElements(dstj, dst, 0);
    r->dispose();
    return dstj;
}

JNIEXPORT jboolean JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_SessionWireImpl_closeNative
(JNIEnv *, jclass, jlong handle)
{
    session_wire_container* container = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));

    if (container != nullptr) {
        delete container;
        return static_cast<jboolean>(true);
    }
    return static_cast<jboolean>(false);
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_impl_low_sql_ResultSetWireImpl
 * Method:    createNative
 * Signature: (JLjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_ResultSetWireImpl_createNative
(JNIEnv *env, jclass, jlong handle, jstring name)
{
    session_wire_container* c = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));

    const char* name_ = env->GetStringUTFChars(name, NULL);
    if (name_ == NULL) return 0;
    jsize len_ = env->GetStringUTFLength(name);

    session_wire_container::resultset_wire_container* container = c->create_resultset_wire(std::string_view(name_, len_));
    env->ReleaseStringUTFChars(name, name_);
    return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(container));
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_impl_low_sql_ResultSetWireImpl
 * Method:    recvMetaNative
 * Signature: (J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jbyteArray JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_ResultSetWireImpl_recvMetaNative
(JNIEnv *env, jclass, jlong handle)
{
    session_wire_container::resultset_wire_container* container = reinterpret_cast<session_wire_container::resultset_wire_container*>(static_cast<std::uintptr_t>(handle));

    std::size_t length = container->peep();
    jbyteArray dstj = env->NewByteArray(length);
    if (dstj == NULL) {
        return NULL;
    }
    jbyte* dst = env->GetByteArrayElements(dstj, NULL);
    if (dst == NULL) {
        return NULL;
    }
    container->recv(dst, length);
    env->ReleaseByteArrayElements(dstj, dst, 0);
    return dstj;
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_impl_low_sql_ResultSetWireImpl
 * Method:    closeNative
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_ResultSetWireImpl_closeNative
(JNIEnv *, jclass, jlong handle)
{
    session_wire_container::resultset_wire_container* container = reinterpret_cast<session_wire_container::resultset_wire_container*>(static_cast<std::uintptr_t>(handle));

    if (container != nullptr) {
        session_wire_container* envelope = container->get_envelope();
        envelope->dispose_resultset_wire(container);
        return static_cast<jboolean>(true);
    }
    return static_cast<jboolean>(false);
}
