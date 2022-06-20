/*
 * Copyright 2019-2022 tsurugi project.
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
#include "com_nautilus_technologies_tsubakuro_channel_ipc_SessionWireImpl.h"
#include "com_nautilus_technologies_tsubakuro_channel_ipc_sql_ResultSetWireImpl.h"
#include "udf_wires.h"

using namespace tateyama::common::wire;

/*
 * Class:     com_nautilus_technologies_tsubakuro_channel_ipc_sql_SessionWireImpl
 * Method:    openNative
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_nautilus_1technologies_tsubakuro_channel_ipc_SessionWireImpl_openNative
(JNIEnv *env, jclass, jstring name)
{
    session_wire_container* swc;
    const char* name_ = env->GetStringUTFChars(name, nullptr);
    if (name_ == nullptr) return 0;
    jsize len_ = env->GetStringUTFLength(name);

    try {
        swc = new session_wire_container(std::string_view(name_, len_));
    } catch (std::runtime_error &e) {
        env->ReleaseStringUTFChars(name, name_);
        jclass classj = env->FindClass("Ljava/io/IOException;");
        if (classj == nullptr) { std::abort(); }
        env->ThrowNew(classj, e.what());
        env->DeleteLocalRef(classj);
        return 0;
    }
    env->ReleaseStringUTFChars(name, name_);
    return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(swc));
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_channel_ipc_SessionWireImpl
 * Method:    getResponseHandleNative
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_nautilus_1technologies_tsubakuro_channel_ipc_SessionWireImpl_getResponseHandleNative
  (JNIEnv *, jclass, jlong handle)
{
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));

    return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(swc->get_response_box()));
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_channel_ipc_SessionWireImpl
 * Method:    sendNative
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_channel_ipc_SessionWireImpl_sendNative__J_3B
(JNIEnv *env, jclass, jlong handle, jbyteArray array)
{
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));

    auto address = env->GetByteArrayElements(array, nullptr);
    swc->write(static_cast<signed char*>(address), env->GetArrayLength(array));
    env->ReleaseByteArrayElements(array, address, JNI_ABORT);
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_channel_ipc_SessionWireImpl
 * Method:    sendNative
 * Signature: (JLjava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_channel_ipc_SessionWireImpl_sendNative__JLjava_nio_ByteBuffer_2
(JNIEnv *env, jclass, jlong handle, jobject buf)
{
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));

    swc->write(static_cast<signed char*>(env->GetDirectBufferAddress(buf)), env->GetDirectBufferCapacity(buf));
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_channel_ipc_SessionWireImpl
 * Method:    setQueryModeNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_channel_ipc_SessionWireImpl_setQueryModeNative
(JNIEnv *, jclass, jlong responseHandle)
{
    response_box::response *r = reinterpret_cast<response_box::response*>(static_cast<std::uintptr_t>(responseHandle));
    r->set_query_mode();
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_channel_ipc_SessionWireImpl
 * Method:    flushNative
 * Signature: (JJZ)V
 */
JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_channel_ipc_SessionWireImpl_flushNative
(JNIEnv *, jclass, jlong handle)
{
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));
    swc->flush();
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_channel_ipc_SessionWireImpl
 * Method:    receiveNative
 * Signature: (J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_com_nautilus_1technologies_tsubakuro_channel_ipc_SessionWireImpl_receiveNative__J
(JNIEnv *env, jclass, jlong handle)
{
    response_box::response *r = reinterpret_cast<response_box::response*>(static_cast<std::uintptr_t>(handle));

    auto b = r->recv();
    return env->NewDirectByteBuffer(b.first, b.second);
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_channel_ipc_SessionWireImpl
 * Method:    receiveNative
 * Signature: (JJ)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_com_nautilus_1technologies_tsubakuro_channel_ipc_SessionWireImpl_receiveNative__JJ
(JNIEnv *env, jclass, jlong handle, jlong timeout)
{
    response_box::response *r = reinterpret_cast<response_box::response*>(static_cast<std::uintptr_t>(handle));

    try {
        auto b = r->recv(timeout);
        return env->NewDirectByteBuffer(b.first, b.second);
    } catch (std::runtime_error &e) {
        jclass classj = env->FindClass("Ljava/util/concurrent/TimeoutException;");
        if (classj == nullptr) { std::abort(); }
        env->ThrowNew(classj, e.what());
        env->DeleteLocalRef(classj);
        return nullptr;
    }
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_channel_ipc_SessionWireImpl
 * Method:    unReceiveNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_channel_ipc_SessionWireImpl_unReceiveNative
  (JNIEnv *, jclass, jlong handle)
{
    response_box::response *r = reinterpret_cast<response_box::response*>(static_cast<std::uintptr_t>(handle));
    r->un_receive();
}

pthread_mutex_t release_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * Class:     com_nautilus_technologies_tsubakuro_channel_ipc_SessionWireImpl
 * Method:    releaseNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_channel_ipc_SessionWireImpl_releaseNative
(JNIEnv *, jclass, jlong handle)
{
    response_box::response *r = reinterpret_cast<response_box::response*>(static_cast<std::uintptr_t>(handle));

    if (int ret = pthread_mutex_lock(&release_mutex); ret != 0) {
        std::abort();
    }
    r->dispose();
    if (int ret = pthread_mutex_unlock(&release_mutex); ret != 0) {
        std::abort();
    }
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_channel_ipc_SessionWireImpl
 * Method:    closeNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_channel_ipc_SessionWireImpl_closeNative
(JNIEnv *, jclass, jlong handle)
{
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));

    if (swc != nullptr) {
        delete swc;
    }
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_channel_ipc_sql_ResultSetWireImpl
 * Method:    createNative
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_nautilus_1technologies_tsubakuro_channel_ipc_sql_ResultSetWireImpl_createNative
(JNIEnv *env, jclass, jlong handle)
{
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));

    session_wire_container::resultset_wires_container* rwc;
    try {
        rwc = swc->create_resultset_wire();
    } catch (std::runtime_error &e) {
        jclass classj = env->FindClass("Ljava/io/IOException;");
        if (classj == nullptr) { std::abort(); }
        env->ThrowNew(classj, e.what());
        env->DeleteLocalRef(classj);
    }

    return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(rwc));
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_channel_ipc_sql_ResultSetWireImpl
 * Method:    connectNative
 * Signature: (JLjava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_channel_ipc_sql_ResultSetWireImpl_connectNative
(JNIEnv *env, jclass, jlong handle, jstring name)
{
    session_wire_container::resultset_wires_container* rwc = reinterpret_cast<session_wire_container::resultset_wires_container*>(static_cast<std::uintptr_t>(handle));

    const char* name_ = env->GetStringUTFChars(name, nullptr);
    if (name_ == nullptr) {
        jclass classj = env->FindClass("Ljava/io/IOException;");
        if (classj == nullptr) { std::abort(); }
        env->ThrowNew(classj, "name is null");
        env->DeleteLocalRef(classj);
    }
    jsize len_ = env->GetStringUTFLength(name);

    try {
        rwc->connect(std::string_view(name_, len_));
    } catch (std::runtime_error &e) {
        jclass classj = env->FindClass("Ljava/io/IOException;");
        if (classj == nullptr) { std::abort(); }
        env->ThrowNew(classj, e.what());
        env->DeleteLocalRef(classj);
    }
    env->ReleaseStringUTFChars(name, name_);
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_channel_ipc_sql_ResultSetWireImpl
 * Method:    getChunkNative
 * Signature: (J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_com_nautilus_1technologies_tsubakuro_channel_ipc_sql_ResultSetWireImpl_getChunkNative
(JNIEnv *env, jclass, jlong handle)
{
    session_wire_container::resultset_wires_container* rwc = reinterpret_cast<session_wire_container::resultset_wires_container*>(static_cast<std::uintptr_t>(handle));

    auto buf = rwc->get_chunk();
    if(buf.second > 0) {
        return env->NewDirectByteBuffer(buf.first, buf.second);
    }
    return nullptr;
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_channel_ipc_sql_ResultSetWireImpl
 * Method:    disposeUsedDataNative
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_channel_ipc_sql_ResultSetWireImpl_disposeUsedDataNative
(JNIEnv *env, jclass, jlong handle, jlong length)
{
    session_wire_container::resultset_wires_container* rwc = reinterpret_cast<session_wire_container::resultset_wires_container*>(static_cast<std::uintptr_t>(handle));

    try {
        rwc->dispose(length);
    } catch (std::runtime_error &e) {
        jclass classj = env->FindClass("Ljava/io/IOException;");
        if (classj == nullptr) { std::abort(); }
        env->ThrowNew(classj, e.what());
        env->DeleteLocalRef(classj);
    }
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_channel_ipc_sql_ResultSetWireImpl
 * Method:    isEndOfRecordNative
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_nautilus_1technologies_tsubakuro_channel_ipc_sql_ResultSetWireImpl_isEndOfRecordNative
(JNIEnv *, jclass, jlong handle)
{
    session_wire_container::resultset_wires_container* rwc = reinterpret_cast<session_wire_container::resultset_wires_container*>(static_cast<std::uintptr_t>(handle));

    return static_cast<jboolean>(rwc->is_eor());
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_channel_ipc_sql_ResultSetWireImpl
 * Method:    closeNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_channel_ipc_sql_ResultSetWireImpl_closeNative
(JNIEnv *, jclass, jlong handle)
{
    session_wire_container::resultset_wires_container* rwc = reinterpret_cast<session_wire_container::resultset_wires_container*>(static_cast<std::uintptr_t>(handle));

    if (rwc != nullptr) {
        session_wire_container* envelope = rwc->get_envelope();
        envelope->dispose_resultset_wire(rwc);
    }
}
