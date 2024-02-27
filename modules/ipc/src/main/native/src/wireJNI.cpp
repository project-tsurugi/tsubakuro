/*
 * Copyright 2019-2023 Project Tsurugi.
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
#include "com_tsurugidb_tsubakuro_channel_ipc_IpcLink.h"
#include "com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl.h"
#include "udf_wires.h"

using namespace tateyama::common::wire;

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_IpcLink
 * Method:    openNative
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_openNative
(JNIEnv *env, jclass, jstring name)
{
    session_wire_container* swc;
    const char* name_ = env->GetStringUTFChars(name, nullptr);
    if (name_ == nullptr) return 0;
    jsize len_ = env->GetStringUTFLength(name);

    try {
        swc = new session_wire_container(std::string_view(name_, len_));
        env->ReleaseStringUTFChars(name, name_);
        return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(swc));
    } catch (std::runtime_error &e) {
        env->ReleaseStringUTFChars(name, name_);
        jclass classj = env->FindClass("Ljava/io/IOException;");
        if (classj == nullptr) { std::abort(); }
        env->ThrowNew(classj, e.what());
        env->DeleteLocalRef(classj);
        return 0;
    }
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_IpcLink
 * Method:    sendNative
 * Signature: (JI[B)V
 */
JNIEXPORT void JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_sendNative
  (JNIEnv *env, jclass, jlong handle, jint slot, jbyteArray message) {
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));

    auto m_address = env->GetByteArrayElements(message, nullptr);

    auto& request_wire = swc->get_request_wire();
    request_wire.write(static_cast<signed char*>(m_address), env->GetArrayLength(message), slot);
    env->ReleaseByteArrayElements(message, m_address, JNI_ABORT);
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_IpcLink
 * Method:    awaitNative
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_awaitNative
  (JNIEnv *env, jclass, jlong handle, jlong timeout) {
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));

    while (true) {
        try {
            auto header = swc->get_response_wire().await(timeout);
            if (header.get_type() != 0) {
                return header.get_idx();
            }
            return -1;
        } catch (std::runtime_error &e) {
            if (timeout > 0) {
                jclass classj = env->FindClass("Ljava/util/concurrent/TimeoutException;");
                if (classj == nullptr) { std::abort(); }
                env->ThrowNew(classj, e.what());
                env->DeleteLocalRef(classj);
                return 0;
            }
            if (auto err = swc->get_status_provider().is_alive(); !err.empty()) {
                jclass classj = env->FindClass("Ljava/io/IOException;");
                if (classj == nullptr) { std::abort(); }
                env->ThrowNew(classj, (std::string("No response from the server for a long time, server status check result is '") + err + "'").c_str());;
                env->DeleteLocalRef(classj);
                return 0;
            }
        }
    }
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_IpcLink
 * Method:    getInfoNative
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_getInfoNative
  (JNIEnv *, jclass, jlong handle)
{
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));

    return swc->get_response_wire().get_type();
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_IpcLink
 * Method:    receiveNative
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_receiveNative
  (JNIEnv *env, jclass, jlong handle)
{
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));
    auto& response_wire = swc->get_response_wire();

    auto len = response_wire.get_length();
    jbyteArray dstj = env->NewByteArray(static_cast<jint>(len));
    if (dstj == NULL) {
        return NULL;
    }
    jbyte* dst = env->GetByteArrayElements(dstj, NULL);
    if (dst == NULL) {
        return NULL;
    }
    response_wire.read(dst);
    env->ReleaseByteArrayElements(dstj, dst, 0);

    return dstj;
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_IpcLink
 * Method:    isAliveNative
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_isAliveNative
(JNIEnv *, jclass, jlong handle)
{
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));
    return swc->get_status_provider().is_alive().empty();
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_IpcLink
 * Method:    closeNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_closeNative
(JNIEnv *, jclass, jlong handle)
{
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));

    if (swc != nullptr) {
        swc->get_response_wire().close();
    }
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_IpcLink
 * Method:    destroyNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_destroyNative
(JNIEnv *, jclass, jlong handle)
{
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));

    if (swc != nullptr) {
        if (swc->is_deletable()) {
            swc->get_request_wire().disconnect();
            delete swc;
        }
    }
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl
 * Method:    createNative
 * Signature: (JLjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_createNative
(JNIEnv *env, jclass, jlong handle, jstring name)
{
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));

    session_wire_container::resultset_wires_container* rwc {};
    try {
        rwc = swc->create_resultset_wire();

        const char* name_ = env->GetStringUTFChars(name, nullptr);
        if (name_ == nullptr) {
            jclass classj = env->FindClass("Ljava/io/IOException;");
            if (classj == nullptr) { std::abort(); }
            env->ThrowNew(classj, "name is null");
            env->DeleteLocalRef(classj);
            return 0;
        }
        jsize len_ = env->GetStringUTFLength(name);

        try {
            rwc->connect(std::string_view(name_, len_));
            env->ReleaseStringUTFChars(name, name_);
        } catch (std::runtime_error &e) {
            env->ReleaseStringUTFChars(name, name_);
            jclass classj = env->FindClass("Ljava/io/IOException;");
            if (classj == nullptr) { std::abort(); }
            env->ThrowNew(classj, e.what());
            env->DeleteLocalRef(classj);
            return 0;
        }

        return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(rwc));
    } catch (std::runtime_error &e) {
        jclass classj = env->FindClass("Ljava/io/IOException;");
        if (classj == nullptr) { std::abort(); }
        env->ThrowNew(classj, e.what());
        env->DeleteLocalRef(classj);
        return 0;
    }
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl
 * Method:    getChunkNative
 * Signature: (J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_getChunkNative
(JNIEnv *env, jclass, jlong handle)
{
    session_wire_container::resultset_wires_container* rwc = reinterpret_cast<session_wire_container::resultset_wires_container*>(static_cast<std::uintptr_t>(handle));

    while (true) {
        try {
            auto buf = rwc->get_chunk();
            if(buf.data()) {
                return env->NewDirectByteBuffer(static_cast<void*>(const_cast<char*>(buf.data())), buf.length());
            }
            return nullptr;
        } catch (std::runtime_error &e) {
            if (auto err = rwc->get_envelope()->get_status_provider().is_alive(); !err.empty()) {
                jclass classj = env->FindClass("Ljava/io/IOException;");
                if (classj == nullptr) { std::abort(); }
                env->ThrowNew(classj, (std::string("No response from the server for a long time, server status check result is '") + err + "'").c_str());;
                env->DeleteLocalRef(classj);
                return nullptr;
            }
            continue;
        }
    }
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl
 * Method:    disposeUsedDataNative
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_disposeUsedDataNative
(JNIEnv *, jclass, jlong handle, jlong size)
{
    session_wire_container::resultset_wires_container* rwc = reinterpret_cast<session_wire_container::resultset_wires_container*>(static_cast<std::uintptr_t>(handle));

    if (size > 0) {
        rwc->dispose();
    }
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl
 * Method:    isEndOfRecordNative
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_isEndOfRecordNative
(JNIEnv *, jclass, jlong handle)
{
    session_wire_container::resultset_wires_container* rwc = reinterpret_cast<session_wire_container::resultset_wires_container*>(static_cast<std::uintptr_t>(handle));

    return static_cast<jboolean>(rwc->is_eor());
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl
 * Method:    closeNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_closeNative
(JNIEnv *, jclass, jlong handle)
{
    session_wire_container::resultset_wires_container* rwc = reinterpret_cast<session_wire_container::resultset_wires_container*>(static_cast<std::uintptr_t>(handle));

    if (rwc != nullptr) {
        session_wire_container* swc = rwc->get_envelope();
        if (swc != nullptr) {
            if (swc->dispose_resultset_wire(rwc)) {
                swc->get_request_wire().disconnect();
                delete swc;
            } 
        }
    }
}
