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
#include "com_tsurugidb_tsubakuro_channel_ipc_connection_IpcConnectorImpl.h"
#include "udf_wires.h"

using namespace tateyama::common::wire;

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_connection_IpcConnectorImpl
 * Method:    getConnectorNative
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_connection_IpcConnectorImpl_getConnectorNative
(JNIEnv *env, jclass, jstring name)
{
    connection_container* container;
    const char* name_ = env->GetStringUTFChars(name, NULL);
    if (name_ == NULL) return 0;
    jsize len_ = env->GetStringUTFLength(name);

    try {
        container = new connection_container(std::string_view(name_, len_));
        env->ReleaseStringUTFChars(name, name_);
        return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(container));
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
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_connection_IpcConnectorImpl
 * Method:    requestNative
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_connection_IpcConnectorImpl_requestNative
(JNIEnv *env, jclass, jlong handle)
{
    connection_container* container = reinterpret_cast<connection_container*>(static_cast<std::uintptr_t>(handle));
    try {
        return container->get_connection_queue().request();
    } catch (std::runtime_error &e) {
        jclass classj = env->FindClass("Ljava/io/IOException;");
        if (classj == nullptr) { std::abort(); }
        env->ThrowNew(classj, e.what());
        env->DeleteLocalRef(classj);
        return 0;
    }
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_connection_IpcConnectorImpl
 * Method:    waitNative
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_connection_IpcConnectorImpl_waitNative__JJ
(JNIEnv *env, jclass, jlong handle, jlong id)
{
    connection_container* container = reinterpret_cast<connection_container*>(static_cast<std::uintptr_t>(handle));
    if (auto session_id = container->get_connection_queue().wait(id); session_id != tateyama::common::wire::connection_queue::session_id_indicating_error) {
        return session_id;
    }
    jclass classj = env->FindClass("Ljava/net/ConnectException;");
    if (classj == nullptr) { std::abort(); }
    env->ThrowNew(classj, "IPC connection establishment failure");
    env->DeleteLocalRef(classj);
    return 0;
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_connection_IpcConnectorImpl
 * Method:    waitNative
 * Signature: (JJJ)J
 */
JNIEXPORT jlong JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_connection_IpcConnectorImpl_waitNative__JJJ
(JNIEnv *env, jclass, jlong handle, jlong id, long timeout)
{
    connection_container* container = reinterpret_cast<connection_container*>(static_cast<std::uintptr_t>(handle));
    try {
        if (auto session_id = container->get_connection_queue().wait(id, timeout / 1000); session_id != tateyama::common::wire::connection_queue::session_id_indicating_error) {
            return session_id;
        }
        jclass classj = env->FindClass("Ljava/net/ConnectException;");
        if (classj == nullptr) { std::abort(); }
        env->ThrowNew(classj, "IPC connection establishment failure");
        env->DeleteLocalRef(classj);
        return 0;
    } catch (std::runtime_error &e) {
        jclass classj = env->FindClass("Ljava/util/concurrent/TimeoutException;");
        if (classj == nullptr) { std::abort(); }
        env->ThrowNew(classj, e.what());
        env->DeleteLocalRef(classj);
        return 0;
    }
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_connection_IpcConnectorImpl
 * Method:    checkNative
 * Signature: (JJ)Z
 */
JNIEXPORT jboolean JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_connection_IpcConnectorImpl_checkNative
(JNIEnv *, jclass, jlong handle, jlong id)
{
    connection_container* container = reinterpret_cast<connection_container*>(static_cast<std::uintptr_t>(handle));
    return container->get_connection_queue().check(id);
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_connection_IpcConnectorImpl
 * Method:    closeConnectorNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_connection_IpcConnectorImpl_closeConnectorNative
(JNIEnv *, jclass, jlong handle)
{
    connection_container* container = reinterpret_cast<connection_container*>(static_cast<std::uintptr_t>(handle));
    delete container;
}
