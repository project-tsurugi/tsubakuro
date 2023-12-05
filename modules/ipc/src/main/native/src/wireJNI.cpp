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
#include <csetjmp>
#include <csignal>
#include <mutex>

#include "com_tsurugidb_tsubakuro_channel_ipc_IpcLink.h"
#include "com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl.h"
#include "udf_wires.h"

using namespace tateyama::common::wire;

static bool ipc_sighandler_installed{};
static std::mutex mtx{};
static struct sigaction old_sa;

static thread_local sigjmp_buf jump_buf{};
static thread_local bool in_ipc{};

static void ipc_sighandler(int signum, siginfo_t*, void*) {
    if (signum == SIGSEGV) {
        if (in_ipc) {
            siglongjmp(jump_buf, 1);
        } else {
            if (old_sa.sa_handler != nullptr) {
                old_sa.sa_handler(signum);
            }
        }
    }
}

static int enter_ipc() {
    in_ipc = true;
    return sigsetjmp(jump_buf, 0);
}
static void exit_ipc() {
    in_ipc = false;
}

static void throw_server_crash_exception(JNIEnv *env, session_wire_container* swc = nullptr) {
    exit_ipc();
    jclass classj = env->FindClass("Ljava/io/IOException;");
    if (classj == nullptr) { std::abort(); }
    const char *msg{};
    if (swc != nullptr) {
        if (!swc->get_status_provider().is_alive()) {
            msg = "Server crashed";
        } else {
            msg = "SIGSEGV have arised in ipc channel";
        }
    } else {
        msg = "Server crashed";
    }
    env->ThrowNew(classj, msg);
    env->DeleteLocalRef(classj);
}

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

    if (!ipc_sighandler_installed) {
        std::lock_guard<std::mutex> lock(mtx);

        struct sigaction handler;
        memset(&handler, 0, sizeof(handler));
        handler.sa_sigaction = ipc_sighandler;
        handler.sa_flags = SA_RESETHAND;
        sigaction(SIGSEGV, &handler, &old_sa);

        ipc_sighandler_installed = true;
    }

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

static void JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_sendNative_impl
  (JNIEnv *env, jlong session_wire_handle, jint slot, jbyteArray message) {
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(session_wire_handle));

    auto m_address = env->GetByteArrayElements(message, nullptr);

    auto& request_wire = swc->get_request_wire();
    request_wire.write(static_cast<signed char*>(m_address), env->GetArrayLength(message), slot);
    env->ReleaseByteArrayElements(message, m_address, JNI_ABORT);
}
/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_IpcLink
 * Method:    sendNative
 * Signature: (JI[B)V
 */
JNIEXPORT void JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_sendNative
  (JNIEnv *env, jclass, jlong session_wire_handle, jint slot, jbyteArray message) {
    if (auto rv = enter_ipc(); rv == 0) {
        Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_sendNative_impl(env, session_wire_handle, slot, message);
        exit_ipc();
    } else {
        throw_server_crash_exception(env, reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(session_wire_handle)));
    }
}

static jint JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_awaitNative_impl
  (JNIEnv *env, jlong session_wire_handle, jlong timeout) {
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(session_wire_handle));

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
            if (!swc->get_status_provider().is_alive()) {
                throw_server_crash_exception(env);
                return 0;
            }
        }
    }
}
/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_IpcLink
 * Method:    awaitNative
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_awaitNative
  (JNIEnv *env, jclass, jlong session_wire_handle, jlong timeout) {
    if (auto rv = enter_ipc(); rv == 0) {
        auto retrun_value = Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_awaitNative_impl(env, session_wire_handle, timeout);
        exit_ipc();
        return retrun_value;
    } else {
        throw_server_crash_exception(env, reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(session_wire_handle)));
        return 0;
    }
}

static jint JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_getInfoNative_impl(jlong session_wire_handle) {
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(session_wire_handle));

    return swc->get_response_wire().get_type();
}
/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_IpcLink
 * Method:    getInfoNative
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_getInfoNative
  (JNIEnv *env, jclass, jlong session_wire_handle)
{
    if (auto rv = enter_ipc(); rv == 0) {
        auto retrun_value = Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_getInfoNative_impl(session_wire_handle);
        exit_ipc();
        return retrun_value;
    } else {
        throw_server_crash_exception(env, reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(session_wire_handle)));
        return 0;
    }
}


static jbyteArray JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_receiveNative_impl(JNIEnv *env, jlong session_wire_handle) {
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(session_wire_handle));
    auto& response_wire = swc->get_response_wire();

    auto len = response_wire.get_length();
    jbyteArray dstj = env->NewByteArray(static_cast<jint>(len));
    if (dstj == nullptr) {
        return nullptr;
    }
    jbyte* dst = env->GetByteArrayElements(dstj, nullptr);
    if (dst == nullptr) {
        return nullptr;
    }
    response_wire.read(dst);
    env->ReleaseByteArrayElements(dstj, dst, 0);

    return dstj;
}
/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_IpcLink
 * Method:    receiveNative
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_receiveNative
  (JNIEnv *env, jclass, jlong session_wire_handle)
{
    if (auto rv = enter_ipc(); rv == 0) {
        auto retrun_value = Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_receiveNative_impl(env, session_wire_handle);
        exit_ipc();
        return retrun_value;
    } else {
        throw_server_crash_exception(env, reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(session_wire_handle)));
        return nullptr;
    }
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_IpcLink
 * Method:    isAliveNative
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_isAliveNative
(JNIEnv *, jclass, jlong session_wire_handle)
{
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(session_wire_handle));
    return swc->get_status_provider().is_alive();
}

static void JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_closeNative_impl(jlong session_wire_handle) {
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(session_wire_handle));

    if (swc != nullptr) {
        swc->get_response_wire().close();
    }
}
/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_IpcLink
 * Method:    closeNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_closeNative
(JNIEnv *env, jclass, jlong session_wire_handle)
{
    if (auto rv = enter_ipc(); rv == 0) {
        Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_closeNative_impl(session_wire_handle);
        exit_ipc();
    } else {
        throw_server_crash_exception(env, reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(session_wire_handle)));
    }
}

static void JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_destroyNative(jlong session_wire_handle) {
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(session_wire_handle));

    if (swc != nullptr) {
        if (swc->is_deletable()) {
            swc->get_request_wire().disconnect();
            delete swc;
        }
    }
}
/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_IpcLink
 * Method:    destroyNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_destroyNative
(JNIEnv *env, jclass, jlong session_wire_handle)
{
    if (auto rv = enter_ipc(); rv == 0) {
        Java_com_tsurugidb_tsubakuro_channel_ipc_IpcLink_destroyNative(session_wire_handle);
        exit_ipc();
    } else {
        throw_server_crash_exception(env, reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(session_wire_handle)));
    }
}

static jlong JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_createNative_impl(JNIEnv *env, jlong session_wire_handle, jstring name) {
    session_wire_container* swc = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(session_wire_handle));

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
 * Method:    createNative
 * Signature: (JLjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_createNative
(JNIEnv *env, jclass, jlong session_wire_handle, jstring name)
{
    if (auto rv = enter_ipc(); rv == 0) {
        auto retrun_value = Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_createNative_impl(env, session_wire_handle, name);
        exit_ipc();
        return retrun_value;
    } else {
        throw_server_crash_exception(env, reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(session_wire_handle)));
        return 0;
    }
}

static jobject JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_getChunkNative_impl(JNIEnv *env, jlong resultset_wire_handle) {
    session_wire_container::resultset_wires_container* rwc = reinterpret_cast<session_wire_container::resultset_wires_container*>(static_cast<std::uintptr_t>(resultset_wire_handle));

    while (true) {
        try {
            auto buf = rwc->get_chunk();
            if(buf.data()) {
                return env->NewDirectByteBuffer(static_cast<void*>(const_cast<char*>(buf.data())), buf.length());
            }
            return nullptr;
        } catch (std::runtime_error &e) {
            if (!rwc->get_envelope()->get_status_provider().is_alive()) {
                throw_server_crash_exception(env);
                return nullptr;
            }
            continue;
        }
    }
}
/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl
 * Method:    getChunkNative
 * Signature: (J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_getChunkNative
(JNIEnv *env, jclass, jlong resultset_wire_handle)
{
    if (auto rv = enter_ipc(); rv == 0) {
        auto retrun_value = Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_getChunkNative_impl(env, resultset_wire_handle);
        exit_ipc();
        return retrun_value;
    } else {
        throw_server_crash_exception(env, reinterpret_cast<session_wire_container::resultset_wires_container*>(static_cast<std::uintptr_t>(resultset_wire_handle))->get_envelope());
        return nullptr;
    }
}

static void JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_disposeUsedDataNative_impl(jlong resultset_wire_handle, jlong size) {
    session_wire_container::resultset_wires_container* rwc = reinterpret_cast<session_wire_container::resultset_wires_container*>(static_cast<std::uintptr_t>(resultset_wire_handle));

    if (size > 0) {
        rwc->dispose();
    }
}
/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl
 * Method:    disposeUsedDataNative
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_disposeUsedDataNative
(JNIEnv *env, jclass, jlong resultset_wire_handle, jlong size)
{
    if (auto rv = enter_ipc(); rv == 0) {
        Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_disposeUsedDataNative_impl(resultset_wire_handle, size);
        exit_ipc();
    } else {
        throw_server_crash_exception(env, reinterpret_cast<session_wire_container::resultset_wires_container*>(static_cast<std::uintptr_t>(resultset_wire_handle))->get_envelope());
    }
}

static jboolean JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_isEndOfRecordNative_impl(jlong resultset_wire_handle) {
    session_wire_container::resultset_wires_container* rwc = reinterpret_cast<session_wire_container::resultset_wires_container*>(static_cast<std::uintptr_t>(resultset_wire_handle));

    return static_cast<jboolean>(rwc->is_eor());
}
/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl
 * Method:    isEndOfRecordNative
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_isEndOfRecordNative
(JNIEnv *env, jclass, jlong resultset_wire_handle)
{
    if (auto rv = enter_ipc(); rv == 0) {
        auto retrun_value = Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_isEndOfRecordNative_impl(resultset_wire_handle);
        exit_ipc();
        return retrun_value;
    } else {
        throw_server_crash_exception(env, reinterpret_cast<session_wire_container::resultset_wires_container*>(static_cast<std::uintptr_t>(resultset_wire_handle))->get_envelope());
        return false;
    }
}

static void JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_closeNative_impl(jlong resultset_wire_handle) {
    session_wire_container::resultset_wires_container* rwc = reinterpret_cast<session_wire_container::resultset_wires_container*>(static_cast<std::uintptr_t>(resultset_wire_handle));

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
/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl
 * Method:    closeNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_closeNative
(JNIEnv *env, jclass, jlong resultset_wire_handle)
{
    if (auto rv = enter_ipc(); rv == 0) {
        Java_com_tsurugidb_tsubakuro_channel_ipc_sql_ResultSetWireImpl_closeNative_impl(resultset_wire_handle);
        exit_ipc();
    } else {
        throw_server_crash_exception(env, reinterpret_cast<session_wire_container::resultset_wires_container*>(static_cast<std::uintptr_t>(resultset_wire_handle))->get_envelope());
    }
}
