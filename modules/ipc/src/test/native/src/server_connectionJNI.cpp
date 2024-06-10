#include <iostream>

#include <jni.h>
#include "com_tsurugidb_tsubakuro_channel_ipc_connection_ServerConnectionImpl.h"
#include "server_wires.h"

using namespace tateyama::common::wire;

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_connection_ServerConnectionImpl
 * Method:    createNative
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_connection_ServerConnectionImpl_createNative
(JNIEnv *env, jclass, jstring name)
{
    const char* name_ = env->GetStringUTFChars(name, NULL);
    if (name_ == NULL) {
        std::abort();
    }
    jsize len_ = env->GetStringUTFLength(name);

    connection_container* container = new connection_container(std::string_view(name_, len_));
    env->ReleaseStringUTFChars(name, name_);
    return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(container));
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_connection_ServerConnectionImpl
 * Method:    listenNative
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_connection_ServerConnectionImpl_listenNative
(JNIEnv *, jclass, jlong handle)
{
    connection_container* container = reinterpret_cast<connection_container*>(static_cast<std::uintptr_t>(handle));
    container->slot_ = container->get_connection_queue().slot();
    return container->get_connection_queue().listen();
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_connection_ServerConnectionImpl
 * Method:    acceptNative
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_connection_ServerConnectionImpl_acceptNative
(JNIEnv *, jclass, jlong handle, jlong id)
{
    connection_container* container = reinterpret_cast<connection_container*>(static_cast<std::uintptr_t>(handle));

    container->get_connection_queue().accept(container->get_connection_queue().slot(), id);
}

/*
 * Class:     com_tsurugidb_tsubakuro_channel_ipc_connection_ServerConnectionImpl
 * Method:    closeNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_connection_ServerConnectionImpl_closeNative
(JNIEnv *, jclass, jlong handle)
{
    connection_container* container = reinterpret_cast<connection_container*>(static_cast<std::uintptr_t>(handle));
    delete container;
}
