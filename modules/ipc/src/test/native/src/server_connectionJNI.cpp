#include <iostream>

#include <jni.h>
#include "com_nautilus_technologies_tsubakuro_impl_low_connection_ServerConnectionImpl.h"
#include "server_wires.h"

using namespace tsubakuro::common::wire;

/*
 * Class:     com_nautilus_technologies_tsubakuro_impl_low_connection_ServerConnectionImpl
 * Method:    listenNative
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_connection_ServerConnectionImpl_listenNative
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
 * Class:     com_nautilus_technologies_tsubakuro_impl_low_connection_ServerConnectionImpl
 * Method:    acceptNative
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_connection_ServerConnectionImpl_acceptNative
(JNIEnv *, jclass, jlong handle)
{
    connection_container* container = reinterpret_cast<connection_container*>(static_cast<std::uintptr_t>(handle));
    return container->get_connection_queue().accept();
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_impl_low_connection_ServerConnectionImpl
 * Method:    closeNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_connection_ServerConnectionImpl_closeNative
(JNIEnv *, jclass, jlong handle)
{
    connection_container* container = reinterpret_cast<connection_container*>(static_cast<std::uintptr_t>(handle));
    delete container;
}
