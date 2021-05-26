#include <iostream>

#include <jni.h>
#include "com_nautilus_technologies_tsubakuro_impl_low_sql_ServerWireImpl.h"
#include "wire.h"

using namespace tsubakuro::common;

JNIEXPORT jlong JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_ServerWireImpl_createNative
(JNIEnv *env, [[maybe_unused]] jclass thisObj, jstring name)
{
    const char* name_ = env->GetStringUTFChars(name, NULL);
    if (name_ == NULL) return 0;
    jsize len_ = env->GetStringUTFLength(name);

    session_wire_container* container = new session_wire_container(std::string_view(name_, len_), true);
    return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(container));
}

JNIEXPORT jobject JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_WireImpl_getNative
(JNIEnv *env, [[maybe_unused]] jclass thisObj, jlong handle)
{
    session_wire_container* container = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));

    std::size_t length = container->get_request_wire().length();
    char buffer[length];

    container->get_response_wire().read(buffer, length);
    return env->NewDirectByteBuffer(buffer, length);
}

JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_WireImpl_putNative
(JNIEnv *env, [[maybe_unused]] jclass thisObj, jlong handle, jobject buffer)
{
    session_wire_container* container = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));

    char *buf = (char*)env->GetDirectBufferAddress(buffer);
    jlong capacity = env->GetDirectBufferCapacity(buffer);

    container->get_response_wire().write(buf, capacity);
}

JNIEXPORT jboolean JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_WireImpl_closeNative
([[maybe_unused]] JNIEnv *env, [[maybe_unused]] jclass thisObj, jlong handle)
{
    session_wire_container* container = reinterpret_cast<session_wire_container*>(static_cast<std::uintptr_t>(handle));

    delete container;
    return static_cast<jboolean>(true);
}
