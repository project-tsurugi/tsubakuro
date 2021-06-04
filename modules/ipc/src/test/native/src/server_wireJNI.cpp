#include <iostream>

#include <jni.h>
#include "com_nautilus_technologies_tsubakuro_impl_low_sql_ServerWireImpl.h"
#include "server_wires.h"

using namespace tsubakuro::common::wire;

JNIEXPORT jlong JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_ServerWireImpl_createNative
(JNIEnv *env, [[maybe_unused]] jclass thisObj, jstring name)
{
    const char* name_ = env->GetStringUTFChars(name, NULL);
    if (name_ == NULL) return 0;
    jsize len_ = env->GetStringUTFLength(name);

    wire_container* container = new wire_container(std::string_view(name_, len_));
    env->ReleaseStringUTFChars(name, name_);
    return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(container));
}

JNIEXPORT jbyteArray JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_ServerWireImpl_getNative
(JNIEnv *env, [[maybe_unused]] jclass thisObj, jlong handle)
{
    wire_container* container = reinterpret_cast<wire_container*>(static_cast<std::uintptr_t>(handle));

    auto& wire = container->get_request_wire();
    message_header h = wire.peep();
    if (h.get_idx() != 0) {
        std::abort();  // out of the scope of this test program
    }
    std::size_t length = h.get_length();
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

JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_ServerWireImpl_putNative
(JNIEnv *env, [[maybe_unused]] jclass thisObj, jlong handle, jbyteArray srcj)
{
    wire_container* container = reinterpret_cast<wire_container*>(static_cast<std::uintptr_t>(handle));

    jbyte *src = env->GetByteArrayElements(srcj, 0);
    jsize capacity = env->GetArrayLength(srcj);

    if (src == nullptr) {
        std::abort();  // This is OK, because server_wire is used for test purpose only
    }

    container->get_response_wire().write(src, message_header(0, capacity));
    env->ReleaseByteArrayElements(srcj, src, 0);
}

JNIEXPORT jboolean JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_ServerWireImpl_closeNative
([[maybe_unused]] JNIEnv *env, [[maybe_unused]] jclass thisObj, jlong handle)
{
    wire_container* container = reinterpret_cast<wire_container*>(static_cast<std::uintptr_t>(handle));

    delete container;
    return static_cast<jboolean>(true);
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_impl_low_sql_ServerWireImpl
 * Method:    createRSLNative
 * Signature: (JLjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_ServerWireImpl_createRSLNative
(JNIEnv *env, jclass, jlong handle, jstring name)
{
    wire_container* container = reinterpret_cast<wire_container*>(static_cast<std::uintptr_t>(handle));

    const char* name_ = env->GetStringUTFChars(name, NULL);
    if (name_ == NULL) return 0;
    jsize len_ = env->GetStringUTFLength(name);

    wire_container::resultset_wire_container* rs_container = container->create_resultset_wire(std::string_view(name_, len_));
    env->ReleaseStringUTFChars(name, name_);
    return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(rs_container));
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_impl_low_sql_ServerWireImpl
 * Method:    putRSLNative
 * Signature: (J[B)J
 */
JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_ServerWireImpl_putRSLNative
(JNIEnv *env, jclass, jlong handle, jbyteArray srcj)
{
    wire_container::resultset_wire_container* container = reinterpret_cast<wire_container::resultset_wire_container*>(static_cast<std::uintptr_t>(handle));
    auto& wire = container->get_resultset_wire();

    jbyte *src = env->GetByteArrayElements(srcj, 0);
    jsize capacity = env->GetArrayLength(srcj);

    if (src == nullptr) {
        std::abort();  // This is OK, because server_wire is used for test purpose only
    }

    wire.write(src, length_header(capacity));
    env->ReleaseByteArrayElements(srcj, src, 0);
}
