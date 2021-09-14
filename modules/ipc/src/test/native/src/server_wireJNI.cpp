#include <iostream>

#include <jni.h>
#include "com_nautilus_technologies_tsubakuro_impl_low_sql_ServerWireImpl.h"
#include "server_wires.h"

using namespace tateyama::common::wire;

JNIEXPORT jlong JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_ServerWireImpl_createNative
(JNIEnv *env, [[maybe_unused]] jclass thisObj, jstring name)
{
    const char* name_ = env->GetStringUTFChars(name, NULL);
    if (name_ == NULL) return 0;
    jsize len_ = env->GetStringUTFLength(name);

    server_wire_container* container = new server_wire_container(std::string_view(name_, len_));
    env->ReleaseStringUTFChars(name, name_);
    return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(container));
}

JNIEXPORT jbyteArray JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_ServerWireImpl_getNative
(JNIEnv *env, [[maybe_unused]] jclass thisObj, jlong handle)
{
    server_wire_container* container = reinterpret_cast<server_wire_container*>(static_cast<std::uintptr_t>(handle));

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

    wire.read(reinterpret_cast<char*>(dst), length);
    env->ReleaseByteArrayElements(dstj, dst, 0);
    return dstj;
}

JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_ServerWireImpl_putNative
(JNIEnv *env, [[maybe_unused]] jclass thisObj, jlong handle, jbyteArray srcj)
{
    server_wire_container* container = reinterpret_cast<server_wire_container*>(static_cast<std::uintptr_t>(handle));

    jbyte *src = env->GetByteArrayElements(srcj, 0);
    jsize capacity = env->GetArrayLength(srcj);

    if (src == nullptr) {
        std::abort();  // This is OK, because server_wire is used for test purpose only
    }

    auto& response = container->get_response(0);
    memcpy(response.get_buffer(capacity), src, capacity);
    response.flush();
    env->ReleaseByteArrayElements(srcj, src, 0);
}

JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_ServerWireImpl_closeNative
([[maybe_unused]] JNIEnv *env, [[maybe_unused]] jclass thisObj, jlong handle)
{
    server_wire_container* container = reinterpret_cast<server_wire_container*>(static_cast<std::uintptr_t>(handle));

    delete container;
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_impl_low_sql_ServerWireImpl
 * Method:    createRSLNative
 * Signature: (JLjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_ServerWireImpl_createRSLNative
(JNIEnv *env, jclass, jlong handle, jstring name)
{
    server_wire_container* container = reinterpret_cast<server_wire_container*>(static_cast<std::uintptr_t>(handle));

    const char* name_ = env->GetStringUTFChars(name, NULL);
    if (name_ == NULL) return 0;
    jsize len_ = env->GetStringUTFLength(name);

    server_wire_container::resultset_wire* rs_container = container->create_resultset_wire(std::string_view(name_, len_));
    env->ReleaseStringUTFChars(name, name_);
    return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(rs_container));
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_impl_low_sql_ServerWireImpl
 * Method:    putRecordsRSLNative
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_ServerWireImpl_putRecordsRSLNative
(JNIEnv *env, jclass, jlong handle, jbyteArray srcj)
{
    server_wire_container::resultset_wire* wire = reinterpret_cast<server_wire_container::resultset_wire*>(static_cast<std::uintptr_t>(handle));

    jbyte *src = env->GetByteArrayElements(srcj, 0);
    jsize capacity = env->GetArrayLength(srcj);

    if (src == nullptr) {
        std::abort();  // This is OK, because server_wire is used for test purpose only
    }

    wire->write(reinterpret_cast<const char*>(src), capacity);
    env->ReleaseByteArrayElements(srcj, src, 0);
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_impl_low_sql_ServerWireImpl
 * Method:    commitRSLNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_ServerWireImpl_commitRSLNative
(JNIEnv *, jclass, jlong handle)
{
    server_wire_container::resultset_wire* wire = reinterpret_cast<server_wire_container::resultset_wire*>(static_cast<std::uintptr_t>(handle));

    wire->commit();
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_impl_low_sql_ServerWireImpl
 * Method:    closeRSLNative
 * Signature: (J)J
 */
JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_ServerWireImpl_closeRSLNative
(JNIEnv *, jclass, jlong handle)
{
    server_wire_container::resultset_wire* container = reinterpret_cast<server_wire_container::resultset_wire*>(static_cast<std::uintptr_t>(handle));

    delete container;
}
