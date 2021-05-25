#include <iostream>

#include <jni.h>
#include "com_nautilus_technologies_tsubakuro_impl_low_sql_WireImpl.h"

class server {
public:
    server([[maybe_unused]] std::string_view name) {
        std::cout << __func__ << ":" << __LINE__ << ": name : " << name << std::endl;
    }
    void send(std::string_view data) {
        std::cout << __func__ << ":" << __LINE__ << ": Buffer : " <<
            static_cast<const void*>(data.data()) << " : " << data.length()<< std::endl;
    }
    std::string recv() {
        std::string data;
        data.resize(128);
        return data;
    }
};

JNIEXPORT jlong JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_WireImpl_openNative
(JNIEnv *env, [[maybe_unused]] jclass thisObj, jstring name)
{
    const char* name_ = env->GetStringUTFChars(name, NULL);
    if (name_ == NULL) return 0;
    jsize len_ = env->GetStringUTFLength(name);
    server* server_ = new server(std::string_view(name_, len_));
                                
    return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(server_));
}

JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_WireImpl_sendNative
(JNIEnv *env, [[maybe_unused]] jclass thisObj, jlong handle, jobject buffer)
{
    server* server_ = reinterpret_cast<server*>(static_cast<std::uintptr_t>(handle));

    char *buf = (char*)env->GetDirectBufferAddress(buffer);
    jlong capacity = env->GetDirectBufferCapacity(buffer);

    server_->send(std::string_view(buf, capacity));
}


JNIEXPORT jobject JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_WireImpl_recvNative
(JNIEnv *env, [[maybe_unused]] jclass thisObj, jlong handle)
{
    server* server_ = reinterpret_cast<server*>(static_cast<std::uintptr_t>(handle));

    auto data = server_->recv();
    return env->NewDirectByteBuffer(data.data(), data.length());
}

JNIEXPORT jboolean JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_sql_WireImpl_closeNative
([[maybe_unused]] JNIEnv *env, [[maybe_unused]] jclass thisObj, jlong handle)
{
    server* server_ = reinterpret_cast<server*>(static_cast<std::uintptr_t>(handle));

    delete server_;
    return static_cast<jboolean>(true);
}
