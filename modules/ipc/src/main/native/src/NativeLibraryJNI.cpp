#include <com_tsurugidb_tsubakuro_channel_ipc_NativeLibrary.h>

#include <string_view>

#if !defined(TSUBAKURO_LIBRARY_VERSION)
#error TSUBAKURO_LIBRARY_VERSION is required
#endif

#define stringify(s) stringify_internal(s)
#define stringify_internal(s) #s

JNIEXPORT jbyteArray JNICALL Java_com_tsurugidb_tsubakuro_channel_ipc_NativeLibrary_getNativeLibraryVersionNative
(JNIEnv *env, jclass)
{
    std::string_view text { stringify(TSUBAKURO_LIBRARY_VERSION) };
    if (text.empty()) {
        return nullptr;
    }
    auto result = env->NewByteArray(static_cast<jint>(text.size()));
    if (result == nullptr) {
        return nullptr;
    }
    static_assert(sizeof(jbyte) == sizeof(char));
    env->SetByteArrayRegion(result, 0, static_cast<jsize>(text.size()), reinterpret_cast<jbyte const*>(text.data())); // NOLINT
    return result;
}
