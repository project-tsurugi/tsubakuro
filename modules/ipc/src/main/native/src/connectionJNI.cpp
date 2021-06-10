/*
 * Copyright 2019-2021 tsurugi project.
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
#include "com_nautilus_technologies_tsubakuro_impl_low_connection_IpcConnectorImpl.h"
#include "wire.h"

using namespace tsubakuro::common::wire;

static connection_queue* get_connection_queue(std::string_view db_name)
{
    boost::interprocess::managed_shared_memory segment(boost::interprocess::open_only, std::string(db_name).c_str());
    auto connection_queue_ptr = segment.find<connection_queue>(connection_queue::name).first;
    return connection_queue_ptr;
}

long connection_request(std::string_view db_name)
{
    auto connection_queue_ptr = get_connection_queue(db_name);
    return connection_queue_ptr->request();
}

bool connection_check(std::string_view db_name, long n)
{
    auto connection_queue_ptr = get_connection_queue(db_name);
    return connection_queue_ptr->check(n);
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_impl_low_connection_IpcConnectorImpl
 * Method:    requestConnectionNative
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_connection_IpcConnectorImpl_requestConnectionNative
(JNIEnv *env, jclass, jstring name)
{
    const char* name_ = env->GetStringUTFChars(name, NULL);
    if (name_ == NULL) return 0;
    jsize len_ = env->GetStringUTFLength(name);

    auto rv = connection_request(std::string_view(name_, len_));
    env->ReleaseStringUTFChars(name, name_);
    return rv;
}

/*
 * Class:     com_nautilus_technologies_tsubakuro_impl_low_connection_IpcConnectorImpl
 * Method:    checkConnectionNative
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_nautilus_1technologies_tsubakuro_impl_low_connection_IpcConnectorImpl_checkConnectionNative
(JNIEnv *env, jclass, jstring name, jlong handle)
{
    const char* name_ = env->GetStringUTFChars(name, NULL);
    if (name_ == NULL) return false;
    jsize len_ = env->GetStringUTFLength(name);

    auto rv = connection_check(std::string_view(name_, len_), handle);
    env->ReleaseStringUTFChars(name, name_);
    return rv;
}
