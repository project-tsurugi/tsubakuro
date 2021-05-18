#include "HelloJNIImpl.h"

#include <memory>
#include <functional>
#include <iostream>

#include <jni.h>
#include "com_nautilus_technologies_tsubakuro_HelloJNI.h"

using std::string;
using std::function;
using std::unique_ptr;
using std::cout;
using std::endl;

class jstring_deleter
{
    JNIEnv *m_env;
    jstring m_jstr;

public:

    jstring_deleter(JNIEnv *env, jstring jstr)
        : m_env(env)
        , m_jstr(jstr)
    {
    }

    void operator()(const char *cstr)
    {
        cout << "[DEBUG] Releasing " << cstr << endl;
        m_env->ReleaseStringUTFChars(m_jstr, cstr);
    }

};

const string ToString(JNIEnv *env, jstring jstr)
{
    jstring_deleter deleter(env, jstr);     // using a function object
    unique_ptr<const char, jstring_deleter> pcstr(
            env->GetStringUTFChars(jstr, JNI_FALSE),
            deleter );

    return string( pcstr.get() );
}

JNIEXPORT void JNICALL Java_com_nautilus_1technologies_tsubakuro_HelloJNI_sayHello
  (JNIEnv *env, jobject thisObj, jstring arg)
{
    (void)thisObj; //FIXME workaround for "error: unused parameter"
    DoSayHello(ToString(env, arg));
}

void DoSayHello(const string &name)
{
    cout << "Hello, " << name << endl;
}
