package com.nautilus_technologies.tsubakuro;

public class HelloJNI {
    static {
        System.loadLibrary("template-native");
    }

    public native void sayHello(String name);

    public static void main(String[] args) {
        new HelloJNI().sayHello("JNI");
    }
}
