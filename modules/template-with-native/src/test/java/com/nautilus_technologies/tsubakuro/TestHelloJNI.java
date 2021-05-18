package com.nautilus_technologies.tsubakuro;

import org.junit.jupiter.api.Test;

public class TestHelloJNI {

    @Test
    void sayHello() {
        new HelloJNI().sayHello("JNI");
    }
}
