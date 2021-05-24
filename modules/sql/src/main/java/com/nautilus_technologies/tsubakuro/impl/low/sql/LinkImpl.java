package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.nio.ByteBuffer;

/**
 * SessionLinkImpl type.
 */
public class LinkImpl {
    static native void send(ByteBuffer buffer);
    static native ByteBuffer recv();
}
