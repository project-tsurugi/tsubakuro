package com.tsurugidb.tsubakuro.sql.impl.testing;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

public class ResultSetWireMock implements ResultSetWire {
    private ByteBufferInputStream byteBufferInput;
    private ByteBuffer buf;

    public ResultSetWireMock(ByteBuffer buf) {
        byteBufferInput = null;
        this.buf = buf;
    }

    public ResultSetWireMock(byte[] ba) {
        this(ByteBuffer.wrap(ba));
    }

    @Override
    public ByteBufferInputStream getByteBufferBackedInput() {
        if (byteBufferInput == null) {
            byteBufferInput = new ByteBufferInputStream(buf);
        }
        return byteBufferInput;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}