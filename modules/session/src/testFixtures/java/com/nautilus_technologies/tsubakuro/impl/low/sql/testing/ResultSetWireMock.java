package com.nautilus_technologies.tsubakuro.impl.low.sql.testing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.nautilus_technologies.tsubakuro.util.ByteBufferInputStream;
import com.nautilus_technologies.tsubakuro.channel.common.connection.sql.ResultSetWire;

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
        if (Objects.isNull(byteBufferInput)) {
            byteBufferInput = new ByteBufferInputStream(buf);
        }
        return byteBufferInput;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}