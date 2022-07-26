package com.nautilus_technologies.tsubakuro.impl.low.sql.testing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.nautilus_technologies.tsubakuro.util.ByteBufferInputStream;
import com.nautilus_technologies.tsubakuro.channel.common.connection.sql.ResultSetWire;

public class ResultSetWireMock implements ResultSetWire {
    private ByteBufferBackedInputMock byteBufferInput;
    private ByteBuffer buf;

    class ByteBufferBackedInputMock extends ByteBufferInputStream {
        ByteBufferBackedInputMock(ByteBuffer byteBuffer) {
            super(byteBuffer);
        }

        boolean disposeUsedData(long length) {
            var size = buf.capacity() - (int) length;
            if (size == 0) {
                return false;
            }

            var newBuf = ByteBuffer.allocate(size);  // buffer created by allocateDirect() does not support .array(), so it is not appropriate here
            newBuf.put(buf.array(), (int) length, size);
            newBuf.rewind();
            buf = newBuf;
//            reset(newBuf);
            return true;
        }
    }

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
            byteBufferInput = new ByteBufferBackedInputMock(buf);
        }
        return byteBufferInput;
    }

    @Override
    public boolean disposeUsedData(long length) {
        return byteBufferInput.disposeUsedData(length);
    }

    @Override
    public void connect(String name) throws IOException {
    }

    @Override
    public void close() throws IOException {
    }
}