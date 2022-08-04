package com.nautilus_technologies.tsubakuro.channel.stream.sql;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.nautilus_technologies.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.channel.stream.StreamWire;

/**
 * ResultSetWireImpl type.
 */
public class ResultSetWireImpl implements ResultSetWire {
    private StreamWire streamWire;
    private ResultSetBox resultSetBox;
    private int slot;
    private ByteBufferBackedInput byteBufferBackedInput;

    class ByteBufferBackedInputForStream extends ByteBufferBackedInput {
        private final ResultSetWireImpl resultSetWireImpl;

        ByteBufferBackedInputForStream(ByteBuffer source, ResultSetWireImpl resultSetWireImpl) {
            super(source);
            this.resultSetWireImpl = resultSetWireImpl;
        }

        @Override
        protected boolean next() {
            try {
                var buffer = resultSetBox.receive(slot).getPayload();
                if (Objects.isNull(buffer)) {
                    return false;
                }
                source = ByteBuffer.wrap(buffer);
                return true;
            } catch (IOException e) {
                System.err.println(e);
                e.printStackTrace();
                return false;
            }
        }

        @Override
        public void close() throws IOException {
            super.close();
            resultSetWireImpl.close();
        }
    }

    /**
     * Class constructor, called from FutureResultWireImpl.
     * @param streamWire the stream object of the Wire
     */
    public ResultSetWireImpl(StreamWire streamWire) {
        this.streamWire = streamWire;
        this.resultSetBox = streamWire.getResultSetBox();
        this.byteBufferBackedInput = null;
    }

    /**
     * Connect this to the wire specifiec by the name.
     * @param name the result set name specified by the SQL server.
     * @throws IOException connection error
     */
    public ResultSetWire connect(String name) throws IOException {
        if (name.length() == 0) {
            throw new IOException("ResultSet wire name is empty");
        }
        slot = resultSetBox.hello(name);
        return this;
    }

    /**
     * Provides the Input to retrieve the received data.
     */
    public InputStream getByteBufferBackedInput() {
        if (Objects.isNull(byteBufferBackedInput)) {
            try {
                var receivedData = resultSetBox.receive(slot);
                var buffer = receivedData.getPayload();
                if (Objects.isNull(buffer)) {
                    return null;
                }
                byteBufferBackedInput = new ByteBufferBackedInputForStream(ByteBuffer.wrap(buffer), this);
            } catch (IOException e) {
                System.err.println(e);
                e.printStackTrace();
                return null;
            }
        }
        return byteBufferBackedInput;
    }

    /**
     * Close the wire
     */
    public void close() throws IOException {
        while (true) {
            var entry = resultSetBox.receive(slot);
            if (Objects.isNull(entry.getPayload())) {
                break;
            }
        }
        streamWire.sendResutSetByeOk(slot);
        if (Objects.nonNull(byteBufferBackedInput)) {
            byteBufferBackedInput.close();
        }
    }
}
