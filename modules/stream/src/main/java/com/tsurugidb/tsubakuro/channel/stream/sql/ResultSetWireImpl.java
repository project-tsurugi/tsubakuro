package com.tsurugidb.tsubakuro.channel.stream.sql;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.stream.StreamLink;

/**
 * ResultSetWireImpl type.
 */
public class ResultSetWireImpl implements ResultSetWire {
    private StreamLink streamWire;
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
                throw new UncheckedIOException(e);
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
    public ResultSetWireImpl(StreamLink streamWire) {
        this.streamWire = streamWire;
        this.resultSetBox = streamWire.getResultSetBox();
        this.byteBufferBackedInput = null;
    }

    /**
     * Connect this to the wire specifiec by the name.
     * @param name the result set name specified by the SQL server.
     * @throws IOException connection error
     */
    @Override
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
    @Override
    public InputStream getByteBufferBackedInput() {
        if (Objects.isNull(byteBufferBackedInput)) {
            try {
                var receivedData = resultSetBox.receive(slot);
                var buffer = receivedData.getPayload();
                if (Objects.isNull(buffer)) {
                    close();
                    return null;
                }
                byteBufferBackedInput = new ByteBufferBackedInputForStream(ByteBuffer.wrap(buffer), this);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return byteBufferBackedInput;
    }

    /**
     * Close the wire
     */
    @Override
    public void close() throws IOException {
        if (Objects.nonNull(streamWire)) {
            while (true) {
                var entry = resultSetBox.receive(slot);
                if (Objects.isNull(entry.getPayload())) {
                    break;
                }
            }
            streamWire.sendResutSetByeOk(slot);
            streamWire = null;
        }
    }
}
