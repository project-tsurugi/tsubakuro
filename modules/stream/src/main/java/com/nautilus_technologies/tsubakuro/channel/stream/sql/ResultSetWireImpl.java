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

    class ByteBufferBackedInput extends InputStream {
        private ByteBuffer source;

        /**
         * Creates a new instance.
         * @param source the source buffer
         */
        ByteBufferBackedInput(ByteBuffer source) {
            Objects.requireNonNull(source);
            this.source = source;
        }
    
        @Override
        public int read() {
            while (true) {
                if (source.hasRemaining()) {
                    return source.get() & 0xff;
                }
                if (!next()) {
                    return -1;
                }
            }
        }
    
        @Override
        public int read(byte[] b, int off, int len) {
            int read = 0;
            while (true) {
                int count = Math.min((len - read), source.remaining());
                if (count > 0) {
                    source.get(b, (off + read), count);
                    read += count;
                    if (read == len) {
                        return read;
                    }
                }
                if (!next()) {
                    return -1;
                }
            }
        }

        boolean next() {
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
    public void connect(String name) throws IOException {
        if (name.length() == 0) {
            throw new IOException("ResultSet wire name is empty");
        }
        slot = resultSetBox.hello(name);
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
                byteBufferBackedInput = new ByteBufferBackedInput(ByteBuffer.wrap(buffer));
            } catch (IOException e) {
                System.err.println(e);
                e.printStackTrace();
                return null;
            }
        }
        return byteBufferBackedInput;
    }

    public boolean disposeUsedData(long length) throws IOException {
    // FIXME
    // When multiple writer is implemented, this becomes necessary.
    return true;
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
