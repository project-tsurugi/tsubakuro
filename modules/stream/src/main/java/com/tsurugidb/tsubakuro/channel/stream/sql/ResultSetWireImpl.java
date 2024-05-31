package com.tsurugidb.tsubakuro.channel.stream.sql;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.stream.StreamLink;

/**
 * ResultSetWireImpl type.
 */
public class ResultSetWireImpl implements ResultSetWire {
    private final StreamLink streamLink;
    private final ResultSetBox resultSetBox;
    private final HashMap<Integer, LinkedList<byte[]>> lists = new HashMap<>();
    private final ConcurrentLinkedQueue<byte[]> queues = new ConcurrentLinkedQueue<>();
    private ByteBufferBackedInput byteBufferBackedInput;
    private boolean eor;
    private IOException exception;

    class ByteBufferBackedInputForStream extends ByteBufferBackedInput {
        private final ResultSetWireImpl resultSetWireImpl;

        ByteBufferBackedInputForStream(ByteBuffer source, ResultSetWireImpl resultSetWireImpl) {
            super(source);
            this.resultSetWireImpl = resultSetWireImpl;
        }

        @Override
        protected boolean next() {
            try {
                var buffer = receive();
                if (buffer == null) {
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
     * @param streamLink the stream object of the Wire
     */
    public ResultSetWireImpl(StreamLink streamLink) {
        this.streamLink = streamLink;
        this.resultSetBox = streamLink.getResultSetBox();
        this.byteBufferBackedInput = null;
        this.eor = false;
        this.exception = null;
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
        resultSetBox.register(name, this);
        return this;
    }

    /**
     * Provides the Input to retrieve the received data.
     */
    @Override
    public InputStream getByteBufferBackedInput() {
        if (byteBufferBackedInput == null) {
            try {
                var buffer = receive();
                if (buffer != null) {
                    byteBufferBackedInput = new ByteBufferBackedInputForStream(ByteBuffer.wrap(buffer), this);
                } else {
                    byteBufferBackedInput = new ByteBufferBackedInputForStream(ByteBuffer.allocate(0), this);
                }
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
    }

    /**
     * Receive resultSet payload
     */
    private byte[] receive() throws IOException {
        while (true) {
            var n = streamLink.messageNumber();
            if (!queues.isEmpty()) {
                return queues.poll();
            }
            if (eor) {
                return null;
            }
            if (exception != null) {
                throw exception;
            }
            try {
                streamLink.pullMessage(n, 0, null);
            } catch (TimeoutException e) {
                throw new IOException(e);
            }
        }
    }

    public void add(int writerId, byte[] payload) {
        if (!lists.containsKey(writerId)) {
            lists.put(writerId, new LinkedList<byte[]>());
        }
        var targetList = lists.get(writerId);
        if (payload != null) {
            targetList.add(payload);
        } else {
            queues.addAll(targetList);
            targetList.clear();
        }
    }

    void endOfRecords() {
        eor = true;
    }

    void endOfRecords(IOException e) {
        exception = e;
    }

    String linkLostMessage() {
        return streamLink.linkLostMessage();
    }
}
