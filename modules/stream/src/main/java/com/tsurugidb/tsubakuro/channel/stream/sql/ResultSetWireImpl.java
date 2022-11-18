package com.tsurugidb.tsubakuro.channel.stream.sql;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.stream.StreamLink;

/**
 * ResultSetWireImpl type.
 */
public class ResultSetWireImpl implements ResultSetWire {
    private final StreamLink streamLink;
    private final ResultSetBox resultSetBox;
    private final Lock lock = new ReentrantLock();
    private final Condition availableCondition = lock.newCondition();
    private final ConcurrentLinkedQueue<ResultSetResponse> queues = new ConcurrentLinkedQueue<>();
    private ByteBufferBackedInput byteBufferBackedInput;
    private boolean closed;
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
                var buffer = receive().getPayload();
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
     * @param streamLink the stream object of the Wire
     */
    public ResultSetWireImpl(StreamLink streamLink) {
        this.streamLink = streamLink;
        this.resultSetBox = streamLink.getResultSetBox();
        this.byteBufferBackedInput = null;
        this.closed = false;
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
        if (Objects.isNull(byteBufferBackedInput)) {
            try {
                var buffer = receive().getPayload();
                if (Objects.nonNull(buffer)) {
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
     * Receive resultSet payload
     */
    private ResultSetResponse receive() throws IOException {
        while (true) {
            lock.lock();
            try {
                if (!queues.isEmpty()) {
                    return queues.poll();
                }
                if (eor) {
                    return new ResultSetResponse(0, null);
                }
                if (Objects.nonNull(exception)) {
                    throw exception;
                }
                availableCondition.await();
            } catch (InterruptedException e) {
                throw new IOException(e);
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Close the wire
     */
    @Override
    public void close() throws IOException {
        closed = true;
    }

    public void add(ResultSetResponse resultSetResponse) {
        queues.add(resultSetResponse);
        lock.lock();
        try {
            availableCondition.signal();
        } finally {
            lock.unlock();
        }
    }

    public void endOfRecords() {
        eor = true;
        if (!closed) {
            lock.lock();
            try {
                availableCondition.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    public void endOfRecords(IOException e) {
        exception = e;
        if (!closed) {
            lock.lock();
            try {
                availableCondition.signal();
            } finally {
                lock.unlock();
            }
        }
    }
}
