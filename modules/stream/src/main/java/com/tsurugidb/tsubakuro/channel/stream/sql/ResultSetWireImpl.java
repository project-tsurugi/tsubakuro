/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.tsubakuro.channel.stream.sql;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private ByteBufferBackedInputForStream byteBufferBackedInput;
    private boolean eor;
    private IOException exception;

    static final Logger LOG = LoggerFactory.getLogger(ResultSetWireImpl.class);

    class ByteBufferBackedInputForStream extends ByteBufferBackedInput {
        private final ResultSetWireImpl resultSetWireImpl;

        ByteBufferBackedInputForStream(ResultSetWireImpl resultSetWireImpl) {
            this.resultSetWireImpl = resultSetWireImpl;
        }

        @Override
        protected boolean next() throws IOException {
            var buffer = receive(timeoutNanos);
            if (buffer == null) {
                return false;
            }
            source = ByteBuffer.wrap(buffer);
            return true;
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
            byteBufferBackedInput = new ByteBufferBackedInputForStream(this);
            return byteBufferBackedInput;
        }
        return byteBufferBackedInput;
    }

    /**
     * Close the wire
     */
    @Override
    public void close() throws IOException {
        // If the data in the ResultSet has not been received at the time the close is executed,
        // it is treated as if it had not been, so a short timeout value is used.
        long timeoutNanos = 1000000000L;
        while (!eor && exception == null) {
            var n = streamLink.messageNumber();
            try {
                streamLink.pullMessage(n, timeoutNanos, TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                throw new InterruptedIOException(e.getMessage());
            }
        }
    }

    /**
     * Receive resultSet payload
     */
    private byte[] receive(long timeoutNanos) throws IOException {
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
                streamLink.pullMessage(n, timeoutNanos, TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                throw new InterruptedIOException(e.getMessage());
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
