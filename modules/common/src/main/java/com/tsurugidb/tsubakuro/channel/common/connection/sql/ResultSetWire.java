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
package com.tsurugidb.tsubakuro.channel.common.connection.sql;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;

/**
 * ResultSetWire type.
 */
public interface ResultSetWire extends Closeable {
    /**
     * Provide multiple buffers as one input stream
     */
    abstract class ByteBufferBackedInput extends InputStream {
        /**
         * The source ByteBuffer.
         */
        protected ByteBuffer source;

        /**
         * The timeout for nextRow.
         */
        protected long timeoutNanos;

        /**
         * Creates a new instance.
         */
        public ByteBufferBackedInput() {
            this.source = ByteBuffer.allocate(0);
            this.timeoutNanos = 0;
        }

        @Override
        public int read() throws IOException {
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
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            while (true) {
                int count = Math.min(len, source.remaining());
                if (count > 0) {
                    source.get(b, off, count);
                    return count;
                }
                if (!next()) {
                    return -1;
                }
            }
        }

        @Override
        public int available() {
            if (source != null) {
                return source.remaining();
            }
            return 0;
        }

        /**
         * Sets timeout for nextRow.
         * @param timeout the maximum time to wait, or {@code 0} to disable
         * @param unit the time unit of {@code timeout}
         *
         * @since 1.9.0
         */
        public void setTimeout(long timeout, @Nonnull TimeUnit unit) {
            Objects.requireNonNull(unit);
            timeoutNanos = (WireImpl.MAX_TIMEOUT_DAYS > TimeUnit.DAYS.convert(timeout, unit)) ? unit.toNanos(timeout) : WireImpl.MAX_TIMEOUT_DAYS * 24 * 3600_000_000_000L;
        }

        /**
         * Fetches the next ByteBuffer.
         * @return {@code true} if the next ByteBuffer is available, {@code false} if no more ByteBuffer is available
         * @throws IOException if an I/O error occurs
         */
        protected abstract boolean next() throws IOException;

        /**
         * Getters, exists for avoid spotbugs warning.
         * @return the timeout in nanoseconds
         */
        protected long getTimeoutNanos() {
            return timeoutNanos;
        }
    }

    /**
     * Connect this to the wire specified by the name.
     * @param name the result set name specified by the SQL server.
     * @throws IOException connection error
     * @return ResultSetWire
     */
    default ResultSetWire connect(String name) throws IOException {
        return this;
    }

    /**
     * Provides an InputStream to retrieve the received data.
     * @return InputStream throuth which the record data from the SQL server will be provided.
     */
    default InputStream getByteBufferBackedInput() {
        throw new UnsupportedOperationException();
    }
}
