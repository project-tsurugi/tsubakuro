package com.tsurugidb.tsubakuro.channel.common.connection.sql;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * ResultSetWire type.
 */
public interface ResultSetWire extends Closeable {
    /**
     * Provide multiple buffers as one input stream
     */
    abstract class ByteBufferBackedInput extends InputStream {
        protected ByteBuffer source;

        /**
         * Creates a new instance.
         * @param source the source buffer
         */
        public ByteBufferBackedInput(@Nonnull ByteBuffer source) {
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

        protected abstract boolean next();
    }

    /**
     * Connect this to the wire specifiec by the name.
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
