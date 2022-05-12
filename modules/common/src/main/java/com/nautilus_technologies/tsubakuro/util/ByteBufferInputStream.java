package com.nautilus_technologies.tsubakuro.util;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * An {@link InputStream} which reads from {@link ByteBuffer}.
 */
@NotThreadSafe
public class ByteBufferInputStream extends InputStream {

    private final ByteBuffer source;

    /**
     * Creates a new instance.
     * @param source the source buffer
     */
    public ByteBufferInputStream(@Nonnull ByteBuffer source) {
        Objects.requireNonNull(source);
        this.source = source;
    }

    /**
     * Returns the source buffer.
     * @return the source buffer
     */
    public ByteBuffer getSource() {
        return source;
    }

    @Override
    public int read() {
        if (source.hasRemaining()) {
            return source.get() & 0xff;
        }
        return -1;
    }

    @Override
    public int read(byte[] b) {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) {
        if (!source.hasRemaining()) {
            return -1;
        }
        int count = Math.min(len, source.remaining());
        if (count > 0) {
            source.get(b, off, count);
        }
        return count;
    }

    @Override
    public long skip(long n) {
        var count = (int) Math.min(n, source.remaining());
        source.position(source.position() + count);
        return count;
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public String toString() {
        return String.valueOf(source);
    }
}
