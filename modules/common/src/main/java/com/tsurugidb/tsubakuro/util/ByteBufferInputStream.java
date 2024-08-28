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
package com.tsurugidb.tsubakuro.util;

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
