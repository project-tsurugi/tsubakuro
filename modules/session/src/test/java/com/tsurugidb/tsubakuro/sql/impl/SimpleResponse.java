package com.tsurugidb.tsubakuro.sql.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.tsurugidb.tsubakuro.sql.SqlServiceException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ChannelResponse;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

/**
 * A simple implementation of {@link Response} which just returns payload data.
 */
public class SimpleResponse implements Response {

    private final ByteBuffer main;

    private final ByteBuffer metadata;

    private final ByteBuffer relation;

//    private final ByteBuffer status;

    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Creates a new instance.
     * @param main the main response data
     * @param sub the sub response data
     */
    public SimpleResponse(ByteBuffer main, ByteBuffer metadata, ByteBuffer relation) {
        Objects.requireNonNull(main);
        this.main = main;
        this.metadata = metadata;
        this.relation = relation;
    }

    /**
     * Creates a new instance.
     * @param main the main response data
     * @param subMap map of sub response ID and its data
     */
    public SimpleResponse(ByteBuffer main) {
        this(main, null, null);
    }

    @Override
    public boolean isMainResponseReady() {
        return true;
    }

    @Override
    public ByteBuffer waitForMainResponse() {
        if (Objects.nonNull(main)) {
            return main;
        }
        return null;
    }

    @Override
    public ByteBuffer waitForMainResponse(long timeout, TimeUnit unit) {
        return waitForMainResponse();
    }

    @Override
    public InputStream openSubResponse(String id) throws IOException,SqlServiceException, InterruptedException {
        if (id.equals(ChannelResponse.METADATA_CHANNEL_ID)) {
            if (Objects.nonNull(metadata)) {
                return new ByteBufferInputStream(metadata);
            }
            throw new SqlServiceException(SqlServiceCode.ERR_UNKNOWN, "metadata is not received");
        } else if (id.equals(ChannelResponse.RELATION_CHANNEL_ID)) {
            if (Objects.nonNull(relation)) {
                return new ByteBufferInputStream(relation);
            }
            throw new SqlServiceException(SqlServiceCode.ERR_UNKNOWN, "relation data is not set up");
        }
        throw new IOException("illegal SubResponse id");
    }

    private void checkOpen() {
        if (closed.get()) {
            throw new IllegalStateException("response was already closed");
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        closed.set(true);
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "SimpleResponse(main={0}, sub={1})",
                main.remaining());
    }

    public ByteBuffer getRelation() {
        return relation;
    }
}
