package com.tsurugidb.tsubakuro.channel.common.connection.wire;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * A simple implementation of {@link Response} which just returns payload data.
 */
public class SecondChannelResponse implements Response {

    private final Response response;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Creates a new instance with the response
     * @param response the ChannelResponse to receive BODY_HEAD message
     */
    public SecondChannelResponse(@Nonnull Response response) {
        Objects.requireNonNull(response);
        this.response = response;
    }

    @Override
    public boolean isMainResponseReady() {
        return response.isSecondResponseReady();
    }

    @Override
    public ByteBuffer waitForMainResponse() throws IOException, ServerException, InterruptedException {
        return response.waitForSecondResponse();
    }

    @Override
    public ByteBuffer waitForMainResponse(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
        try {
            return response.waitForSecondResponse(timeout, unit);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

    }

    @Override
    public void close() throws IOException, InterruptedException {
        closed.set(true);
    }
}
