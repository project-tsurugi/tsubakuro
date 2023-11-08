package com.tsurugidb.tsubakuro.debug.impl;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.debug.proto.DebugRequest;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.debug.DebugClient;
import com.tsurugidb.tsubakuro.debug.LogLevel;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * An implementation of {@link DebugClient}.
 */
public class DebugClientImpl implements DebugClient {

    private final DebugService service;

    /**
     * Attaches to the datastore service in the current session.
     * @param session the current session
     * @return the datastore service client
     */
    public static DebugClientImpl attach(@Nonnull Session session) {
        Objects.requireNonNull(session);
        return new DebugClientImpl(new DebugServiceStub(session));
    }

    /**
     * Creates a new instance.
     * @param service the service stub
     */
    public DebugClientImpl(@Nonnull DebugService service) {
        Objects.requireNonNull(service);
        this.service = service;
    }

    @Override
    public FutureResponse<Void> logging(@Nonnull String message) throws IOException {
        Objects.requireNonNull(message);
        var builder = DebugRequest.Logging.newBuilder()
                .setMessage(message);
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<Void> logging(@Nonnull LogLevel level, @Nonnull String message) throws IOException {
        Objects.requireNonNull(level);
        Objects.requireNonNull(message);
        var builder = DebugRequest.Logging.newBuilder()
                .setLevel(level.getMapping())
                .setMessage(message);
        return service.send(builder.build());
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        service.close();
    }
}
