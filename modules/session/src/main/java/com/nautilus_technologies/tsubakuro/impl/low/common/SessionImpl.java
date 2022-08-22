package com.nautilus_technologies.tsubakuro.impl.low.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.Timeout;
import com.nautilus_technologies.tsubakuro.channel.common.connection.wire.Wire;
import com.nautilus_technologies.tsubakuro.channel.common.connection.wire.Response;
import com.nautilus_technologies.tsubakuro.channel.common.connection.wire.ResponseProcessor;
import com.nautilus_technologies.tsubakuro.channel.common.connection.Credential;
import com.nautilus_technologies.tsubakuro.channel.common.connection.ForegroundFutureResponse;  // FIXME move Session.java to com.nautilus_technologies.tsubakuro.channel.common
import com.nautilus_technologies.tsubakuro.channel.common.connection.BackgroundFutureResponse;  // FIXME same

/**
 * SessionImpl type.
 */
public class SessionImpl implements Session {
    static final Logger LOG = LoggerFactory.getLogger(SessionImpl.class);

    private long timeout;
    private TimeUnit unit;

    private Wire wire;  // FIXME use Wire

    private final ExecutorService executor;
    private static final ThreadFactory THREAD_FACTORY = new ThreadFactory() {

        private final AtomicInteger serial = new AtomicInteger();

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(String.format("tsubakuro-worker-%04d", serial.incrementAndGet())); //$NON-NLS-1$
            t.setDaemon(true);
            return t;
        }
    };

    /**
     * Creates a new instance.
     * @param wire the underlying wire
     */
    public SessionImpl(@Nonnull Wire wire) {
        this(wire, Executors.newCachedThreadPool(THREAD_FACTORY));
    }

    /**
     * Creates a new instance.
     * @param wire the underlying wire
     * @param executor worker threads to process responses
     */
    public SessionImpl(@Nonnull Wire wire, @Nonnull ExecutorService executor) {
        Objects.requireNonNull(wire);
        Objects.requireNonNull(executor);
        this.wire = wire;
        this.executor = executor;
    }

    /**
     * Connect this session to the SQL server.
     *
     * Note. How to connect to a SQL server is implementation dependent.
     * This implementation assumes that the session wire connected to the database is given.
     *
     * @param Wire the wire that connects to the Database
     */
    @Override
    public void connect(Wire w) {
        wire = w;
    }

    @Override
    public <R> FutureResponse<R> send(
            int serviceId,
            @Nonnull byte[] payload,
            @Nonnull ResponseProcessor<R> processor,
            boolean background) throws IOException {
        Objects.requireNonNull(payload);
        Objects.requireNonNull(processor);
    
        if (Objects.isNull(wire)) {
            System.out.println("wire is null");
        }
        FutureResponse<? extends Response> future = wire.send(serviceId, payload);
        return convert(future, processor, background);
    }

    @Override
    public <R> FutureResponse<R> send(
            int serviceId,
            @Nonnull ByteBuffer payload,
            @Nonnull ResponseProcessor<R> processor,
            boolean background) throws IOException {
        Objects.requireNonNull(payload);
        Objects.requireNonNull(processor);
        FutureResponse<? extends Response> future = wire.send(serviceId, payload);
        return convert(future, processor, background);
    }

    private <R> FutureResponse<R> convert(
            @Nonnull FutureResponse<? extends Response> response,
            @Nonnull ResponseProcessor<R> processor,
            boolean background) {
        assert response != null;
        assert processor != null;
        if (background) {
            var f = new BackgroundFutureResponse<>(response, processor);
            executor.execute(f); // process in background thread
            return f;
        }
        return new ForegroundFutureResponse<>(response, processor);
    }

    @Override
    public FutureResponse<Void> updateCredential(@Nonnull Credential credential) throws IOException {
        Objects.requireNonNull(credential);
        return wire.updateCredential(credential);
    }

    @Override
    public FutureResponse<Void> updateExpirationTime(long t, @Nonnull TimeUnit u) throws IOException {
        return FutureResponse.returns(null);
    }

    /**
     * set timeout to close(), which won't timeout if this is not performed.
     * @param t time length until the close operation timeout
     * @param u unit of timeout
     */
    public void setCloseTimeout(long t, TimeUnit u) {
        timeout = t;
        unit = u;
    }

    @Override
    public void setCloseTimeout(@Nonnull Timeout to) {
        Objects.requireNonNull(to);
    }

    /**
     * Close the Session
     */
    @Override
    public void close() throws IOException, InterruptedException {
        // FIXME: close timeout
        if (Objects.nonNull(executor)) {
            executor.shutdownNow();
        }
        if (Objects.nonNull(wire)) {
            wire.close();
        }
    }

    /**
     * Creates a new instance, exist for SessionImpl.
     */
    public SessionImpl() {
        wire = null;
        executor = null;
    }

    @Override
    public Wire getWire() {
        return wire;
    }
}