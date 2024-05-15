package com.tsurugidb.tsubakuro.common.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.tsurugidb.core.proto.CoreRequest;
import com.tsurugidb.core.proto.CoreResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.ForegroundFutureResponse;  // FIXME move Session.java to com.tsurugidb.tsubakuro.channel.common
import com.tsurugidb.tsubakuro.channel.common.connection.wire.MainResponseProcessor;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.ResponseProcessor;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.ShutdownType;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.impl.SqlServiceStub;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.Timeout;

/**
 * SessionImpl type.
 */
public class SessionImpl implements Session {
    static final Logger LOG = LoggerFactory.getLogger(SessionImpl.class);

    /**
     * The Core service ID.
     */
    public static final int SERVICE_ID = Constants.SERVICE_ID_ROUTING;

    private final ServiceShelf services = new ServiceShelf();
    private final AtomicBoolean closed = new AtomicBoolean();
    private Wire wire;
    private Timeout closeTimeout;

    /**
     * Creates a new instance.
     * @param wire the underlying wire
     */
    public SessionImpl(@Nonnull Wire wire) {
        Objects.requireNonNull(wire);
        this.wire = wire;
    }

    /**
     * Creates a new instance, exist for SessionBuilder.
     */
    public SessionImpl() {
        this.wire = null;
    }

    /**
     * Connect this session to the SQL server.
     *
     * Note. How to connect to a SQL server is implementation dependent.
     * This implementation assumes that the session wire connected to the database is given.
     *
     * @param w the wire that connects to the Database
     */
    @Override
    public void connect(@Nonnull Wire w) {
        Objects.requireNonNull(w);
        wire = w;
    }

    @Override
    public <R> FutureResponse<R> send(
            int serviceId,
            @Nonnull byte[] payload,
            @Nonnull ResponseProcessor<R> processor) throws IOException {
        Objects.requireNonNull(payload);
        Objects.requireNonNull(processor);
        FutureResponse<? extends Response> future = wire.send(serviceId, payload);
        return convert(future, processor);
    }

    @Override
    public <R> FutureResponse<R> send(
            int serviceId,
            @Nonnull ByteBuffer payload,
            @Nonnull ResponseProcessor<R> processor) throws IOException {
        Objects.requireNonNull(payload);
        Objects.requireNonNull(processor);
        FutureResponse<? extends Response> future = wire.send(serviceId, payload);
        return convert(future, processor);
    }

    private <R> FutureResponse<R> convert(
            @Nonnull FutureResponse<? extends Response> response,
            @Nonnull ResponseProcessor<R> processor) {
        assert response != null;
        assert processor != null;
        return new ForegroundFutureResponse<>(response, processor);
    }

    private static CoreRequest.Request.Builder newRequest() {
        return CoreRequest.Request.newBuilder()
                .setServiceMessageVersionMajor(Session.SERVICE_MESSAGE_VERSION_MAJOR)
                .setServiceMessageVersionMinor(Session.SERVICE_MESSAGE_VERSION_MINOR);
    }

    @Override
    public FutureResponse<Void> updateCredential(@Nonnull Credential credential) throws IOException {
        Objects.requireNonNull(credential);
        return wire.updateCredential(credential);
    }

    static class UpdateExpirationTimeProcessor implements MainResponseProcessor<Void> {
        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = CoreResponse.UpdateExpirationTime.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                return null;

            case UNKNOWN_ERROR:
                throw newUnknown(message.getUnknownError());

            default:
                break;
            }
            throw new AssertionError(); // may not occur
        }
    }

    @Override
    public FutureResponse<Void> updateExpirationTime(long t, @Nonnull TimeUnit u) throws IOException {
        return send(
            SERVICE_ID,
            toDelimitedByteArray(newRequest()
                .setUpdateExpirationTime(CoreRequest.UpdateExpirationTime.newBuilder()
                    .setExpirationTime(u.toMillis(t)))
                .build()),
            new UpdateExpirationTimeProcessor().asResponseProcessor());
    }

    static class ShutdownProcessor implements MainResponseProcessor<Void> {
        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = CoreResponse.Shutdown.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            // No error checking is performed here,
            // as only tateyama's core diagnostic is accepted for shutdown response.
            return null;
        }
    }

    public FutureResponse<Void> shutdown(@Nonnull ShutdownType type) throws IOException {
        var shutdownMessageBuilder = CoreRequest.Shutdown.newBuilder();

        return send(
            SERVICE_ID,
                toDelimitedByteArray(newRequest()
                    .setShutdown(shutdownMessageBuilder.setType(type.type()))
                    .build()),
            new ShutdownProcessor().asResponseProcessor());
    }

    static CoreServiceException newUnknown() {
        return new CoreServiceException(CoreServiceCode.UNKNOWN);
    }

    @Override
    public void setCloseTimeout(Timeout timeout) {
        closeTimeout = timeout;
        services.setCloseTimeout(timeout);
    }

    @Override
    public Timeout getCloseTimeout() {
        return closeTimeout;
    }

    static class CloseAction implements Consumer<ServerResource> {
        @Override
        public void accept(ServerResource r)  {
            try {
                r.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (ServerException | InterruptedException e) {
                throw new UncheckedIOException(new IOException(e));
            }
        }
    }

    /**
     * Close the Session
     */
    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        if (!closed.getAndSet(true)) {
            // take care of the serviceStubs
            try {
                services.forEach(new CloseAction());
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
            // take care of the wire
            if (wire != null) {
                if (closeTimeout != null) {
                    wire.setCloseTimeout(closeTimeout);
                }
                wire.close();
            }
        }
    }

    @Override
    public Wire getWire() {
        return wire;
    }

    @Override
    public void put(@Nonnull ServerResource resource) {
        services.put(resource);
    }

    @Override
    public void remove(@Nonnull ServerResource resource) {
        services.remove(resource);
    }

    static CoreServiceException newUnknown(@Nonnull CoreResponse.UnknownError message) {
        assert message != null;
        return new CoreServiceException(CoreServiceCode.UNKNOWN, message.getMessage());
    }

    static byte[] toDelimitedByteArray(Message request) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            request.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        }
    }

    // for diagnostic
    static class ServiceInfoAction implements Consumer<ServerResource> {
        String diagnosticInfo = "";

        @Override
        public void accept(ServerResource r) {
            if (r instanceof SqlServiceStub) {
                diagnosticInfo += ((SqlServiceStub) r).diagnosticInfo();
            }
        }
        public String diagnosticInfo() {
            return diagnosticInfo;
        }
    }
    public String diagnosticInfo() {
        if (!closed.get()) {
            String sessionId = "";
            if (wire instanceof WireImpl) {
                sessionId = Long.valueOf(((WireImpl) wire).sessionId()).toString();
            }
            String diagnosticInfo = "session " + sessionId + System.getProperty("line.separator");

            var serviceInfoAction = new ServiceInfoAction();
            services.forEach(serviceInfoAction);
            return diagnosticInfo + serviceInfoAction.diagnosticInfo() + wire.diagnosticInfo() + System.getProperty("line.separator");
        }
        return "";
    }
}
