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
package com.tsurugidb.tsubakuro.common.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.tsurugidb.core.proto.CoreRequest;
import com.tsurugidb.core.proto.CoreResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.Disposer; 
import com.tsurugidb.tsubakuro.channel.common.connection.ForegroundFutureResponse;  // FIXME move Session.java to com.tsurugidb.tsubakuro.channel.common
import com.tsurugidb.tsubakuro.channel.common.connection.wire.MainResponseProcessor;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.ResponseProcessor;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.common.BlobInfo;
import com.tsurugidb.tsubakuro.common.BlobPathMapping;
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
    private final AtomicBoolean shutdownCompleted = new AtomicBoolean();
    private final Disposer disposer = new Disposer(this);
    private final BlobPathMapping blobPathMapping;
    private Wire wire;
    private Timeout closeTimeout;

    /**
     * The keep alive interval in milliseconds.
     */
    public static final int KEEP_ALIVE_INTERVAL = 60000;
    private final Timer timer = new Timer();
    private boolean doKeepAlive = false;

    private class KeepAliveTask extends TimerTask {
        final Timer timer;

        KeepAliveTask(Timer timer) {
            this.timer = timer;
        }

        public void run() {
            try {
                if (!closed.get()) {
                    updateExpirationTime().get();
                } else {
                    timer.cancel();
                }
            } catch (Exception ex) {
                timer.cancel();
            }
        }
    }

    /**
     * Creates a new instance, exist for SessionBuilder.
     * @param doKeepAlive activate keep alive chore when doKeepAlive is true
     * @param blobPathMapping path mapping used when passing blobs using file
     */
    public SessionImpl(boolean doKeepAlive, BlobPathMapping blobPathMapping) {
        this.wire = null;
        this.doKeepAlive = doKeepAlive;
        this.blobPathMapping = blobPathMapping;
    }

    /**
     * Creates a new instance with doKeepAlive is false and blobPathMapping is null, exist for tests.
     */
    public SessionImpl() {
        this.wire = null;
        this.doKeepAlive = false;
        this.blobPathMapping = null;
    }

    /**
     * Creates a new instance.
     * @deprecated use SessionImpl() after connect(wire)
     * @param wire the underlying wire
     */
    @Deprecated
    public SessionImpl(@Nonnull Wire wire) {
        Objects.requireNonNull(wire);
        this.wire = wire;
        this.doKeepAlive = false;
        this.blobPathMapping = null;
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
        if (doKeepAlive) {
            timer.scheduleAtFixedRate(new KeepAliveTask(timer), KEEP_ALIVE_INTERVAL, KEEP_ALIVE_INTERVAL);
        }
        if (wire instanceof WireImpl) {
            ((WireImpl) wire).setBlobPathMapping(blobPathMapping);
        }
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

    @Override
    public <R> FutureResponse<R> send(
        int serviceId,
        @Nonnull byte[] payload,
        @Nonnull List<? extends BlobInfo> blobs,
        @Nonnull ResponseProcessor<R> processor) throws IOException {
            FutureResponse<? extends Response> future = wire.send(serviceId, payload, blobs);
            return convert(future, processor);
    }

    @Override
    public <R> FutureResponse<R> send(
        int serviceId,
        @Nonnull ByteBuffer payload,
        @Nonnull List<? extends BlobInfo> blobs,
        @Nonnull ResponseProcessor<R> processor) throws IOException {
            FutureResponse<? extends Response> future = wire.send(serviceId, payload, blobs);
            return convert(future, processor);
        }

    private <R> FutureResponse<R> convert(
            @Nonnull FutureResponse<? extends Response> response,
            @Nonnull ResponseProcessor<R> processor) {
        assert response != null;
        assert processor != null;
        return new ForegroundFutureResponse<>(response, processor, disposer);
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

    private <R> FutureResponse<R> sendUrgent(
        int serviceId,
        @Nonnull byte[] payload,
        @Nonnull ResponseProcessor<R> processor) throws IOException {
        Objects.requireNonNull(payload);
        Objects.requireNonNull(processor);
        if (wire instanceof WireImpl) {
            FutureResponse<? extends Response> future = ((WireImpl) wire).sendUrgent(serviceId, payload);
            return convert(future, processor);
        }
        FutureResponse<? extends Response> future = wire.send(serviceId, payload);
        return convert(future, processor);
    }

    @Override
    public FutureResponse<Void> updateExpirationTime() throws IOException {
        return sendUrgent(
            SERVICE_ID,
            toDelimitedByteArray(newRequest()
                .setUpdateExpirationTime(CoreRequest.UpdateExpirationTime.newBuilder())
                .build()),
            new UpdateExpirationTimeProcessor().asResponseProcessor());
    }

    @Override
    public FutureResponse<Void> updateExpirationTime(long t, @Nonnull TimeUnit u) throws IOException {
        return sendUrgent(
            SERVICE_ID,
            toDelimitedByteArray(newRequest()
                .setUpdateExpirationTime(CoreRequest.UpdateExpirationTime.newBuilder()
                    .setExpirationTime(u.toMillis(t)))
                .build()),
            new UpdateExpirationTimeProcessor().asResponseProcessor());
    }

    class ShutdownProcessor implements MainResponseProcessor<Void> {
        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = CoreResponse.Shutdown.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            // No error checking is performed here,
            // as only tateyama's core diagnostic is accepted for shutdown response.
            shutdownCompleted.set(true);
            wireClose();
            return null;
        }
    }

    public FutureResponse<Void> shutdown(@Nonnull ShutdownType type) throws IOException {
        var shutdownMessageBuilder = CoreRequest.Shutdown.newBuilder();
        try {
            disposer.waitForFinishDisposal();
            return sendUrgent(
                SERVICE_ID,
                    toDelimitedByteArray(newRequest()
                        .setShutdown(shutdownMessageBuilder.setType(type.type()))
                        .build()),
                new ShutdownProcessor().asResponseProcessor());
        } catch (IOException e) {
            // if shutdown has been completed, it is OK to ignore this exception.
            if (!shutdownCompleted.get()) {
                throw e;
            }
            return FutureResponse.returns(null);
        }
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

    /**
     * Close the Session
     */
    @Override
    public void close() throws ServerException, IOException, InterruptedException {
// FIXME Remove the following line when the server implementation improves.
        disposer.prepareCloseAndIsEmpty();
// FIXME Revive these lines when the server implementation improves.
//        if (!disposer.prepareCloseAndIsEmpty()) {
//           return;
//        }

        if (!closed.getAndSet(true)) {
            timer.cancel();  // does not throw any exception

            Exception exception = null;
            // take care of the serviceStubs
            if (closeTimeout != null) {
                services.setCloseTimeout(closeTimeout);
            }
            for (var e : services.entries()) {
                try {
                    e.close();
                } catch (ServerException | IOException | InterruptedException fe) {
                    if (exception == null) {
                        exception = fe;
                    } else {
                        exception.addSuppressed(fe);
                    }
                }
            }

            try {
                wireClose();
            } catch (ServerException | IOException | InterruptedException se) {
                if (exception == null) {
                    exception = se;
                } else {
                    exception.addSuppressed(se);
                }
            }

            if (exception != null) {
                if (exception instanceof IOException) {
                    throw (IOException) exception;
                }
                if (exception instanceof InterruptedException) {
                    throw (InterruptedException) exception;
                }
                if (exception instanceof ServerException) {
                    throw (ServerException) exception;
                }
                if (exception instanceof RuntimeException) {
                    throw (RuntimeException) exception;
                }
                throw new AssertionError(exception);
            }

        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    private void wireClose()  throws ServerException, IOException, InterruptedException {
        // take care of the wire
        if (wire != null) {
            if (closeTimeout != null) {
                wire.setCloseTimeout(closeTimeout);
            }
            try {
                wire.close();
            } catch (ServerException | IOException | InterruptedException e) {
                // if shutdown has been completed, it is OK to ignore this exception.
                if (!shutdownCompleted.get()) {
                    throw e;
                }
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

    /**
     * Returns the blobPathMapping.
     * Supporsed to be used by SqlServiceStub
     * @return the blobPathMapping
     */
    public BlobPathMapping getBlobPathMapping() {
        return blobPathMapping;
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
