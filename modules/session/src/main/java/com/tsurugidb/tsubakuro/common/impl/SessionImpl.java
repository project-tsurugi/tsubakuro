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
import java.util.concurrent.TimeoutException;
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
    private final AtomicBoolean cleanUpFinished = new AtomicBoolean();
    private final AtomicBoolean closeCleanUpRegistered = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final BlobPathMapping blobPathMapping;
    private final Disposer disposer = new Disposer();

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

    // for SqlServiceStubImpl only
    public Disposer disposer() {  
        return disposer;
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

    static class ShutdownProcessor implements MainResponseProcessor<Void> {
        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            // No error checking is performed here,
            // as only core diagnostic errors can occur for shutdown requests.
            var message = CoreResponse.Shutdown.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            return null;
        }
    }

    class ShutdownCleanUp implements FutureResponse<Void>, Disposer.DelayedShutdown {
        private final ShutdownType type;
        private FutureResponse<Void> future = null;
        private Exception exception = null;

        ShutdownCleanUp(ShutdownType type) {
            this.type = type;
        }
        @Override
        public synchronized void shutdown() throws IOException {
            if (future == null) {
                exception = cleanServiceStub();
                disposer.waitForEmpty();
                var shutdownMessageBuilder = CoreRequest.Shutdown.newBuilder();
                future = sendUrgent(
                    SERVICE_ID,
                        toDelimitedByteArray(newRequest()
                            .setShutdown(shutdownMessageBuilder.setType(type.type()))
                            .build()),
                    new ShutdownProcessor().asResponseProcessor());
            }
        }
        @Override
        public Void get() throws IOException, ServerException, InterruptedException {
            try {
                return get(0, null);
            } catch (TimeoutException e) {
                throw new AssertionError(e);
            }
        }
        @Override
        public Void get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
            if (future == null) {
                try {
                    shutdown();
                } catch (IOException fe) {
                    addSuppressed(fe);
                }
            }
            try {
                if (future != null) {
                    if (timeout == 0 && unit == null) {
                        future.get();
                    } else {
                        future.get(timeout, unit);
                    }
                }
            } catch (IOException | ServerException | InterruptedException | TimeoutException fe) {
                addSuppressed(fe);
            }
            try {
                doClose();
            } catch (IOException | ServerException | InterruptedException fe) {
                addSuppressed(fe);
            }
            throwException();
            return null;
        }
        @Override
        public boolean isDone() {
            if (future == null) {
                return false;
            }
            return future.isDone();
        }
        @Override
        public void close() throws IOException, ServerException, InterruptedException {
            if (future == null) {
                shutdown();
            }
            future.close();
        }

        private void addSuppressed(Exception exceptionNew) {
            if (exception == null) {
                exception = exceptionNew;
            } else {
                exception.addSuppressed(exceptionNew);
            }
        }
        private void throwException() throws IOException, ServerException, InterruptedException, TimeoutException {
            if (exception != null) {
                if (exception instanceof IOException) {
                    throw (IOException) exception;
                }
                if (exception instanceof ServerException) {
                    throw (ServerException) exception;
                }
                if (exception instanceof TimeoutException) {
                    throw (TimeoutException) exception;
                }
                if (exception instanceof InterruptedException) {
                    throw (InterruptedException) exception;
                }
                if (exception instanceof RuntimeException) {
                    throw (RuntimeException) exception;
                }
                throw new AssertionError(exception);
            }
        }
    }

    public FutureResponse<Void> shutdown(@Nonnull ShutdownType type) throws IOException {
        ShutdownCleanUp shutdownCleanUp = new ShutdownCleanUp(type);
        disposer.registerDelayedShutdown(shutdownCleanUp);
        return shutdownCleanUp;
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

    class CloseCleanUp implements Disposer.DelayedClose {
        public void delayedClose() throws ServerException, IOException, InterruptedException {
            var exception = cleanServiceStub();

            try {
                doClose();
            } catch (ServerException | IOException | InterruptedException fe) {
                if (exception == null) {
                    exception = fe;
                } else {
                    exception.addSuppressed(fe);
                }
            }
            throwException(exception);
        }
    }

    /**
     * Close the Session
     */
    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        if (!closeCleanUpRegistered.getAndSet(true)) {
            disposer.registerDelayedClose(new CloseCleanUp());
        }
    }

    private Exception cleanServiceStub() {
        if (!cleanUpFinished.getAndSet(true)) {
            Exception exception = null;

            // take care of the serviceStubs
            if (closeTimeout != null) {
                services.setCloseTimeout(closeTimeout);
            }
            for (var e : services.entries()) {
                try {
                    e.close();
                    services.remove(e);
                } catch (ServerException | IOException | InterruptedException fe) {
                    if (exception == null) {
                        exception = fe;
                    } else {
                        exception.addSuppressed(fe);
                    }
                }
            }
            return exception;
        }
        return null;
    }

    private void doClose() throws ServerException, IOException, InterruptedException {
        waitForDisposerEmpty();
        if (!closed.getAndSet(true)) {
            timer.cancel();  // does not throw any exception
            wireClose();
        }
    }

    private static void throwException(Exception exception) throws IOException, ServerException, InterruptedException {
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
            wire.close();
        }
    }

    /**
     * Wait until there are no more ServerResources in the Disposer to process.
     * It can be called even when the Session is not shutdown or closed. However, in that case,
     * there may be a possibility that a ServerResource to be processed is added to the Disposer later.
     * NOTE: This method is provided for testing purposes only.
     */
    public void waitForDisposerEmpty() {
        disposer.waitForEmpty();
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
