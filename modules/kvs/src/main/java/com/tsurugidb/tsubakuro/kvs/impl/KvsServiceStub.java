package com.tsurugidb.tsubakuro.kvs.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.tsurugidb.kvs.proto.KvsRequest;
import com.tsurugidb.kvs.proto.KvsResponse;
import com.tsurugidb.kvs.proto.KvsTransaction;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.MainResponseProcessor;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.exception.BrokenResponseException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.BatchResult;
import com.tsurugidb.tsubakuro.kvs.GetResult;
import com.tsurugidb.tsubakuro.kvs.KvsServiceCode;
import com.tsurugidb.tsubakuro.kvs.KvsServiceException;
import com.tsurugidb.tsubakuro.kvs.PutResult;
import com.tsurugidb.tsubakuro.kvs.RecordCursor;
import com.tsurugidb.tsubakuro.kvs.RemoveResult;
import com.tsurugidb.tsubakuro.kvs.TransactionHandle;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResourceHolder;

/**
 * An implementation of {@link KvsService} communicate to the KVS service.
 */
public class KvsServiceStub implements KvsService {

    static final Logger LOG = LoggerFactory.getLogger(KvsServiceStub.class);

    /**
     * The KVS service ID.
     */
    public static final int SERVICE_ID = Constants.SERVICE_ID_KVS;

    private final Session session;

    private final ServerResourceHolder resources = new ServerResourceHolder();

    /**
     * Creates a new instance.
     * @param session the current session
     */
    public KvsServiceStub(@Nonnull Session session) {
        Objects.requireNonNull(session);
        this.session = session;
    }

    static KvsServiceException newUnknown(@Nonnull KvsResponse.UnknownError message) {
        assert message != null;
        return new KvsServiceException(KvsServiceCode.UNKNOWN, message.getMessage());
    }

    static BrokenResponseException newResultNotSet(@Nonnull Class<? extends Message> aClass, @Nonnull String name) {
        assert aClass != null;
        assert name != null;
        return new BrokenResponseException(MessageFormat.format("{0}.{1} is not set", aClass.getSimpleName(), name));
    }

    private static byte[] toDelimitedByteArray(KvsRequest.Request request) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            request.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    @Override
    public KvsTransaction.Handle extract(@Nonnull TransactionHandle handle) {
        if (handle instanceof TransactionHandleImpl) {
            var imp = (TransactionHandleImpl) handle;
            var builder = KvsTransaction.Handle.newBuilder().setSystemId(imp.getSystemId());
            return builder.build();
        }
        throw new AssertionError(); // may not occur
    }

    class BeginProcessor implements MainResponseProcessor<TransactionHandle> {

        @Override
        public TransactionHandle process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = KvsResponse.Begin.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                var tranHandle = message.getSuccess().getTransactionHandle();
                return resources.register(new TransactionHandleImpl(tranHandle.getSystemId()));

            case UNKNOWN_ERROR:
                throw newUnknown(message.getUnknownError());

            case RESULT_NOT_SET:
                throw newResultNotSet(message.getClass(), "result"); //$NON-NLS-1$
            }
            throw new AssertionError(); // may not occur
        }
    }


    @Override
    public FutureResponse<TransactionHandle> send(@Nonnull KvsRequest.Begin request) throws IOException {
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(SERVICE_ID, toDelimitedByteArray(KvsRequest.Request.newBuilder().setBegin(request).build()),
                new BeginProcessor().asResponseProcessor());
    }

    static class CommitProcessor implements MainResponseProcessor<Void> {
        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = KvsResponse.Commit.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                return null;

            case UNKNOWN_ERROR:
                throw newUnknown(message.getUnknownError());

            case RESULT_NOT_SET:
                throw newResultNotSet(message.getClass(), "result"); //$NON-NLS-1$
            }
            throw new AssertionError(); // may not occur
        }
    }

    @Override
    public FutureResponse<Void> send(@Nonnull KvsRequest.Commit request) throws IOException {
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(SERVICE_ID, toDelimitedByteArray(KvsRequest.Request.newBuilder().setCommit(request).build()),
                new CommitProcessor().asResponseProcessor());
    }

    static class RollbackProcessor implements MainResponseProcessor<Void> {
        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = KvsResponse.Rollback.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                return null;

            case UNKNOWN_ERROR:
                throw newUnknown(message.getUnknownError());

            case RESULT_NOT_SET:
                throw newResultNotSet(message.getClass(), "result"); //$NON-NLS-1$
            }
            throw new AssertionError(); // may not occur
        }
    }

    @Override
    public FutureResponse<Void> send(@Nonnull KvsRequest.Rollback request) throws IOException {
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(SERVICE_ID, toDelimitedByteArray(KvsRequest.Request.newBuilder().setRollback(request).build()),
                new RollbackProcessor().asResponseProcessor());
    }

    static class CloseTransactionProcessor implements MainResponseProcessor<Void> {

        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = KvsResponse.CloseTransaction.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                return null;

            case UNKNOWN_ERROR:
                throw newUnknown(message.getUnknownError());

            case RESULT_NOT_SET:
                throw newResultNotSet(message.getClass(), "result"); //$NON-NLS-1$
            }
            throw new AssertionError(); // may not occur
        }
    }

    @Override
    public FutureResponse<Void> send(@Nonnull KvsRequest.CloseTransaction request) throws IOException {
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(SERVICE_ID,
                toDelimitedByteArray(KvsRequest.Request.newBuilder().setCloseTransaction(request).build()),
                new CloseTransactionProcessor().asResponseProcessor());
    }

    static class GetProcessor implements MainResponseProcessor<GetResult> {
        @Override
        public GetResult process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = KvsResponse.Get.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                var records = message.getSuccess().getRecordsList();
                return new GetResultImpl(records);

            case UNKNOWN_ERROR:
                throw newUnknown(message.getUnknownError());

            case RESULT_NOT_SET:
                throw newResultNotSet(message.getClass(), "result"); //$NON-NLS-1$
            }
            throw new AssertionError(); // may not occur
        }
    }

    @Override
    public FutureResponse<GetResult> send(@Nonnull KvsRequest.Get request) throws IOException {
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(SERVICE_ID, toDelimitedByteArray(KvsRequest.Request.newBuilder().setGet(request).build()),
                new GetProcessor().asResponseProcessor());
    }

    static class PutProcessor implements MainResponseProcessor<PutResult> {
        @Override
        public PutResult process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = KvsResponse.Put.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                return new PutResultImpl(message.getSuccess().getWritten());

            case UNKNOWN_ERROR:
                throw newUnknown(message.getUnknownError());

            case RESULT_NOT_SET:
                throw newResultNotSet(message.getClass(), "result"); //$NON-NLS-1$
            }
            throw new AssertionError(); // may not occur
        }
    }

    @Override
    public FutureResponse<PutResult> send(@Nonnull KvsRequest.Put request) throws IOException {
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(SERVICE_ID, toDelimitedByteArray(KvsRequest.Request.newBuilder().setPut(request).build()),
                new PutProcessor().asResponseProcessor());
    }

    static class RemoveProcessor implements MainResponseProcessor<RemoveResult> {
        @Override
        public RemoveResult process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = KvsResponse.Remove.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                return new RemoveResultImpl(message.getSuccess().getRemoved());

            case UNKNOWN_ERROR:
                throw newUnknown(message.getUnknownError());

            case RESULT_NOT_SET:
                throw newResultNotSet(message.getClass(), "result"); //$NON-NLS-1$
            }
            throw new AssertionError(); // may not occur
        }
    }

    @Override
    public FutureResponse<RemoveResult> send(@Nonnull KvsRequest.Remove request) throws IOException {
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(SERVICE_ID, toDelimitedByteArray(KvsRequest.Request.newBuilder().setRemove(request).build()),
                new RemoveProcessor().asResponseProcessor());
    }

    @Override
    public FutureResponse<RecordCursor> send(@Nonnull KvsRequest.Scan request) throws IOException {
        // TODO
        throw new UnsupportedOperationException(String.valueOf(request));
    }

    @Override
    public FutureResponse<BatchResult> send(@Nonnull KvsRequest.Batch request) throws IOException {
        // TODO
        throw new UnsupportedOperationException(String.valueOf(request));
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        session.close();
    }

}
