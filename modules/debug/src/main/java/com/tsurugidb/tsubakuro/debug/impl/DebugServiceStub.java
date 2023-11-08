package com.tsurugidb.tsubakuro.debug.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.tsurugidb.debug.proto.DebugRequest;
import com.tsurugidb.debug.proto.DebugResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.MainResponseProcessor;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.debug.DebugServiceCode;
import com.tsurugidb.tsubakuro.debug.DebugServiceException;
import com.tsurugidb.tsubakuro.exception.BrokenResponseException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * An implementation of {@link DebugService} communicate to the debugging service.
 */
public class DebugServiceStub implements DebugService {

    static final Logger LOG = LoggerFactory.getLogger(DebugServiceStub.class);

    /**
     * The datastore service ID.
     */
    public static final int SERVICE_ID = Constants.SERVICE_ID_DEBUG;

    private final Session session;

    /**
     * Creates a new instance.
     * @param session the current session
     */
    public DebugServiceStub(@Nonnull Session session) {
        Objects.requireNonNull(session);
        this.session = session;
    }

    private static byte[] toDelimitedByteArray(DebugRequest.Request request) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            request.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        }
    }

    static DebugServiceException newUnknown(@Nonnull DebugResponse.UnknownError message) {
        assert message != null;
        return new DebugServiceException(DebugServiceCode.UNKNOWN, message.getMessage());
    }

    static BrokenResponseException newResultNotSet(
            @Nonnull Class<? extends Message> aClass, @Nonnull String name) {
        assert aClass != null;
        assert name != null;
        return new BrokenResponseException(MessageFormat.format(
                "{0}.{1} is not set",
                aClass.getSimpleName(),
                name));
    }

    static class LoggingProcessor implements MainResponseProcessor<Void> {
        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = DebugResponse.Logging.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                // OK
                return null;

            case UNKNOWN_ERROR:
                throw newUnknown(message.getUnknownError());

            case RESULT_NOT_SET:
                throw newResultNotSet(message.getClass(), "result"); //$NON-NLS-1$
            }
            throw new AssertionError(message); // may not occur
        }
    }

    @Override
    public FutureResponse<Void> send(@Nonnull DebugRequest.Logging request) throws IOException {
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(
                    DebugRequest.Request.newBuilder()
                        .setLogging(request)
                        .build()),
                new LoggingProcessor().asResponseProcessor());
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        // do nothing
    }
}
