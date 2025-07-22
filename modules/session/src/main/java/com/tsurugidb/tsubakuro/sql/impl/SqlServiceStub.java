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
package com.tsurugidb.tsubakuro.sql.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.sql.proto.SqlError;
import com.tsurugidb.tsubakuro.channel.common.connection.Disposer;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.MainResponseProcessor;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.ResponseProcessor;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ChannelResponse;
import com.tsurugidb.tsubakuro.common.BlobInfo;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.client.SessionAlreadyClosedException;
import com.tsurugidb.tsubakuro.exception.BrokenResponseException;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.BlobReference;
import com.tsurugidb.tsubakuro.sql.ClobReference;
import com.tsurugidb.tsubakuro.sql.ExecuteResult;
import com.tsurugidb.tsubakuro.sql.LargeObjectCache;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.SearchPath;
import com.tsurugidb.tsubakuro.sql.SqlService;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;
import com.tsurugidb.tsubakuro.sql.StatementMetadata;
import com.tsurugidb.tsubakuro.sql.TableList;
import com.tsurugidb.tsubakuro.sql.TableMetadata;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.sql.TransactionStatus;
import com.tsurugidb.tsubakuro.sql.io.BlobException;
import com.tsurugidb.tsubakuro.sql.io.StreamBackedValueInput;
import com.tsurugidb.tsubakuro.sql.util.SqlRequestUtils;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Owner;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.ServerResourceHolder;
import com.tsurugidb.tsubakuro.util.Timeout;


/**
 * An interface to communicate with SQL service.
 */
public class SqlServiceStub implements SqlService {

    static final Logger LOG = LoggerFactory.getLogger(SqlServiceStub.class);

    /**
     * The SQL service ID.
     */
    public static final int SERVICE_ID = Constants.SERVICE_ID_SQL;

    static final String FORMAT_ID_LEGACY_EXPLAIN = "jogasaki-statement.json"; //$NON-NLS-1$

    static final long FORMAT_VERSION_LEGACY_EXPLAIN = 1;

    private final Session session;

    private final ServerResourceHolder futureResponses = new ServerResourceHolder();
    private final ServerResourceHolder resources = new ServerResourceHolder();

    private Timeout closeTimeout = Timeout.DISABLED;

    private Disposer disposer = null;

    /**
     * Creates a new instance.
     * @param session the current session
     */
    public SqlServiceStub(@Nonnull Session session) {
        Objects.requireNonNull(session);
        this.session = session;
        if (session instanceof SessionImpl) {
            this.disposer = ((SessionImpl) session).disposer();
        }
        this.closeTimeout = session.getCloseTimeout();
    }

    // just avoid programming error
    //    static Void newVoid(@Nonnull SqlResponse.Void message) {
    //        assert message != null;
    //        return null;
    //    }

    //    static SqlServiceException newEngineError(
    //            @Nonnull SqlResponse.EngineError message) {
    //        assert message != null;
    //        // FIXME impl
    //        return new SqlServiceException(SqlServiceCode.UNKNOWN, message.getMessageText());
    //    }

    static BrokenResponseException newResultNotSet(
            @Nonnull Class<? extends Message> aClass, @Nonnull String name) {
        assert aClass != null;
        assert name != null;
        return new BrokenResponseException(MessageFormat.format(
                "{0}.{1} is not set",
                aClass.getSimpleName(),
                name));
    }

    static BrokenResponseException newResultNotRecognized(
            @Nonnull Class<? extends Message> aClass, @Nonnull String name, @Nonnull Enum<?> kind) {
        assert aClass != null;
        assert name != null;
        return new BrokenResponseException(MessageFormat.format(
                "{0}.{1} is not recognized ({2})",
                aClass.getSimpleName(),
                name,
                kind));
    }

    class TransactionBeginProcessor implements MainResponseProcessor<Transaction> {
        private final AtomicReference<SqlResponse.Begin> detailResponseCache = new AtomicReference<>();
        FutureResponse<Transaction> futureResponse = null;

        FutureResponse<Transaction> setFutureResponse(FutureResponse<Transaction> r) {
            futureResponse = r;
            return r;
        }

        @Override
        public Transaction process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            if (futureResponse != null) {
                futureResponses.onClosed(futureResponse);
            }
            if (detailResponseCache.get() == null) {
                var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
                if (!SqlResponse.Response.ResponseCase.BEGIN.equals(response.getResponseCase())) {
                    // FIXME log error message
                    throw new IOException("response type is inconsistent with the request type");
                }
                detailResponseCache.set(response.getBegin());
            }
            var detailResponse = detailResponseCache.get();
            LOG.trace("receive (Begin): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.Begin.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
            }
            var transactionImpl = new TransactionImpl(detailResponse.getSuccess(), SqlServiceStub.this, resources, disposer);
            transactionImpl.setCloseTimeout(closeTimeout);
            synchronized (resources) {
                return resources.register(transactionImpl);
            }
        }
    }

    @Override
    public FutureResponse<Transaction> send(
            @Nonnull SqlRequest.Begin request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (Begin): {}", request); //$NON-NLS-1$
        var processor = new TransactionBeginProcessor();
        return processor.setFutureResponse(futureResponses.register(
            session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                processor.asResponseProcessor())));
    }

    class TransactionCommitProcessor implements MainResponseProcessor<Void> {
        private final AtomicReference<SqlResponse.ResultOnly> detailResponseCache = new AtomicReference<>();

        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            if (detailResponseCache.get() == null) {
                var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
                if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                    // FIXME log error message
                    throw new IOException("response type is inconsistent with the request type");
                }
                detailResponseCache.set(response.getResultOnly());
            }
            var detailResponse = detailResponseCache.get();
            LOG.trace("receive (commit): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
            }
            return null;
        }
    }

    @Override
    public FutureResponse<Void> send(
            @Nonnull SqlRequest.Commit request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (commit): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                new TransactionCommitProcessor().asResponseProcessor());
    }

    class TransactionRollbackProcessor implements MainResponseProcessor<Void> {
        private final AtomicReference<SqlResponse.ResultOnly> detailResponseCache = new AtomicReference<>();

        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            if (detailResponseCache.get() == null) {
                var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
                if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                    // FIXME log error message
                    throw new IOException("response type is inconsistent with the request type");
                }
                detailResponseCache.set(response.getResultOnly());
            }
            var detailResponse = detailResponseCache.get();
            LOG.trace("receive (rollback): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
            }
            return null;
        }
    }

    @Override
    public FutureResponse<Void> send(
            @Nonnull SqlRequest.Rollback request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (rollback): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                new TransactionRollbackProcessor().asResponseProcessor(false));
    }

    class StatementPrepareProcessor implements MainResponseProcessor<PreparedStatement> {
        private final AtomicReference<SqlResponse.Prepare> detailResponseCache = new AtomicReference<>();
        private final SqlRequest.Prepare request;
        FutureResponse<PreparedStatement> futureResponse = null;

        StatementPrepareProcessor(SqlRequest.Prepare request) {
            this.request = request;
        }
        FutureResponse<PreparedStatement> setFutureResponse(FutureResponse<PreparedStatement> r) {
            futureResponse = r;
            return r;
        }

        @Override
        public PreparedStatement process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            if (futureResponse != null) {
                futureResponses.onClosed(futureResponse);
            }
            if (detailResponseCache.get() == null) {
                var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
                if (!SqlResponse.Response.ResponseCase.PREPARE.equals(response.getResponseCase())) {
                    // FIXME log error message
                    throw new IOException("response type is inconsistent with the request type");
                }
                detailResponseCache.set(response.getPrepare());
            }
            var detailResponse = detailResponseCache.get();
            LOG.trace("receive (prepare): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.Prepare.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
            }
            var preparedStatementImpl = new PreparedStatementImpl(detailResponse.getPreparedStatementHandle(), SqlServiceStub.this, resources, request, disposer);
            preparedStatementImpl.setCloseTimeout(closeTimeout);
            synchronized (resources) {
                return resources.register(preparedStatementImpl);
            }
        }
    }

    @Override
    public FutureResponse<PreparedStatement> send(
            SqlRequest.Prepare request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (prepare): {}", request); //$NON-NLS-1$
        var processor = new StatementPrepareProcessor(request);
        return processor.setFutureResponse(futureResponses.register(
            session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                processor.asResponseProcessor())));
    }

    class StatementDisposeProcessor implements MainResponseProcessor<Void> {
        private final AtomicReference<SqlResponse.ResultOnly> detailResponseCache = new AtomicReference<>();

        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            if (detailResponseCache.get() == null) {
                var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
                if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                    // FIXME log error message
                    throw new IOException("response type is inconsistent with the request type");
                }
                detailResponseCache.set(response.getResultOnly());
            }
            var detailResponse = detailResponseCache.get();
            LOG.trace("receive (dispose prepared statement): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
            }
            return null;
        }
    }

    @Override
    public FutureResponse<Void> send(
            @Nonnull SqlRequest.DisposePreparedStatement request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (dispose prepared statement): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                new StatementDisposeProcessor().asResponseProcessor(false));
    }

    class DescribeStatementProcessor implements MainResponseProcessor<StatementMetadata> {
        private final AtomicReference<SqlResponse.Explain> detailResponseCache = new AtomicReference<>();

        @Override
        public StatementMetadata process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            if (detailResponseCache.get() == null) {
                var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
                if (!SqlResponse.Response.ResponseCase.EXPLAIN.equals(response.getResponseCase())) {
                    // FIXME log error message
                    throw new IOException("response type is inconsistent with the request type");
                }
                detailResponseCache.set(response.getExplain());
            }
            var detailResponse = detailResponseCache.get();
            LOG.trace("receive (explain): {}", detailResponse); //$NON-NLS-1$
            switch (detailResponse.getResultCase()) {
            case SUCCESS:
                return new StatementMetadataAdapter(detailResponse.getSuccess());

            case ERROR:
                var errorResponse = detailResponse.getError();
                throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());

            case RESULT_NOT_SET:
                break; // not recognized
            }
            throw newResultNotRecognized(SqlResponse.Explain.class, "result", detailResponse.getResultCase());
        }
    }

    @Override
    public FutureResponse<StatementMetadata> send(
            @Nonnull SqlRequest.Explain request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (explain): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                new DescribeStatementProcessor().asResponseProcessor());
    }

    @Override
    public FutureResponse<StatementMetadata> send(
            @Nonnull SqlRequest.ExplainByText request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (explain): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                new DescribeStatementProcessor().asResponseProcessor(false));
    }

    class DescribeTableProcessor implements MainResponseProcessor<TableMetadata> {
        private final AtomicReference<SqlResponse.DescribeTable> detailResponseCache = new AtomicReference<>();

        @Override
        public TableMetadata process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            if (detailResponseCache.get() == null) {
                var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
                if (!SqlResponse.Response.ResponseCase.DESCRIBE_TABLE.equals(response.getResponseCase())) {
                    // FIXME log error message
                    throw new IOException("response type is inconsistent with the request type");
                }
                detailResponseCache.set(response.getDescribeTable());
            }
            var detailResponse = detailResponseCache.get();
            LOG.trace("receive (describe table): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.DescribeTable.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
            }
            return new TableMetadataAdapter(detailResponse.getSuccess());
        }
    }

    @Override
    public FutureResponse<TableMetadata> send(
            @Nonnull SqlRequest.DescribeTable request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (describe table): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                new DescribeTableProcessor().asResponseProcessor(false));
    }

    class ExecuteProcessor implements MainResponseProcessor<ExecuteResult> {
        private final AtomicReference<SqlResponse.Response> responseCache = new AtomicReference<>();

        @Override
        public ExecuteResult process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            if (responseCache.get() == null) {
                responseCache.set(SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload)));
            }
            var response = responseCache.get();
            switch (response.getResponseCase()) {
                case EXECUTE_RESULT:
                    var detailResponse = response.getExecuteResult();
                    LOG.trace("receive (ExecuteResult): {}", detailResponse); //$NON-NLS-1$
                    if (SqlResponse.ExecuteResult.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                        var errorResponse = detailResponse.getError();
                        throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
                    }
                    return new ExecuteResultAdapter(detailResponse.getSuccess());
                case RESULT_ONLY:
                    var resultOnlyResponse = response.getResultOnly();
                    LOG.trace("receive (ResultOnly): {}", resultOnlyResponse); //$NON-NLS-1$
                    if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(resultOnlyResponse.getResultCase())) {
                        var errorResponse = resultOnlyResponse.getError();
                        throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
                    }
                    return new ExecuteResultAdapter();
            }
            throw new IOException("response type is inconsistent with the request type");
        }
    }

    @Override
    public FutureResponse<ExecuteResult> send(
            @Nonnull SqlRequest.ExecuteStatement request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (execute statement): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                new ExecuteProcessor().asResponseProcessor(false));
    }

    @Override
    public FutureResponse<ExecuteResult> send(
            @Nonnull SqlRequest.ExecutePreparedStatement request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (execute prepared statement): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                new ExecuteProcessor().asResponseProcessor());
    }

    @Override
    public FutureResponse<ExecuteResult> send(
            @Nonnull SqlRequest.ExecutePreparedStatement request, @Nonnull List<? extends BlobInfo> blobs) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (execute prepared statement): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                blobs,
                new ExecuteProcessor().asResponseProcessor());
    }

   @Override
    public FutureResponse<ExecuteResult> send(
            @Nonnull SqlRequest.Batch request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (batch): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                new ExecuteProcessor().asResponseProcessor());
    }

    class QueryProcessor extends AbstractResultSetProcessor<SqlResponse.Response> {
        Message request;
        FutureResponse<ResultSet> futureResponse = null;

        QueryProcessor(Message request) {
            super(resources);
            this.request = request;
        }
        FutureResponse<ResultSet> setFutureResponse(FutureResponse<ResultSet> r) {
            futureResponse = r;
            return r;
        }

        @Override
        SqlResponse.Response parse(@Nonnull ByteBuffer payload) throws IOException {
            var message = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive (execute query bodyhead): {}", message); //$NON-NLS-1$
            return message;
        }

        @Override
        void doTest(@Nonnull SqlResponse.Response message) throws IOException, ServerException, InterruptedException {
            if (SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(message.getResponseCase())) {
                var detailResponse = message.getResultOnly();
                if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                    var errorResponse = detailResponse.getError();
                    throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
                }
                return;
            }
            throw new AssertionError(); // should not occur
        }

        @Override
        public ResultSet process(Response response, Timeout timeout) throws IOException, ServerException, InterruptedException {
            Objects.requireNonNull(response);
            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            if (futureResponse != null) {
                futureResponses.onClosed(futureResponse);
            }
            try (
                var owner = Owner.of(response);
            ) {
                InputStream metadataInput;
                ResultSetMetadataAdapter metadata = null;
                while (true) {
                    Timeout timeoutHere = null;
                    if (timeout.value() > 0) {
                        timeoutHere = timeout;
                    } else if (closeTimeout != null) {
                        if (closeTimeout.value() > 0) {
                            timeoutHere = closeTimeout;
                        }
                    }
                    if (timeoutHere != null) {
                        try {
                            metadataInput = response.openSubResponse(ChannelResponse.METADATA_CHANNEL_ID, timeoutHere.value(), timeoutHere.unit());
                        } catch (TimeoutException e) {
                            throw new ResponseTimeoutException( "ResultSet transfer was not initiated by the server for " + timeoutHere.value() + " " + timeoutHere.unit(), e);
                        }
                    } else {
                        metadataInput = response.openSubResponse(ChannelResponse.METADATA_CHANNEL_ID);
                    }
                    if (metadataInput != null) {
                        try {
                            metadata = new ResultSetMetadataAdapter(SqlResponse.ResultSetMetadata.parseFrom(metadataInput));
                        } finally {
                            metadataInput.close();
                        }
                        break;
                    }
                    if (response.isMainResponseReady()) {
                        test(response);
                    }
                }
                var dataInput = response.openSubResponse(ChannelResponse.RELATION_CHANNEL_ID);
                SqlServiceStub.LOG.trace("result set metadata: {}", metadata); //$NON-NLS-1$
                var cursor = new ValueInputBackedRelationCursor(new StreamBackedValueInput(dataInput));
                String resultSetName = "";
                if (response instanceof ChannelResponse) {
                    resultSetName = ((ChannelResponse) response).resultSetName();
                }
                var resultSetImpl = new ResultSetImpl(resources, metadata, cursor, owner.release(), this, resultSetName, request);
                resultSetImpl.setCloseTimeout(closeTimeout);
                synchronized (resources) {
                    return resources.register(resultSetImpl);
                }
            }
        }

        @Override
        public boolean isMainResponseRequired() {
            return false;
        }
    }

    @Override
    public FutureResponse<ResultSet> send(
            @Nonnull SqlRequest.ExecuteQuery request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (execute query): {}", request); //$NON-NLS-1$
        var processor = new QueryProcessor(request);
        return processor.setFutureResponse(futureResponses.register(
            session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                processor)));
    }

    @Override
    public FutureResponse<ResultSet> send(
            @Nonnull SqlRequest.ExecutePreparedQuery request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (execute prepared query): {}", request); //$NON-NLS-1$
        var processor = new QueryProcessor(request);
        return processor.setFutureResponse(futureResponses.register(
            session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                processor)));
    }

    @Override
    public FutureResponse<ResultSet> send(
            @Nonnull SqlRequest.ExecutePreparedQuery request, @Nonnull List<? extends BlobInfo> blobs) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (execute prepared query): {}", request); //$NON-NLS-1$
        var processor = new QueryProcessor(request);
        return processor.setFutureResponse(futureResponses.register(
            session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                blobs,
                processor)));
    }

    @Override
    public FutureResponse<ResultSet> send(
            @Nonnull SqlRequest.ExecuteDump request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (execute dump): {}", request); //$NON-NLS-1$
        var processor = new QueryProcessor(request);
        return processor.setFutureResponse(futureResponses.register(
            session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                processor)));
    }

    @Override
    public FutureResponse<ResultSet> send(
            @Nonnull SqlRequest.ExecuteDumpByText request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (execute dump): {}", request); //$NON-NLS-1$
        var processor = new QueryProcessor(request);
        return processor.setFutureResponse(futureResponses.register(
            session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                processor)));
    }

    class LoadProcessor implements MainResponseProcessor<ExecuteResult> {
        private final AtomicReference<SqlResponse.Response> responseCache = new AtomicReference<>();

        @Override
        public ExecuteResult process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            if (responseCache.get() == null) {
                responseCache.set(SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload)));
            }
            var response = responseCache.get();
            switch (response.getResponseCase()) {
                case EXECUTE_RESULT:
                    var detailResponse = response.getExecuteResult();
                    LOG.trace("receive (ExecuteResult): {}", detailResponse); //$NON-NLS-1$
                    if (SqlResponse.ExecuteResult.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                        var errorResponse = detailResponse.getError();
                        throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
                    }
                    return new ExecuteResultAdapter(detailResponse.getSuccess());
                case RESULT_ONLY:
                    var resultOnlyResponse = response.getResultOnly();
                    LOG.trace("receive (ResultOnly): {}", resultOnlyResponse); //$NON-NLS-1$
                    if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(resultOnlyResponse.getResultCase())) {
                        var errorResponse = resultOnlyResponse.getError();
                        throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
                    }
                    return new ExecuteResultAdapter();
            }
            throw new IOException("response type is inconsistent with the request type");
        }
    }

    @Override
    public FutureResponse<ExecuteResult> send(
            @Nonnull SqlRequest.ExecuteLoad request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (execute load): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                new LoadProcessor().asResponseProcessor(false));
    }

    class GetTransactionStatusProcessor implements MainResponseProcessor<TransactionStatus.TransactionStatusWithMessage> {
        private final AtomicReference<SqlResponse.GetTransactionStatus> detailResponseCache = new AtomicReference<>();

        @Override
        public TransactionStatus.TransactionStatusWithMessage process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            if (detailResponseCache.get() == null) {
                var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
                if (!SqlResponse.Response.ResponseCase.GET_TRANSACTION_STATUS.equals(response.getResponseCase())) {
                    // FIXME log error message
                    throw new IOException("response type is inconsistent with the request type");
                }
                detailResponseCache.set(response.getGetTransactionStatus());
            }
            var detailResponse = detailResponseCache.get();
            LOG.trace("receive (GetTransactionStatus): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.GetTransactionStatus.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
            }
            return TransactionStatus.of(detailResponse.getSuccess());
        }
    }

    @Override
    public FutureResponse<TransactionStatus.TransactionStatusWithMessage> send(
            @Nonnull SqlRequest.GetTransactionStatus request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (get transaction status): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                new GetTransactionStatusProcessor().asResponseProcessor(false));
    }

    class ListTablesProcessor implements MainResponseProcessor<TableList> {
        private final AtomicReference<SqlResponse.ListTables> detailResponseCache = new AtomicReference<>();

        @Override
        public TableList process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            if (detailResponseCache.get() == null) {
                var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
                if (!SqlResponse.Response.ResponseCase.LIST_TABLES.equals(response.getResponseCase())) {
                    // FIXME log error message
                    throw new IOException("response type is inconsistent with the request type");
                }
                detailResponseCache.set(response.getListTables());
            }
            var detailResponse = detailResponseCache.get();
            LOG.trace("receive (ListTables): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.ListTables.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
            }
            return new TableListAdapter(detailResponse.getSuccess());
        }
    }

    @Override
    public FutureResponse<TableList> send(
            @Nonnull SqlRequest.ListTables request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (ListTables): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                new ListTablesProcessor().asResponseProcessor(false));
    }

    class GetSearchPathProcessor implements MainResponseProcessor<SearchPath> {
        private final AtomicReference<SqlResponse.GetSearchPath> detailResponseCache = new AtomicReference<>();

        @Override
        public SearchPath process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            if (detailResponseCache.get() == null) {
                var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
                if (!SqlResponse.Response.ResponseCase.GET_SEARCH_PATH.equals(response.getResponseCase())) {
                    // FIXME log error message
                    throw new IOException("response type is inconsistent with the request type");
                }
                detailResponseCache.set(response.getGetSearchPath());
            }
            var detailResponse = detailResponseCache.get();
            LOG.trace("receive (SearchPath): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.GetSearchPath.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
            }
            return new SearchPathAdapter(detailResponse.getSuccess());
        }
    }

    @Override
    public FutureResponse<SearchPath> send(
            @Nonnull SqlRequest.GetSearchPath request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (getSearchPath): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                new GetSearchPathProcessor().asResponseProcessor(false));
    }

    class GetErrorInfoProcessor implements MainResponseProcessor<SqlServiceException> {
        private final AtomicReference<SqlResponse.GetErrorInfo> detailResponseCache = new AtomicReference<>();

        @Override
        public SqlServiceException process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            if (detailResponseCache.get() == null) {
                var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
                if (!SqlResponse.Response.ResponseCase.GET_ERROR_INFO.equals(response.getResponseCase())) {
                    // FIXME log error message
                    throw new IOException("response type is inconsistent with the request type");
                }
                detailResponseCache.set(response.getGetErrorInfo());
            }
            var detailResponse = detailResponseCache.get();
            LOG.trace("receive (GetErrorInfo): {}", detailResponse); //$NON-NLS-1$
            switch (detailResponse.getResultCase()) {
                case SUCCESS:
                    var response = detailResponse.getSuccess();
                    if (response.getCode() == SqlError.Code.TRANSACTION_NOT_FOUND_EXCEPTION) {
                        return null;
                    }
                    return SqlServiceException.of(SqlServiceCode.valueOf(response.getCode()), response.getDetail());
                case ERROR_NOT_FOUND:
                    return null;
                case ERROR:
                    var errorResponse = detailResponse.getError();
                    throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
            }
            throw new IOException("unhandled response in GetErrorInfo");  // never reached
        }
    }

    @Override
    public FutureResponse<SqlServiceException> send(
            @Nonnull SqlRequest.GetErrorInfo request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (GetErrorInfo): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                new GetErrorInfoProcessor().asResponseProcessor(false));
    }

    class GetBlobProcessor implements ResponseProcessor<InputStream> {
        private final AtomicReference<SqlResponse.GetLargeObjectData> detailResponseCache = new AtomicReference<>();

        @Override
        public InputStream process(Response response) throws IOException, ServerException, InterruptedException {
            return process(response, Timeout.DISABLED);
        }

        @Override
        public InputStream process(Response response, Timeout timeout) throws IOException, ServerException, InterruptedException {
            Objects.requireNonNull(response);

            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            try (response) {
                var payload = response.waitForMainResponse();
                if (detailResponseCache.get() == null) {
                    var sqlResponse = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
                    if (!SqlResponse.Response.ResponseCase.GET_LARGE_OBJECT_DATA.equals(sqlResponse.getResponseCase())) {
                        // FIXME log error message
                        throw new IOException("response type is inconsistent with the request type");
                    }
                    detailResponseCache.set(sqlResponse.getGetLargeObjectData());
                }
                var detailResponse = detailResponseCache.get();
                LOG.trace("receive (GetLargeObjectData): {}", detailResponse); //$NON-NLS-1$
                if (SqlResponse.GetLargeObjectData.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                    var errorResponse = detailResponse.getError();
                    throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
                }
                try {
                    if (response instanceof ChannelResponse && session instanceof SessionImpl) {
                        ((ChannelResponse) response).setBlobPathMapping(((SessionImpl) session).getBlobPathMapping());
                    }
                    return response.openSubResponse(detailResponse.getSuccess().getChannelName());
                } catch (NoSuchFileException | AccessDeniedException e) {
                    throw new BlobException("error occurred while receiving BLOB data: {" + e.getMessage() + "}");
                } catch (FileNotFoundException e) {  // should not happen, as AccessDeniedException should be thrown
                    throw new BlobException("error occurred while receiving BLOB data: {openSubResponse fail, channel name: " + detailResponse.getSuccess().getChannelName() + "}", e);
                }
            }
        }

        @Override
        public boolean isReturnsServerResource() {
            return false;
        }
    }

    @Override
    public FutureResponse<InputStream> send(
            @Nonnull SqlRequest.GetLargeObjectData request, BlobReference reference) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (GetLargeObjectData): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                new GetBlobProcessor());
    }

    class GetClobProcessor implements ResponseProcessor<Reader> {
        private final AtomicReference<SqlResponse.GetLargeObjectData> detailResponseCache = new AtomicReference<>();

        @Override
        public Reader process(Response response) throws IOException, ServerException, InterruptedException {
            return process(response, Timeout.DISABLED);
        }

        @Override
        public Reader process(Response response, Timeout timeout) throws IOException, ServerException, InterruptedException {
            Objects.requireNonNull(response);

            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            try (response) {
                var payload = response.waitForMainResponse();
                if (detailResponseCache.get() == null) {
                    var sqlResponse = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
                    if (!SqlResponse.Response.ResponseCase.GET_LARGE_OBJECT_DATA.equals(sqlResponse.getResponseCase())) {
                        // FIXME log error message
                        throw new IOException("response type is inconsistent with the request type");
                    }
                    detailResponseCache.set(sqlResponse.getGetLargeObjectData());
                }
                var detailResponse = detailResponseCache.get();
                LOG.trace("receive (GetLargeObjectData): {}", detailResponse); //$NON-NLS-1$
                if (SqlResponse.GetLargeObjectData.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                    var errorResponse = detailResponse.getError();
                    throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
                }
                try {
                    if (response instanceof ChannelResponse && session instanceof SessionImpl) {
                        ((ChannelResponse) response).setBlobPathMapping(((SessionImpl) session).getBlobPathMapping());
                    }
                    return new InputStreamReader(response.openSubResponse(detailResponse.getSuccess().getChannelName()), "UTF-8");
                } catch (NoSuchFileException | AccessDeniedException e) {
                    throw new BlobException("error occurred while receiving BLOB data: {" + e.getMessage() + "}");
                } catch (FileNotFoundException e) {  // should not happen, as AccessDeniedException should be thrown
                    throw new BlobException("error occurred while receiving BLOB data: {openSubResponse fail, channel name: " + detailResponse.getSuccess().getChannelName() + "}", e);

                }
            }
        }

        @Override
        public boolean isReturnsServerResource() {
            return false;
        }
    }

    @Override
    public FutureResponse<Reader> send(
            @Nonnull SqlRequest.GetLargeObjectData request, ClobReference reference) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (GetLargeObjectData): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                new GetClobProcessor());
    }

    class GetLargeObjectCacheProcessor implements ResponseProcessor<LargeObjectCache> {
        private final AtomicReference<SqlResponse.GetLargeObjectData> detailResponseCache = new AtomicReference<>();

        @Override
        public LargeObjectCache process(Response response) throws IOException, ServerException, InterruptedException {
            return process(response, Timeout.DISABLED);
        }

        @Override
        public LargeObjectCache process(Response response, Timeout timeout) throws IOException, ServerException, InterruptedException {
            Objects.requireNonNull(response);

            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            try (response) {
                var payload = response.waitForMainResponse();
                if (detailResponseCache.get() == null) {
                    var sqlResponse = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
                    if (!SqlResponse.Response.ResponseCase.GET_LARGE_OBJECT_DATA.equals(sqlResponse.getResponseCase())) {
                        // FIXME log error message
                        throw new IOException("response type is inconsistent with the request type");
                    }
                    detailResponseCache.set(sqlResponse.getGetLargeObjectData());
                }
                var detailResponse = detailResponseCache.get();
                LOG.trace("receive (GetLargeObjectData): {}", detailResponse); //$NON-NLS-1$
                if (SqlResponse.GetLargeObjectData.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                    var errorResponse = detailResponse.getError();
                    throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
                }
                try {
                    if (response instanceof ChannelResponse && session instanceof SessionImpl) {
                        ((ChannelResponse) response).setBlobPathMapping(((SessionImpl) session).getBlobPathMapping());
                    }
                    var inputStream = response.openSubResponse(detailResponse.getSuccess().getChannelName());
                    if (inputStream instanceof ChannelResponse.FileInputStreamWithPath) {
                        return new LargeObjectCacheImpl(((ChannelResponse.FileInputStreamWithPath) inputStream).path());
                    }
                    return new LargeObjectCacheImpl();
                } catch (AccessDeniedException e) {
                    throw new BlobException("error occurred while receiving BLOB data: {" + e.getMessage() + "}");
                } catch (NoSuchFileException | FileNotFoundException e) {
                    return new LargeObjectCacheImpl();
                }
            }
        }

        @Override
        public boolean isReturnsServerResource() {
            return false;
        }
    }

    @Override
    public FutureResponse<LargeObjectCache> send(
            @Nonnull SqlRequest.GetLargeObjectData request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (GetLargeObjectData): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                new GetLargeObjectCacheProcessor());
    }

    class CopyLargeObjectProcessor implements ResponseProcessor<Void> {
        private final Path destination;
        private final AtomicReference<SqlResponse.GetLargeObjectData> detailResponseCache = new AtomicReference<>();

        CopyLargeObjectProcessor(Path destination) {
            this.destination = destination;
        }

        @Override
        public Void process(Response response) throws IOException, ServerException, InterruptedException {
            return process(response, Timeout.DISABLED);
        }

        @Override
        public Void process(Response response, Timeout timeout) throws IOException, ServerException, InterruptedException {
            Objects.requireNonNull(response);

            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            try (response) {
                var payload = response.waitForMainResponse();
                if (detailResponseCache.get() == null) {
                    var sqlResponse = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
                    if (!SqlResponse.Response.ResponseCase.GET_LARGE_OBJECT_DATA.equals(sqlResponse.getResponseCase())) {
                        // FIXME log error message
                        throw new IOException("response type is inconsistent with the request type");
                    }
                    detailResponseCache.set(sqlResponse.getGetLargeObjectData());
                }
                var detailResponse = detailResponseCache.get();
                LOG.trace("receive (GetLargeObjectData): {}", detailResponse); //$NON-NLS-1$
                if (SqlResponse.GetLargeObjectData.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                    var errorResponse = detailResponse.getError();
                    throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
                }
                var channelName = detailResponse.getSuccess().getChannelName();
                try {
                    if (response instanceof ChannelResponse && session instanceof SessionImpl) {
                        ((ChannelResponse) response).setBlobPathMapping(((SessionImpl) session).getBlobPathMapping());
                    }
                    var inputStream = response.openSubResponse(channelName);
                    try {
                        Files.copy(inputStream, destination);
                        return null;
                    } catch (FileAlreadyExistsException e) {
                        throw new BlobException("error occurred while copying BLOB data: {FileAlreadyExists: " + destination + "}", e);
                    } catch (AccessDeniedException e) {
                        var parent = destination.getParent();
                        if (parent != null) {
                            throw new BlobException("error occurred while copying BLOB data: {AccessDenied: " + parent + "}", e);
                        }
                        throw new BlobException("error occurred while copying BLOB data: {AccessDenied: " + destination + "}", e);
                    } catch (FileSystemException e) {
                        var parent = destination.getParent();
                        if (parent != null) {
                            if (!Files.exists(parent)) {
                                throw new BlobException("error occurred while copying BLOB data: {NoSuchDirectory: " + parent + "}", e);
                            }
                            throw new BlobException("error occurred while copying BLOB data: {IsNotDirectory: " + parent + "}", e);
                        }
                        throw new BlobException("error occurred while copying BLOB data: {NoSuchFile: " + destination + "}", e);
                    }
                } catch (BlobException e) {
                    throw e;
                } catch (NoSuchFileException | AccessDeniedException e) {
                    throw new BlobException("error occurred while receiving BLOB data: {" + e.getMessage() + "}");
                } catch (FileNotFoundException e) {  // should not happen, as AccessDeniedException should be thrown
                    throw new BlobException("error occurred while receiving BLOB data: {openSubResponse fail, channel name: " + channelName + "}", e);
                } catch (Exception e) {
                    throw new BlobException("error occurred while receiving BLOB data: {unknown error}", e);  // should not happen
                }
            }
        }

        @Override
        public boolean isReturnsServerResource() {
            return false;
        }
    }

    @Override
    public FutureResponse<Void> send(
            @Nonnull SqlRequest.GetLargeObjectData request, @Nonnull Path destination) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (GetLargeObjectData): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                new CopyLargeObjectProcessor(destination));
    }

    class DisposeTransactionProcessor implements MainResponseProcessor<Void> {
        private final AtomicReference<SqlResponse.Response> responseCache = new AtomicReference<>();

        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            if (responseCache.get() == null) {
                responseCache.set(SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload)));
            }
            var response = responseCache.get();
            switch (response.getResponseCase()) {
                case DISPOSE_TRANSACTION:
                    var detailResponse = response.getDisposeTransaction();
                    LOG.trace("receive (DisposeTransaction): {}", detailResponse); //$NON-NLS-1$
                    if (SqlResponse.DisposeTransaction.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                        var errorResponse = detailResponse.getError();
                        throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
                    }
                    return null;
                case RESULT_ONLY:
                    var resultOnlyResponse = response.getResultOnly();
                    LOG.trace("receive (ResultOnly): {}", resultOnlyResponse); //$NON-NLS-1$
                    if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(resultOnlyResponse.getResultCase())) {
                        var errorResponse = resultOnlyResponse.getError();
                        throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
                    }
                    return null;
            }
            throw new IOException("response type is inconsistent with the request type");
        }
    }

    @Override
    public FutureResponse<Void> send(
            @Nonnull SqlRequest.DisposeTransaction request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (DisposeTransaction): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequestUtils.toSqlRequestDelimitedByteArray(request),
                new DisposeTransactionProcessor().asResponseProcessor(false));
    }

    // for compatibility
    class DisconnectProcessor implements MainResponseProcessor<Void> {
        private final AtomicReference<SqlResponse.ResultOnly> detailResponseCache = new AtomicReference<>();

        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (session.isClosed()) {
                throw new SessionAlreadyClosedException();
            }
            if (detailResponseCache.get() == null) {
                var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
                if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                    // FIXME log error message
                    throw new IOException("response type is inconsistent with the request type");
                }
                detailResponseCache.set(response.getResultOnly());
            }
            var detailResponse = detailResponseCache.get();
            LOG.trace("receive (disconnect): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw SqlServiceException.of(SqlServiceCode.valueOf(errorResponse.getCode()), errorResponse.getDetail());
            }
            return null;
        }
    }

    @Override
    public void setCloseTimeout(Timeout timeout) {
        closeTimeout = timeout;
        futureResponses.setCloseTimeout(timeout);
        resources.setCloseTimeout(timeout);
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        LOG.trace("closing underlying resources"); //$NON-NLS-1$
        synchronized (futureResponses) {
            futureResponses.close();
        }
        synchronized (resources) {
            resources.close();
        }
        session.remove(this);
    }

    // for diagnostic
    static class ResourceInfoAction implements Consumer<ServerResource> {
        String diagnosticInfo = "";

        @Override
        public void accept(ServerResource r) {
            if (r != null) {
                if (r instanceof TransactionImpl) {
                    diagnosticInfo += ((TransactionImpl) r).diagnosticInfo();
                } else if (r instanceof PreparedStatementImpl) {
                    diagnosticInfo += ((PreparedStatementImpl) r).diagnosticInfo();
                } else if (r instanceof ResultSetImpl) {
                    diagnosticInfo += ((ResultSetImpl) r).diagnosticInfo();
                }
            }
        }
        public String diagnosticInfo() {
            return diagnosticInfo;
        }
    }
    public String diagnosticInfo() {
        var resourceInfoAction = new ResourceInfoAction();
        resources.forEach(resourceInfoAction);
        return resourceInfoAction.diagnosticInfo();
    }
}
