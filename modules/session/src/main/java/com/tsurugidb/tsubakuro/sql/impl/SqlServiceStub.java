package com.tsurugidb.tsubakuro.sql.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.ForegroundFutureResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.MainResponseProcessor;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.SecondChannelResponse;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.exception.BrokenResponseException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.RelationCursor;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.SqlService;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;
import com.tsurugidb.tsubakuro.sql.StatementMetadata;
import com.tsurugidb.tsubakuro.sql.TableMetadata;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.sql.io.StreamBackedValueInput;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Owner;
import com.tsurugidb.tsubakuro.util.ServerResourceHolder;

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

    private final ServerResourceHolder resources = new ServerResourceHolder();

    /**
     * Creates a new instance.
     * @param session the current session
     */
    public SqlServiceStub(@Nonnull Session session) {
        Objects.requireNonNull(session);
        this.session = session;
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
        @Override
        public Transaction process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.BEGIN.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getBegin();
            LOG.trace("receive (Begin): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.Begin.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            var transactionImpl = new TransactionImpl(detailResponse.getTransactionHandle(), SqlServiceStub.this, resources);
            return resources.register(transactionImpl);
        }
    }

    @Override
    public FutureResponse<Transaction> send(
            @Nonnull SqlRequest.Begin request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (Begin): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setBegin(request)
                    .build()),
                new TransactionBeginProcessor().asResponseProcessor());
    }

    static class TransactionCommitProcessor implements MainResponseProcessor<Void> {
        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getResultOnly();
            LOG.trace("receive (commit): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
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
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setCommit(request)
                    .build()),
                new TransactionCommitProcessor().asResponseProcessor());
    }

    static class TransactionRollbackProcessor implements MainResponseProcessor<Void> {
        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getResultOnly();
            LOG.trace("receive (rollback): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
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
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setRollback(request)
                    .build()),
                new TransactionRollbackProcessor().asResponseProcessor());
    }

    class StatementPrepareProcessor implements MainResponseProcessor<PreparedStatement> {
        @Override
        public PreparedStatement process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.PREPARE.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getPrepare();
            LOG.trace("receive (prepare): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.Prepare.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            var preparedStatementImpl = new PreparedStatementImpl(detailResponse.getPreparedStatementHandle(), SqlServiceStub.this, resources);
            return resources.register(preparedStatementImpl);
        }
    }

    @Override
    public FutureResponse<PreparedStatement> send(
            SqlRequest.Prepare request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (prepare): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setPrepare(request)
                    .build()),
                new StatementPrepareProcessor().asResponseProcessor());
    }

    static class StatementDisposeProcessor implements MainResponseProcessor<Void> {
        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getResultOnly();
            LOG.trace("receive (dispose prepared statement): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
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
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setDisposePreparedStatement(request)
                    .build()),
                new StatementDisposeProcessor().asResponseProcessor());
    }

    static class DescribeStatementProcessor implements MainResponseProcessor<StatementMetadata> {
        @Override
        public StatementMetadata process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.EXPLAIN.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getExplain();
            LOG.trace("receive (explain): {}", detailResponse); //$NON-NLS-1$
            switch (detailResponse.getResultCase()) {
            case OUTPUT:
                LOG.warn("deprecated response: {}", detailResponse);
                return new BasicStatementMetadata(
                        FORMAT_ID_LEGACY_EXPLAIN,
                        FORMAT_VERSION_LEGACY_EXPLAIN,
                        detailResponse.getOutput(),
                        List.of());

            case SUCCESS:
                return new StatementMetadataAdapter(detailResponse.getSuccess());

            case ERROR:
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());

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
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setExplain(request)
                    .build()),
                new DescribeStatementProcessor().asResponseProcessor());
    }

    static class DescribeTableProcessor implements MainResponseProcessor<TableMetadata> {
        @Override
        public TableMetadata process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.DESCRIBE_TABLE.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getDescribeTable();
            LOG.trace("receive (describe table): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.DescribeTable.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
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
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setDescribeTable(request)
                    .build()),
                new DescribeTableProcessor().asResponseProcessor());
    }

    static class ExecuteProcessor implements MainResponseProcessor<Void> {
        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getResultOnly();
            LOG.trace("receive (execute (prepared) statement): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            return null;
        }
    }

    @Override
    public FutureResponse<Void> send(
            @Nonnull SqlRequest.ExecuteStatement request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (execute statement): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setExecuteStatement(request)
                    .build()),
                new ExecuteProcessor().asResponseProcessor());
    }

    @Override
    public FutureResponse<Void> send(
            @Nonnull SqlRequest.ExecutePreparedStatement request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (execute prepared statement): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setExecutePreparedStatement(request)
                    .build()),
                new ExecuteProcessor().asResponseProcessor());
    }

    static class SecondResponseProcessor implements MainResponseProcessor<Void> {
        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getResultOnly();
            LOG.trace("receive (execute query body): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            return null;
        }
    }

    class QueryProcessor extends AbstractResultSetProcessor<SqlResponse.Response> {

        QueryProcessor() {
            super(resources);
        }

        @Override
        SqlResponse.Response parse(@Nonnull ByteBuffer payload) throws IOException {
            var message = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive (execute query bodyhead): {}", message); //$NON-NLS-1$
            return message;
        }

        @Override
        void doTest(@Nonnull SqlResponse.Response message) throws IOException, ServerException, InterruptedException {
            if (SqlResponse.Response.ResponseCase.EXECUTE_QUERY.equals(message.getResponseCase())) {
                return; // OK
            } else if (SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(message.getResponseCase())) {
                var detailResponse = message.getResultOnly();
                if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                    var errorResponse = detailResponse.getError();
                    throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
                }
            }
            throw new AssertionError(); // may not occur
        }

        @Override
        public ResultSet process(Response response) throws IOException, ServerException, InterruptedException {
            Objects.requireNonNull(response);
    //        validateMetadata(response);
            try (
                var owner = Owner.of(response);
            ) {
                test(response);
                var sqlResponse = cache.get();
                var secondChannelResponse = new SecondChannelResponse(response);
                var detailResponse = sqlResponse.getExecuteQuery();
                var metadata = new ResultSetMetadataAdapter(detailResponse.getRecordMeta());
                SqlServiceStub.LOG.trace("result set metadata: {}", metadata); //$NON-NLS-1$

                var dataInput = session.getWire().createResultSetWire().connect(detailResponse.getName()).getByteBufferBackedInput();
                RelationCursor cursor;
                if (Objects.nonNull(dataInput)) {
                    cursor = new ValueInputBackedRelationCursor(new StreamBackedValueInput(dataInput));
                } else {
                    cursor = new EmptyRelationCursor();
                }
                var futureResponse = FutureResponse.wrap(Owner.of(secondChannelResponse));
                var future = new ForegroundFutureResponse<Void>(futureResponse, new SecondResponseProcessor().asResponseProcessor());
                var resultSetImpl = new ResultSetImpl(resources, metadata, cursor, owner.release(), this, future);
                return resources.register(resultSetImpl);
            } catch (SqlServiceException e) {
                throw e;
            } catch (Exception e) {
                // if sub-response seems broken, check main-response for detect errors.
                try {
                    test(response);
                } catch (Throwable t) {
                    t.addSuppressed(e);
                    throw t;
                }
                throw e;
            }
        }
    }

    @Override
    public FutureResponse<ResultSet> send(
            @Nonnull SqlRequest.ExecuteQuery request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (execute query): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setExecuteQuery(request)
                    .build()),
                    new QueryProcessor());
// FIXME  use backgroundResponseProcessor
//                new QueryProcessor(session.getWire()),
//                true);

    }

    @Override
    public FutureResponse<ResultSet> send(
            @Nonnull SqlRequest.ExecutePreparedQuery request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (execute prepared query): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setExecutePreparedQuery(request)
                    .build()),
                    new QueryProcessor());
// FIXME  use backgroundResponseProcessor
//                new QueryProcessor(session.getWire()),
//                true);
    }

    static class BatchProcessor implements MainResponseProcessor<Void> {
        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getResultOnly();
            LOG.trace("receive (batch): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            return null;
        }
    }

    @Override
    public FutureResponse<Void> send(
            @Nonnull SqlRequest.Batch request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (batch): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                SqlRequest.Request.newBuilder()
                    .setBatch(request)
                    .build()
                    .toByteArray(),
                new BatchProcessor().asResponseProcessor());
    }

    @Override
    public FutureResponse<ResultSet> send(
            @Nonnull SqlRequest.ExecuteDump request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (execute dump): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setExecuteDump(request)
                    .build()),
                    new QueryProcessor());
    }

    static class LoadProcessor implements MainResponseProcessor<Void> {
        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getResultOnly();
            LOG.trace("receive (execute load): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            return null;
        }
    }

    @Override
    public FutureResponse<Void> send(
            @Nonnull SqlRequest.ExecuteLoad request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (execute load): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setExecuteLoad(request)
                    .build()),
                new LoadProcessor().asResponseProcessor());
    }

    // for compatibility
    static class DisconnectProcessor implements MainResponseProcessor<Void> {
        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getResultOnly();
            LOG.trace("receive (disconnect): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            return null;
        }
    }

    // for compatibility
    @Override
    public FutureResponse<Void> send(
            @Nonnull SqlRequest.Disconnect request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send (disconnect): {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setDisconnect(request)
                    .build()),
                new DisconnectProcessor().asResponseProcessor());
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        try {
            LOG.trace("closing underlying resources"); //$NON-NLS-1$
            resources.close();
        } finally {
            var futureResponse = send(SqlRequest.Disconnect.newBuilder().build());
            if (Objects.nonNull(futureResponse)) {
                futureResponse.get();
            }
        }
    }

    private byte[] toDelimitedByteArray(SqlRequest.Request request) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            request.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new IOException(e);
        }
    }
}
