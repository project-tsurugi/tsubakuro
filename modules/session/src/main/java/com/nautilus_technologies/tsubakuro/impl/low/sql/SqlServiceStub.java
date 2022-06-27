package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import  com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.channel.common.connection.wire.MainResponseProcessor;
import com.nautilus_technologies.tsubakuro.exception.BrokenResponseException;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.SqlService;
import com.nautilus_technologies.tsubakuro.exception.SqlServiceCode;
import com.nautilus_technologies.tsubakuro.exception.SqlServiceException;
import com.nautilus_technologies.tsubakuro.low.sql.TableMetadata;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.ServerResourceHolder;
import com.nautilus_technologies.tsubakuro.util.ByteBufferInputStream;
import com.nautilus_technologies.tsubakuro.util.Owner;
import com.nautilus_technologies.tsubakuro.channel.common.connection.wire.Wire;
import com.nautilus_technologies.tsubakuro.channel.common.connection.wire.Response;
import com.nautilus_technologies.tsubakuro.channel.common.connection.wire.ResponseProcessor;
import com.nautilus_technologies.tsubakuro.channel.common.connection.ForegroundFutureResponse;

/**
 * An interface to communicate with SQL service.
 */
public class SqlServiceStub implements SqlService {

    static final Logger LOG = LoggerFactory.getLogger(SqlServiceStub.class);

    /**
     * The SQL service ID.
     */
    public static final int SERVICE_ID = Constants.SERVICE_ID_SQL;

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

    class TransactionBeginProcessor implements MainResponseProcessor<Transaction> {
        @Override
        public Transaction process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.BEGIN.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getBegin();
            LOG.trace("receive: {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.Begin.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            return new TransactionImpl(detailResponse.getTransactionHandle(), SqlServiceStub.this);
        }
    }

    @Override
    public FutureResponse<Transaction> send(
            @Nonnull SqlRequest.Begin request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setBegin(request)
                    .build()),
                new TransactionBeginProcessor().asResponseProcessor());
    }

    static class TransactionCommitProcessor implements MainResponseProcessor<SqlResponse.ResultOnly> {
        @Override
        public SqlResponse.ResultOnly process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getResultOnly();
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            LOG.trace("receive: {}", detailResponse); //$NON-NLS-1$
            return detailResponse;
        }
    }

    @Override
    public FutureResponse<SqlResponse.ResultOnly> send(
            @Nonnull SqlRequest.Commit request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setCommit(request)
                    .build()),
                new TransactionCommitProcessor().asResponseProcessor());
    }

    static class TransactionRollbackProcessor implements MainResponseProcessor<SqlResponse.ResultOnly> {
        @Override
        public  SqlResponse.ResultOnly process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getResultOnly();
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            LOG.trace("receive: {}", detailResponse); //$NON-NLS-1$
            return detailResponse;
        }
    }

    @Override
    public FutureResponse<SqlResponse.ResultOnly> send(
            @Nonnull SqlRequest.Rollback request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send: {}", request); //$NON-NLS-1$
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
            if (SqlResponse.Prepare.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            return new PreparedStatementImpl(detailResponse.getPreparedStatementHandle(), SqlServiceStub.this);
        }
    }

    @Override
    public FutureResponse<PreparedStatement> send(
            @Nonnull SqlRequest.Prepare request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setPrepare(request)
                    .build()),
                new StatementPrepareProcessor().asResponseProcessor());
    }

    static class StatementDisposeProcessor implements MainResponseProcessor<SqlResponse.ResultOnly> {
        @Override
        public SqlResponse.ResultOnly process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getResultOnly();
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            LOG.trace("receive: {}", detailResponse); //$NON-NLS-1$
            return detailResponse;
        }
    }

    @Override
    public FutureResponse<SqlResponse.ResultOnly> send(
            @Nonnull SqlRequest.DisposePreparedStatement request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setDisposePreparedStatement(request)
                    .build()),
                new StatementDisposeProcessor().asResponseProcessor());
    }

    static class DescribeStatementProcessor implements MainResponseProcessor<String> {
        @Override
        public String process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.EXPLAIN.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getExplain();
            if (SqlResponse.Explain.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            return detailResponse.getOutput();
        }
    }

    @Override
    public FutureResponse<String> send(
            @Nonnull SqlRequest.Explain request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send: {}", request); //$NON-NLS-1$
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
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setDescribeTable(request)
                    .build()),
                new DescribeTableProcessor().asResponseProcessor());
    }

    static class ExecuteProcessor implements MainResponseProcessor<SqlResponse.ResultOnly> {
        @Override
        public SqlResponse.ResultOnly process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getResultOnly();
            LOG.trace("receive: {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            return detailResponse;
        }
    }

    @Override
    public FutureResponse<SqlResponse.ResultOnly> send(
            @Nonnull SqlRequest.ExecuteStatement request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setExecuteStatement(request)
                    .build()),
                new ExecuteProcessor().asResponseProcessor());
    }

    @Override
    public FutureResponse<SqlResponse.ResultOnly> send(
            @Nonnull SqlRequest.ExecutePreparedStatement request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setExecutePreparedStatement(request)
                    .build()),
                new ExecuteProcessor().asResponseProcessor());
    }

    static class SecondResponseProcessor implements MainResponseProcessor<SqlResponse.ResultOnly> {
        @Override
        public SqlResponse.ResultOnly process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getResultOnly();
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            LOG.trace("receive: {}", detailResponse); //$NON-NLS-1$
            return detailResponse;
        }
    }

    static class ResultSetProcessor  implements ResponseProcessor<ResultSet> {
        private final Wire wire;    

        ResultSetProcessor(
            @Nonnull Wire wire) {
            Objects.requireNonNull(wire);
            this.wire = wire;
        }

        @Override
        public ResultSet process(Response response) throws IOException, ServerException, InterruptedException {
            Objects.requireNonNull(response);
            response.setResultSetMode();

            var resultSetImpl = new ResultSetImpl(wire.createResultSetWire());
            var payload = response.waitForMainResponse();
            var sqlResponse = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            var channelResponse = response.duplicate();
            response.release();

            if (SqlResponse.Response.ResponseCase.EXECUTE_QUERY.equals(sqlResponse.getResponseCase())) {
                var futureResponse = FutureResponse.wrap(Owner.of(channelResponse));
                var future = new ForegroundFutureResponse<SqlResponse.ResultOnly>(futureResponse, new SecondResponseProcessor().asResponseProcessor());

                var detailResponse = sqlResponse.getExecuteQuery();
                resultSetImpl.connect(detailResponse.getName(), detailResponse.getRecordMeta(), future);
            } else if (SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(sqlResponse.getResponseCase())) {
                channelResponse.release();
                resultSetImpl.indicateError(new FutureResultOnly(sqlResponse.getResultOnly()));
            } else {
                throw new IOException("response type is inconsistent with the request type");
            }
            return resultSetImpl;
        }
    }

    @Override
    public FutureResponse<ResultSet> send(
            @Nonnull SqlRequest.ExecuteQuery request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setExecuteQuery(request)
                    .build()),
                new ResultSetProcessor(session.getWire()));
    }

    @Override
    public FutureResponse<ResultSet> send(
            @Nonnull SqlRequest.ExecutePreparedQuery request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setExecutePreparedQuery(request)
                    .build()),
                new ResultSetProcessor(session.getWire()));
    }

    @Override
    public FutureResponse<ResultSet> send(
            @Nonnull SqlRequest.ExecuteDump request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setExecuteDump(request)
                    .build()),
                    new ResultSetProcessor(session.getWire()));
    }

    static class LoadProcessor implements MainResponseProcessor<SqlResponse.ResultOnly> {
        @Override
        public SqlResponse.ResultOnly process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
            if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                // FIXME log error message
                throw new IOException("response type is inconsistent with the request type");
            }
            var detailResponse = response.getResultOnly();
            LOG.trace("receive: {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                var errorResponse = detailResponse.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            return detailResponse;
        }
    }

    @Override
    public FutureResponse<SqlResponse.ResultOnly> send(
            @Nonnull SqlRequest.ExecuteLoad request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setExecuteLoad(request)
                    .build()),
                new LoadProcessor().asResponseProcessor());
    }

    // for compatibility
    @Override
    public FutureResponse<SqlResponse.ResultOnly> send(
            @Nonnull SqlRequest.Disconnect request) throws IOException {
        Objects.requireNonNull(request);
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setDisconnect(request)
                    .build()),
                new LoadProcessor().asResponseProcessor());
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        var futureResponse = send(SqlRequest.Disconnect.newBuilder().build());
        if (Objects.nonNull(futureResponse)) {
            var response = futureResponse.get();
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(response.getResultCase())) {
                throw new IOException(response.getError().getDetail());
            }
        }
        LOG.trace("closing underlying resources"); //$NON-NLS-1$
        resources.close();
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