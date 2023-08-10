package com.tsurugidb.tsubakuro.sql.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.MainResponseProcessor;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ChannelResponse;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.exception.BrokenResponseException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.SqlService;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;
import com.tsurugidb.tsubakuro.sql.StatementMetadata;
import com.tsurugidb.tsubakuro.sql.TableMetadata;
import com.tsurugidb.tsubakuro.sql.TableList;
import com.tsurugidb.tsubakuro.sql.SearchPath;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.sql.io.StreamBackedValueInput;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.Owner;
import com.tsurugidb.tsubakuro.util.Timeout;
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

    private Timeout closeTimeout = Timeout.DISABLED;

    /**
     * Creates a new instance.
     * @param session the current session
     */
    public SqlServiceStub(@Nonnull Session session) {
        Objects.requireNonNull(session);
        this.session = session;
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

        @Override
        public Transaction process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (Objects.isNull(detailResponseCache.get())) {
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
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            var transactionImpl = new TransactionImpl(detailResponse.getSuccess(), SqlServiceStub.this, resources);
            transactionImpl.setCloseTimeout(closeTimeout);
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
        private final AtomicReference<SqlResponse.ResultOnly> detailResponseCache = new AtomicReference<>();

        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (Objects.isNull(detailResponseCache.get())) {
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
        private final AtomicReference<SqlResponse.ResultOnly> detailResponseCache = new AtomicReference<>();

        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (Objects.isNull(detailResponseCache.get())) {
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
        private final AtomicReference<SqlResponse.Prepare> detailResponseCache = new AtomicReference<>();
        private final SqlRequest.Prepare request;

        StatementPrepareProcessor(SqlRequest.Prepare request) {
            this.request = request;
        }

        @Override
        public PreparedStatement process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (Objects.isNull(detailResponseCache.get())) {
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
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            var preparedStatementImpl = new PreparedStatementImpl(detailResponse.getPreparedStatementHandle(), SqlServiceStub.this, resources, request);
            preparedStatementImpl.setCloseTimeout(closeTimeout);
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
                new StatementPrepareProcessor(request).asResponseProcessor());
    }

    static class StatementDisposeProcessor implements MainResponseProcessor<Void> {
        private final AtomicReference<SqlResponse.ResultOnly> detailResponseCache = new AtomicReference<>();

        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (Objects.isNull(detailResponseCache.get())) {
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
        private final AtomicReference<SqlResponse.Explain> detailResponseCache = new AtomicReference<>();

        @Override
        public StatementMetadata process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (Objects.isNull(detailResponseCache.get())) {
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
        private final AtomicReference<SqlResponse.DescribeTable> detailResponseCache = new AtomicReference<>();

        @Override
        public TableMetadata process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (Objects.isNull(detailResponseCache.get())) {
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
        private final AtomicReference<SqlResponse.ResultOnly> detailResponseCache = new AtomicReference<>();

        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (Objects.isNull(detailResponseCache.get())) {
                var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
                if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                    // FIXME log error message
                    throw new IOException("response type is inconsistent with the request type");
                }
                detailResponseCache.set(response.getResultOnly());
            }
            var detailResponse = detailResponseCache.get();
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

    class QueryProcessor extends AbstractResultSetProcessor<SqlResponse.Response> {
        Message request;

        QueryProcessor(Message request) {
            super(resources);
            this.request = request;
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
                    throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
                }
                return;
            }
            throw new AssertionError(); // should not occur
        }

        @Override
        public ResultSet process(Response response, Timeout timeout) throws IOException, ServerException, InterruptedException {
            Objects.requireNonNull(response);
            try (
                var owner = Owner.of(response);
            ) {
                InputStream metadataInput;
                ResultSetMetadataAdapter metadata = null;
                while (true) {
                    Timeout timeoutHere = null;
                    if (timeout.value() > 0) {
                        timeoutHere = timeout;
                    } else if (Objects.nonNull(closeTimeout)) {
                        if (closeTimeout.value() > 0) {
                            timeoutHere = closeTimeout;
                        }
                    }
                    if (Objects.nonNull(timeoutHere)) {
                        try {
                            metadataInput = response.openSubResponse(ChannelResponse.METADATA_CHANNEL_ID, timeoutHere.value(), timeoutHere.unit());
                        } catch (TimeoutException e) {
                            throw new ResponseTimeoutException(e);
                        }
                    } else {
                        metadataInput = response.openSubResponse(ChannelResponse.METADATA_CHANNEL_ID);
                    }
                    if (Objects.nonNull(metadataInput)) {
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
                return resources.register(resultSetImpl);
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
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setExecuteQuery(request)
                    .build()),
                    new QueryProcessor(request));
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
                    new QueryProcessor(request));
// FIXME  use backgroundResponseProcessor
//                new QueryProcessor(session.getWire()),
//                true);
    }

    static class BatchProcessor implements MainResponseProcessor<Void> {
        private final AtomicReference<SqlResponse.Batch> detailResponseCache = new AtomicReference<>();

        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (Objects.isNull(detailResponseCache.get())) {
                var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
                if (!SqlResponse.Response.ResponseCase.BATCH.equals(response.getResponseCase())) {
                    // FIXME log error message
                    throw new IOException("response type is inconsistent with the request type");
                }
                detailResponseCache.set(response.getBatch());
            }
            var detailResponse = detailResponseCache.get();
            LOG.trace("receive (batch): {}", detailResponse); //$NON-NLS-1$
            if (SqlResponse.Batch.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
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
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setBatch(request)
                    .build()),
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
                    new QueryProcessor(request));
    }

    static class LoadProcessor implements MainResponseProcessor<Void> {
        private final AtomicReference<SqlResponse.ResultOnly> detailResponseCache = new AtomicReference<>();

        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (Objects.isNull(detailResponseCache.get())) {
                var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload));
                if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
                    // FIXME log error message
                    throw new IOException("response type is inconsistent with the request type");
                }
                detailResponseCache.set(response.getResultOnly());
            }
            var detailResponse = detailResponseCache.get();
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

    static class ListTablesProcessor implements MainResponseProcessor<TableList> {
        private final AtomicReference<SqlResponse.ListTables> detailResponseCache = new AtomicReference<>();

        @Override
        public TableList process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (Objects.isNull(detailResponseCache.get())) {
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
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
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
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setListTables(request)
                    .build()),
                new ListTablesProcessor().asResponseProcessor());
    }

    static class GetSearchPathProcessor implements MainResponseProcessor<SearchPath> {
        private final AtomicReference<SqlResponse.GetSearchPath> detailResponseCache = new AtomicReference<>();

        @Override
        public SearchPath process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (Objects.isNull(detailResponseCache.get())) {
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
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
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
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setGetSearchPath(request)
                    .build()),
                new GetSearchPathProcessor().asResponseProcessor());
    }

    static class GetErrorInfoProcessor implements MainResponseProcessor<SqlServiceException> {
        private final AtomicReference<SqlResponse.GetErrorInfo> detailResponseCache = new AtomicReference<>();

        @Override
        public SqlServiceException process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (Objects.isNull(detailResponseCache.get())) {
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
                    return new SqlServiceException(SqlServiceCode.valueOf(response.getStatus()), response.getDetail());
                case ERROR_NOT_FOUND:
                    return null;
                case ERROR:
                    var errorResponse = detailResponse.getError();
                    throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
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
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setGetErrorInfo(request)
                    .build()),
                new GetErrorInfoProcessor().asResponseProcessor());
    }

    static class DisposeTransactionProcessor implements MainResponseProcessor<Void> {
        private final AtomicReference<SqlResponse.Response> responseCache = new AtomicReference<>();

        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (Objects.isNull(responseCache.get())) {
                responseCache.set(SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(payload)));
            }
            var response = responseCache.get();
            switch (response.getResponseCase()) {
                case DISPOSE_TRANSACTION:
                    var detailResponse = response.getDisposeTransaction();
                    LOG.trace("receive (DisposeTransaction): {}", detailResponse); //$NON-NLS-1$
                    if (SqlResponse.DisposeTransaction.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
                        var errorResponse = detailResponse.getError();
                        throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
                    }
                    return null;
                case RESULT_ONLY:
                    var resultOnlyResponse = response.getResultOnly();
                    LOG.trace("receive (ResultOnly): {}", resultOnlyResponse); //$NON-NLS-1$
                    if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(resultOnlyResponse.getResultCase())) {
                        var errorResponse = resultOnlyResponse.getError();
                        throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
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
                toDelimitedByteArray(SqlRequest.Request.newBuilder()
                    .setDisposeTransaction(request)
                    .build()),
                new DisposeTransactionProcessor().asResponseProcessor());
    }

    // for compatibility
    static class DisconnectProcessor implements MainResponseProcessor<Void> {
        private final AtomicReference<SqlResponse.ResultOnly> detailResponseCache = new AtomicReference<>();

        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            if (Objects.isNull(detailResponseCache.get())) {
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
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            return null;
        }
    }

    @Override
    public void setCloseTimeout(Timeout timeout) {
        closeTimeout = timeout;
        resources.setCloseTimeout(timeout);
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        LOG.trace("closing underlying resources"); //$NON-NLS-1$
        resources.close();
        session.remove(this);
    }

    private byte[] toDelimitedByteArray(SqlRequest.Request request) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            request.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    // for diagnostic
    static class ResourceInfoAction implements Consumer<ServerResource> {
        String diagnosticInfo = "";

        @Override
        public void accept(ServerResource r) {
            if (Objects.nonNull(r)) {
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
