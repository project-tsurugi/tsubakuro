package com.nautilus_technologies.tsubakuro.impl.low.common;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.nautilus_technologies.tsubakuro.channel.common.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.impl.low.backup.FutureBackupImpl;
import com.nautilus_technologies.tsubakuro.impl.low.sql.FutureExplainImpl;
import com.nautilus_technologies.tsubakuro.impl.low.sql.FuturePreparedStatementImpl;
import com.nautilus_technologies.tsubakuro.impl.low.sql.PreparedStatementImpl;
import com.nautilus_technologies.tsubakuro.impl.low.sql.TransactionImpl;
import com.nautilus_technologies.tsubakuro.low.backup.Backup;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.BeginDistiller;
import com.nautilus_technologies.tsubakuro.protos.ExplainDistiller;
import com.nautilus_technologies.tsubakuro.protos.PrepareDistiller;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.ResultOnlyDistiller;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.Pair;
import com.nautilus_technologies.tsubakuro.util.ServerResource;

/**
 * SessionLinkImpl type.
 */
public class SessionLinkImpl implements ServerResource {
    static final long SERVICE_ID_SQL = 3;
    private SessionWire wire;
    private final Set<TransactionImpl> transactions;
    private final Set<PreparedStatementImpl> preparedStatements;

    /**
     * Class constructor, called from SessionImpl
     * @param wire the wire that connects to the Database
     */
    public SessionLinkImpl(SessionWire wire) {
        this.wire = wire;
        // FIXME: use concurrent data structure instead
        this.transactions = new HashSet<>();
        this.preparedStatements = new HashSet<>();
    }

    /**
     * Send prepare request to the SQL server via wire.send().
     * @param request the request message encoded with protocol buffer
     * @return a Future of ResponseProtos.Prepare contains prepared statement handle
     * @throws IOException error occurred in sending request message
     */
    public FutureResponse<PreparedStatement> send(RequestProtos.Prepare.Builder request) throws IOException {
        if (Objects.isNull(wire)) {
            throw new IOException("already closed");
        }
        return new FuturePreparedStatementImpl(wire.<ResponseProtos.Prepare>send(SERVICE_ID_SQL, RequestProtos.Request.newBuilder().setPrepare(request), new PrepareDistiller()), this);
    };

    /**
     * Send explain request to the SQL server via wire.send().
     * @param request the request message encoded with protocol buffer
     * @return a Future of String contains a string to explain the plan
     * @throws IOException error occurred in sending request message
     */
    public FutureResponse<String> send(RequestProtos.Explain.Builder request) throws IOException {
        if (Objects.isNull(wire)) {
            throw new IOException("already closed");
        }
        return new FutureExplainImpl(wire.<ResponseProtos.Explain>send(SERVICE_ID_SQL, RequestProtos.Request.newBuilder().setExplain(request), new ExplainDistiller()));
    };

    /**
     * Send execute sql statement request to via wire.send()
     * @param request the request message encoded with protocol buffer
     * @return a Future of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in sending request message
     */
    public FutureResponse<ResponseProtos.ResultOnly> send(RequestProtos.ExecuteStatement.Builder request) throws IOException {
        if (Objects.isNull(wire)) {
            throw new IOException("already closed");
        }
        return wire.<ResponseProtos.ResultOnly>send(SERVICE_ID_SQL, RequestProtos.Request.newBuilder().setExecuteStatement(request), new ResultOnlyDistiller());
    };

    /**
     * Send execute prepared statement request to via wire.send()
     * @param request the request message encoded with protocol buffer
     * @return a Future of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in sending request message
     */
    public FutureResponse<ResponseProtos.ResultOnly> send(RequestProtos.ExecutePreparedStatement.Builder request) throws IOException {
        if (Objects.isNull(wire)) {
            throw new IOException("already closed");
        }
        return wire.<ResponseProtos.ResultOnly>send(SERVICE_ID_SQL, RequestProtos.Request.newBuilder().setExecutePreparedStatement(request), new ResultOnlyDistiller());
    }

    /**
     * Send execute sql query request to via wire.send()
     * @param request the request message encoded with protocol buffer
     * @return a Pair of a Future of ResponseProtos.ExecuteQuery contains the name of result set wire and record metadata,
     and a Future of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not.
     * @throws IOException error occurred in sending request message
     */
    public Pair<FutureResponse<ResponseProtos.ExecuteQuery>, FutureResponse<ResponseProtos.ResultOnly>> send(RequestProtos.ExecuteQuery.Builder request) throws IOException {
        if (Objects.isNull(wire)) {
            throw new IOException("already closed");
        }
        return wire.sendQuery(SERVICE_ID_SQL, RequestProtos.Request.newBuilder().setExecuteQuery(request));
    };

    /**
     * Send execute prepared query request to via wire.send()
     * @param request the request message encoded with protocol buffer
     * @return a Future of ResponseProtos.ExecuteQuery contains the name of result set wire and record metadata,
     and a Future of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not.
     * @throws IOException error occurred in sending request message
     */
    public Pair<FutureResponse<ResponseProtos.ExecuteQuery>, FutureResponse<ResponseProtos.ResultOnly>> send(RequestProtos.ExecutePreparedQuery.Builder request) throws IOException {
        if (Objects.isNull(wire)) {
            throw new IOException("already closed");
        }
        return wire.sendQuery(SERVICE_ID_SQL, RequestProtos.Request.newBuilder().setExecutePreparedQuery(request));
    };

    /**
     * Send begin request to via wire.send()
     * @param request the request message encoded with protocol buffer
     * @return a Future of ResponseProtos.Begin contains transaction handle
     * @throws IOException error occurred in sending request message
     */
    public FutureResponse<ResponseProtos.Begin> send(RequestProtos.Begin.Builder request) throws IOException {
        if (Objects.isNull(wire)) {
            throw new IOException("already closed");
        }
        return wire.<ResponseProtos.Begin>send(SERVICE_ID_SQL, RequestProtos.Request.newBuilder().setBegin(request), new BeginDistiller());
    };

    /**
     * Send commit request to via wire.send()
     * @param request the request message encoded with protocol buffer
     * @return a Future of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in sending request message
     */
    public FutureResponse<ResponseProtos.ResultOnly> send(RequestProtos.Commit.Builder request) throws IOException {
        if (Objects.isNull(wire)) {
            throw new IOException("already closed");
        }
        return wire.<ResponseProtos.ResultOnly>send(SERVICE_ID_SQL, RequestProtos.Request.newBuilder().setCommit(request), new ResultOnlyDistiller());
    };

    /**
     * Send rollback request to via wire.send()
     * @param request the request message encoded with protocol buffer
     * @return a Future of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in sending request message
     */
    public FutureResponse<ResponseProtos.ResultOnly> send(RequestProtos.Rollback.Builder request) throws IOException {
        if (Objects.isNull(wire)) {
            throw new IOException("already closed");
        }
        return wire.<ResponseProtos.ResultOnly>send(SERVICE_ID_SQL, RequestProtos.Request.newBuilder().setRollback(request), new ResultOnlyDistiller());
    };

    /**
     * Send disposePreparedStatement request to via wire.send()
     * @param request the request message encoded with protocol buffer
     * @return a Future of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in sending request message
     */
    public FutureResponse<ResponseProtos.ResultOnly> send(RequestProtos.DisposePreparedStatement.Builder request) throws IOException {
        if (Objects.isNull(wire)) {
            throw new IOException("already closed");
        }
        return wire.<ResponseProtos.ResultOnly>send(SERVICE_ID_SQL, RequestProtos.Request.newBuilder().setDisposePreparedStatement(request), new ResultOnlyDistiller());
    };

    /**
     * Send Disconnect request to via wire.send()
     * @param request the request message encoded with protocol buffer
     * @return a Future of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in sending request message
     */
    public FutureResponse<ResponseProtos.ResultOnly> send(RequestProtos.Disconnect.Builder request) throws IOException {
        if (Objects.isNull(wire)) {
            throw new IOException("already closed");
        }
        return wire.<ResponseProtos.ResultOnly>send(SERVICE_ID_SQL, RequestProtos.Request.newBuilder().setDisconnect(request), new ResultOnlyDistiller());
    };

    /**
     * Create a ResultSetWire without a name, meaning that this wire is not connected
     * @return a resultSetWire
     * @throws IOException error occurred in resultSetWire creation
     */
    public ResultSetWire createResultSetWire() throws IOException {
        if (Objects.isNull(wire)) {
            throw new IOException("already closed");
        }
        return wire.createResultSetWire();
    }

    /**
     * Send execute load request to via wire.send()
     * @param request the request message encoded with protocol buffer
     * @return a Future of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not
     * @throws IOException error occurred in sending request message
     */
    public FutureResponse<ResponseProtos.ResultOnly> send(RequestProtos.ExecuteLoad.Builder request) throws IOException {
        if (Objects.isNull(wire)) {
            throw new IOException("already closed");
        }
        return wire.<ResponseProtos.ResultOnly>send(SERVICE_ID_SQL, RequestProtos.Request.newBuilder().setExecuteLoad(request), new ResultOnlyDistiller());
    };

    /**
     * Send execute dump request to via wire.send()
     * @param request the request message encoded with protocol buffer
     * @return a Pair of a Future of ResponseProtos.ExecuteQuery contains the name of result set wire and record metadata,
     and a Future of ResponseProtos.ResultOnly indicate whether the command is processed successfully or not.
     * @throws IOException error occurred in sending request message
     */
    public Pair<FutureResponse<ResponseProtos.ExecuteQuery>, FutureResponse<ResponseProtos.ResultOnly>> send(RequestProtos.ExecuteDump.Builder request) throws IOException {
        if (Objects.isNull(wire)) {
            throw new IOException("already closed");
        }
        return wire.sendQuery(SERVICE_ID_SQL, RequestProtos.Request.newBuilder().setExecuteDump(request));
    };

    /**
     * Send beginBackup request to the backup service via wire.send().
     * @return a Future of FutureBackupImpl object
     * @throws IOException error occurred in creating backup session
     */
    public FutureResponse<Backup> send() throws IOException {
        if (Objects.isNull(wire)) {
            throw new IOException("already closed");
        }
        return new FutureBackupImpl();
    };

    /**
     * Add TransactionImpl to transactions
     * @param transaction the transaction that begins
     * @return true when transaction addition is successful
     */
    public boolean add(TransactionImpl transaction) {
        return transactions.add(transaction);
    }
    /**
     * Remove TransactionImpl from transactions
     * @param transaction the transaction that ends
     * @return true when transaction removal is successful
     */
    public boolean remove(TransactionImpl transaction) {
        return transactions.remove(transaction);
    }
    /**
     * Add PreparedStatementImpl to preparedStatements
     * @param preparedStatement the preparedStatement that has created
     * @return true when preparedStatement addition is successful
     */
    public boolean add(PreparedStatementImpl preparedStatement) {
        return preparedStatements.add(preparedStatement);
    }
    /**
     * Remove PreparedStatementImpl from preparedStatements
     * @param preparedStatement the preparedStatement that has discarded
     * @return true when preparedStatement removal is successful
     */
    public boolean remove(PreparedStatementImpl preparedStatement) {
        return preparedStatements.remove(preparedStatement);
    }

    void discardRemainingResources(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException {
        while (!transactions.isEmpty()) {
            try (var transaction = transactions.iterator().next()) {
                transaction.setCloseTimeout(timeout, unit);
            }
        }
        while (!preparedStatements.isEmpty()) {
            try (var preparedStatement = preparedStatements.iterator().next()) {
                preparedStatement.setCloseTimeout(timeout, unit);
            }
        }
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        if (Objects.nonNull(wire)) {
            wire.close();
            wire = null;
        }
    }
}
