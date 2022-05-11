package com.nautilus_technologies.tsubakuro.low.common;

import java.io.IOException;

import com.nautilus_technologies.tsubakuro.channel.common.SessionWire;
import com.nautilus_technologies.tsubakuro.low.backup.Backup;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.ServerResource;

/**
 * Session type.
 */
public interface Session extends ServerResource {
    /**
     * Connect this session to the Database
     * @param sessionWire the wire that connects to the Database
     */
    void connect(SessionWire sessionWire);

    /**
     * Begin the new transaction
     * @param readOnly specify whether the new transaction is read-only or not
     * @return a FutureResponse of the transaction
     * @throws IOException error occurred in BEGIN
     */
    @Deprecated
    FutureResponse<Transaction> createTransaction(boolean readOnly) throws IOException;

    /**
     * Begin the new transaction
     * @return a FutureResponse of the transaction
     * @throws IOException error occurred in BEGIN
     */
    FutureResponse<Transaction> createTransaction() throws IOException;

    /**
     * Begin the new transaction
     * @param option specify the transaction type
     * @return a FutureResponse of the transaction
     * @throws IOException error occurred in BEGIN
     */
    FutureResponse<Transaction> createTransaction(RequestProtos.TransactionOption option) throws IOException;

    @Deprecated
    default FutureResponse<Transaction> createTransaction(RequestProtos.TransactionOption.Builder option) throws IOException {
        return createTransaction(option.build());
    }

    /**
     * Request prepare to the SQL service
     * @param sql sql text for the command
     * @param placeHolder the set of place holder name and type of its variable encoded with protocol buffer
     * @return a FutureResponse holding the result of the SQL service
     * @throws IOException error occurred in PREPARE
     */
    FutureResponse<PreparedStatement> prepare(String sql, RequestProtos.PlaceHolder placeHolder) throws IOException;

    @Deprecated
    default FutureResponse<PreparedStatement> prepare(String sql, RequestProtos.PlaceHolder.Builder placeHolder) throws IOException {
        return prepare(sql, placeHolder.build());
    }

    /**
     * Request explain to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @return a FutureResponse holding a string to explain the plan
     * @throws IOException error occurred in EXPLAIN
     */
    FutureResponse<String> explain(PreparedStatement preparedStatement, RequestProtos.ParameterSet parameterSet) throws IOException;

    @Deprecated
    default FutureResponse<String> explain(PreparedStatement preparedStatement, RequestProtos.ParameterSet.Builder parameterSet) throws IOException {
        return explain(preparedStatement, parameterSet.build());
    }

    /**
     * Begin the new backup session
     * @return a FutureResponse of a backup session
     * @throws IOException error occurs during the backup session initiation process
     */
    FutureResponse<Backup> beginBackup() throws IOException;
}
