package com.nautilus_technologies.tsubakuro.low.common;

import java.util.concurrent.Future;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.backup.Backup;
import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;

/**
 * Session type.
 */
public interface Session extends CloseableIpc {
    /**
     * Connect this session to the Database
     * @param sessionWire the wire that connects to the Database
     */
    void connect(SessionWire sessionWire);

    /**
     * Begin the new transaction
     * @param readOnly specify whether the new transaction is read-only or not
     * @return a Future of the transaction
     * @throws IOException error occurred in BEGIN
     */
    @Deprecated
    Future<Transaction> createTransaction(boolean readOnly) throws IOException;

    /**
     * Begin the new transaction
     * @return a Future of the transaction
     * @throws IOException error occurred in BEGIN
     */
    Future<Transaction> createTransaction() throws IOException;

    /**
     * Begin the new transaction
     * @param option specify the transaction type
     * @return a Future of the transaction
     * @throws IOException error occurred in BEGIN
     */
    Future<Transaction> createTransaction(RequestProtos.TransactionOption option) throws IOException;
    @Deprecated
    Future<Transaction> createTransaction(RequestProtos.TransactionOption.Builder option) throws IOException;

    /**
     * Request prepare to the SQL service
     * @param sql sql text for the command
     * @param placeHolder the set of place holder name and type of its variable encoded with protocol buffer
     * @return a Future holding the result of the SQL service
     * @throws IOException error occurred in PREPARE
     */
    Future<PreparedStatement> prepare(String sql, RequestProtos.PlaceHolder placeHolder) throws IOException;
    @Deprecated
    Future<PreparedStatement> prepare(String sql, RequestProtos.PlaceHolder.Builder placeHolder) throws IOException;

    /**
     * Request explain to the SQL service
     * @param preparedStatement prepared statement for the command
     * @param parameterSet parameter set for the prepared statement encoded with protocol buffer
     * @return a Future holding a string to explain the plan
     * @throws IOException error occurred in EXPLAIN
     */
    Future<String> explain(PreparedStatement preparedStatement, RequestProtos.ParameterSet parameterSet) throws IOException;
    @Deprecated
    Future<String> explain(PreparedStatement preparedStatement, RequestProtos.ParameterSet.Builder parameterSet) throws IOException;

    /**
     * Begin the new backup session
     * @return a Future of a backup session
     * @throws IOException error occurs during the backup session initiation process
     */
    Future<Backup> beginBackup() throws IOException;
}