package com.tsurugidb.tsubakuro.console.executor;

import java.io.IOException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.util.ServerResource;
import com.tsurugidb.tateyama.proto.SqlRequest;
import com.tsurugidb.tsubakuro.console.model.Region;

/**
 * Processes SQL-related actions.
 */
public interface SqlProcessor extends ServerResource {

    /**
     * Returns whether or not the holding transaction is active.
     * @return {@code true} if the holding transaction is (probably) active, or {@code false} otherwise
     */
    boolean isTransactionActive();

    /**
     * Executes a SQL statement.
     * @param statement the target SQL statement text
     * @param region the region of the statement in the document
     * @return the result set of the execution, or {@code null} if the statement does not returns any results
     * @throws ServerException if server side error was occurred
     * @throws IOException if I/O error was occurred while executing the statement
     * @throws InterruptedException if interrupted while executing the statement
     */
    @Nullable ResultSet execute(
            @Nonnull String statement,
            @Nullable Region region) throws ServerException, IOException, InterruptedException;

    /**
     * Starts a new transaction.
     * After this operation, this object will hold the started transaction as active.
     * @param option the transaction option
     * @throws IllegalStateException if another transaction is active in this object
     * @throws ServerException if server side error was occurred
     * @throws IOException if I/O error was occurred while starting a transaction
     * @throws InterruptedException if interrupted while starting a transaction
     */
    void startTransaction(
            @Nonnull SqlRequest.TransactionOption option) throws ServerException, IOException, InterruptedException;

    /**
     * Commits the current transaction.
     * After this operation, this object will releases the holding transaction.
     * @param status the commit status wait for
     * @throws IllegalStateException if any transactions are active in this object
     * @throws ServerException if server side error was occurred
     * @throws IOException if I/O error was occurred while committing the transaction
     * @throws InterruptedException if interrupted while committing the transaction
     */
    void commitTransaction(
            @Nonnull SqlRequest.CommitStatus status) throws ServerException, IOException, InterruptedException;

    /**
     * Aborts the current transaction.
     * After this operation, this object will releases the holding transaction.
     * @throws ServerException if server side error was occurred
     * @throws IOException if I/O error was occurred while committing the transaction
     * @throws InterruptedException if interrupted while committing the transaction
     */
    void rollbackTransaction() throws ServerException, IOException, InterruptedException;

    @Override
    default void close() throws ServerException, IOException, InterruptedException {
        return;
    }
}
