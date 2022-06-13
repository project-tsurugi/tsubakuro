package com.nautilus_technologies.tsubakuro.low.sql.util;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.TableMetadata;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.ServerResource;
import com.nautilus_technologies.tsubakuro.util.Timeout;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;

/**
 * Executes load operations.
 */
public class Load implements ServerResource {

    private final TableMetadata destination;

    private final PreparedStatement statement;

    private final List<SqlRequest.Parameter> parameters;

    /**
     * Creates a new instance.
     * @param destination the destination table information
     * @param statement the load statement
     * @param parameters the load statement arguments
     */
    public Load(
            @Nonnull TableMetadata destination,
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters) {
        Objects.requireNonNull(destination);
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        this.destination = destination;
        this.statement = statement;
        this.parameters = List.copyOf(parameters);
    }

    /**
     * Returns the destination table information.
     * @return the destination table information
     */
    public TableMetadata getDestination() {
        return destination;
    }

    /**
     * Submits {@link Transaction#executeLoad(PreparedStatement, Collection, Collection) load operation}.
     * @param transaction the transaction which the load operation executes in
     * @param files the input dump file paths, which SQL server can read them
     * @return a future response of the result set
     * @throws IOException if I/O error was occurred
     * @see Transaction#executeLoad(PreparedStatement, Collection, Collection)
     */
    public FutureResponse<SqlResponse.ResultOnly> submit(
            @Nonnull Transaction transaction,
            @Nonnull Collection<? extends Path> files) throws IOException {
        Objects.requireNonNull(transaction);
        Objects.requireNonNull(files);
        return transaction.executeLoad(statement, parameters, files);
    }

    /**
     * Submits {@link Transaction#executeLoad(PreparedStatement, Collection, Collection) load operation}.
     * @param transaction the transaction which the load operation executes in
     * @param files the input dump file paths, which SQL server can read them
     * @return a future response of the result set
     * @throws IOException if I/O error was occurred
     * @see #submit(Transaction, Collection)
     */
    public FutureResponse<SqlResponse.ResultOnly> submit(
            @Nonnull Transaction transaction,
            @Nonnull Path... files) throws IOException {
        Objects.requireNonNull(transaction);
        Objects.requireNonNull(files);
        return submit(transaction, Arrays.asList(files));
    }

    @Override
    public void setCloseTimeout(Timeout timeout) {
        statement.setCloseTimeout(timeout);
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        statement.close();
    }
}
