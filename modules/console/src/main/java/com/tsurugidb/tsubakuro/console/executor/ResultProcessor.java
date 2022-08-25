package com.tsurugidb.tsubakuro.console.executor;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;

/**
 * Processes {@link ResultSet}.
 */
@FunctionalInterface
public interface ResultProcessor extends Closeable {

    /**
     * Processes {@link ResultSet}.
     * @param target the target result set
     * @throws ServerException if server side error was occurred
     * @throws IOException if I/O error was occurred while processing the result set
     * @throws InterruptedException if interrupted while processing the result set
     */
    void process(@Nonnull ResultSet target) throws ServerException, IOException, InterruptedException;

    @Override
    default void close() throws IOException {
        return;
    }
}
