package com.tsurugidb.tsubakuro.console.executor;

import java.io.IOException;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.console.model.Statement;

/**
 * An interface of Tsurugi SQL console engine.
 */
@FunctionalInterface
public interface Engine {

    /**
     * Executes a statement.
     * @param statement the target statement
     * @return {@code true} to continue execution, {@code false} if shutdown was requested
     * @throws EngineException if error occurred in engine itself
     * @throws ServerException if server side error was occurred
     * @throws IOException if I/O error was occurred while executing the statement
     * @throws InterruptedException if interrupted while executing the statement
     */
    boolean execute(
            @Nonnull Statement statement) throws EngineException, ServerException, IOException, InterruptedException;
}
