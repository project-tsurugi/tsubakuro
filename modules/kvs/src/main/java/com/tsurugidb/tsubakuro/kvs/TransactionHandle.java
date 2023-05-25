package com.tsurugidb.tsubakuro.kvs;

import java.io.IOException;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * A handle of server side transaction.
 */
public interface TransactionHandle extends ServerResource {


    /**
     * Disposes the corresponding server side transaction.
     */
    @Override
    default void close() throws ServerException, IOException, InterruptedException {
        // do nothing
    }
}
