package com.tsurugidb.tsubakuro.debug.impl;

import java.io.IOException;

import javax.annotation.Nonnull;

import com.tsurugidb.debug.proto.DebugRequest;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * An interface to communicate Tsurugi debugging service.
 */
public interface DebugService extends ServerResource {

    /**
     * Requests {@code Logging} to debug service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     *      If the request was succeeded, future will returns a {@code null} value
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Void> send(@Nonnull DebugRequest.Logging request) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    default void close() throws ServerException, IOException, InterruptedException {
        // do nothing
    }
}
