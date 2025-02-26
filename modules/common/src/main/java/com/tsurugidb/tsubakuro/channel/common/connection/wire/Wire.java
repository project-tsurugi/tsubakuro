/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.tsubakuro.channel.common.connection.wire;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.tsubakuro.common.BlobInfo;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.Timeout;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;

/**
 * An abstract interface of communication path to the server.
 * This encapsulates how communicate to the server.
 * Application developer should not use this interface directly.
 */
@ThreadSafe
public interface Wire extends ServerResource {
    /**
     * send a message to the destination server.
     * <p>
     * The returned future will raise {@link CoreServiceException} on calling {@link FutureResponse#get()}.
     * This is because the server returned an erroneous response (e.g. the destination service is not found).
     * </p>
     * <p>
     * Otherwise, you can retrieve {@link Response} and its {@link Response#waitForMainResponse() main response}
     * contains reply message from the destination service specified by {@code serviceId}.
     * </p>
     * @param serviceId the destination service ID
     * @param payload the request payload
     * @return a future of the response
     * @throws IOException if I/O error was occurred while sending message
     */
    FutureResponse<? extends Response> send(int serviceId, @Nonnull ByteBuffer payload) throws IOException;

    /**
     * send a message to the destination server.
     * <p>
     * The returned future will raise {@link CoreServiceException} on calling {@link FutureResponse#get()}.
     * This is because the server returned an erroneous response (e.g. the destination service is not found).
     * </p>
     * <p>
     * Otherwise, you can retrieve {@link Response} and its {@link Response#waitForMainResponse() main response}
     * contains reply message from the destination service specified by {@code serviceId}.
     * </p>
     * @param serviceId the destination service ID
     * @param payload the request payload
     * @return a future of the response
     * @throws IOException if I/O error was occurred while sending message
     */
    default FutureResponse<? extends Response> send(int serviceId, @Nonnull byte[] payload) throws IOException {
        Objects.requireNonNull(payload);
        return send(serviceId, ByteBuffer.wrap(payload));
    }

    /**
     * send a message to the destination server.
     * <p>
     * The returned future will raise {@link CoreServiceException} on calling {@link FutureResponse#get()}.
     * This is because the server returned an erroneous response (e.g. the destination service is not found).
     * </p>
     * <p>
     * Otherwise, you can retrieve {@link Response} and its {@link Response#waitForMainResponse() main response}
     * contains reply message from the destination service specified by {@code serviceId}.
     * </p>
     * @param serviceId the destination service ID
     * @param payload the request payload
     * @param blobs the blobs to send
     * @return a future of the response
     * @throws IOException if I/O error was occurred while sending message
     *
     * @since 1.8.0
     */
    default FutureResponse<? extends Response> send(int serviceId, @Nonnull ByteBuffer payload, @Nonnull List<? extends BlobInfo> blobs) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * send a message to the destination server.
     * <p>
     * The returned future will raise {@link CoreServiceException} on calling {@link FutureResponse#get()}.
     * This is because the server returned an erroneous response (e.g. the destination service is not found).
     * </p>
     * <p>
     * Otherwise, you can retrieve {@link Response} and its {@link Response#waitForMainResponse() main response}
     * contains reply message from the destination service specified by {@code serviceId}.
     * </p>
     * @param serviceId the destination service ID
     * @param payload the request payload
     * @param blobs the blobs to send
     * @return a future of the response
     * @throws IOException if I/O error was occurred while sending message
     *
     * @since 1.8.0
     */
    default FutureResponse<? extends Response> send(int serviceId, @Nonnull byte[] payload, @Nonnull List<? extends BlobInfo> blobs) throws IOException {
        Objects.requireNonNull(payload);
        return send(serviceId, ByteBuffer.wrap(payload), blobs);
    }

    /**
     * updates credential information of this connection, and retries authenticate it.
     * @param credential the new credential information
     * @return a future of the authentication result:
     *      it may throw {@link CoreServiceException} if authentication was failed.
     * @throws IOException if I/O error was occurred while sending message
     */
    default FutureResponse<Void> updateCredential(@Nonnull Credential credential) throws IOException {
        Objects.requireNonNull(credential);
        return FutureResponse.returns(null);
    }

    /**
     * Create a ResultSetWire without a name, meaning that this wire is not connected
     * @return ResultSetWire
     * @throws IOException if I/O error was occurred while creating a ResultSetWire
     */
    default ResultSetWire createResultSetWire() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Provide dead/alive information of the server to which the session is connected
     * @return true when the server is alive
     */
    default boolean isAlive() {
        return false;
    }

    /**
     * Sets close timeout.
     * @param t the timeout
     */
    default void setCloseTimeout(Timeout t) {
        throw new UnsupportedOperationException();
    }

    /**
     * Closes this connection.
     * This method will be invoked one or more times.
     */
    @Override
    void close() throws IOException, InterruptedException, ServerException;

    // for diagnostic
    default String diagnosticInfo() {
        return " diagnosticInfo for the wire is not implementd";
    }
}
