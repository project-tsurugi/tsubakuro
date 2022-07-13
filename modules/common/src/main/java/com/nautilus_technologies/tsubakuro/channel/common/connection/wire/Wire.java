package com.nautilus_technologies.tsubakuro.channel.common.connection.wire;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.nautilus_technologies.tsubakuro.channel.common.connection.Credential;
import com.nautilus_technologies.tsubakuro.exception.CoreServiceException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.ServerResource;
import com.nautilus_technologies.tsubakuro.channel.common.connection.sql.ResultSetWire;

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
     * Receive the message corresponding to the given responseWireHandle from the SQL server
     * @param handle the handle of communication wire to receive incoming message
     * @return SqlResponse.Response the response message received from the SQL server
     * @throws IOException error occurred in responce receive
     */
    ByteBuffer response(ResponseWireHandle handle) throws IOException;

    /**
     * Receive the message corresponding to the given responseWireHandle from the SQL server
     * @param handle the handle of communication wire to receive incoming message
     * @param timeout the maximum time to wait
     * @param unit the time unit of {@code timeout}
     * @return SqlResponse.Response the response message received from the SQL server
     * @throws IOException error occurred in responce receive
     */
    ByteBuffer response(ResponseWireHandle handle, long timeout, TimeUnit unit) throws TimeoutException, IOException;

    /**
     * Set to receive a Query type response by response box
     */
    void setResultSetMode(ResponseWireHandle handle);

    /**
     * release the message in the response box
     * This method can also be executed before receiving a response. However, at this time, 
     * such usage is only possible when a command using a ResultSet fails.
     * @param handle the handle to the response box
     */
    void release(ResponseWireHandle handle);

    /**
     * Create a ResultSetWire without a name, meaning that this wire is not connected
     * @return ResultSetWireImpl
    */
    ResultSetWire createResultSetWire() throws IOException;

    /**
     * Closes this connection.
     * This method will be invoked one or more times.
     */
    @Override
    void close() throws IOException, InterruptedException;
}
