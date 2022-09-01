package com.tsurugidb.tsubakuro.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.ResponseProcessor;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;

/**
 * Represents a connection to Tsurugi server.
 */
@ThreadSafe
public interface Session extends ServerResource {
    /**
     * Sends a message to the destination server.
     * @param <R> the result value type
     * @param serviceId the destination service ID
     * @param payload the message payload
     * @param processor the future response processor
     * @param background whether or not process responses in back ground
     * @return the future of response
     * @throws IOException if I/O error was occurred while requesting
     */
    <R> FutureResponse<R> send(
            int serviceId,
            @Nonnull byte[] payload,
            @Nonnull ResponseProcessor<R> processor,
            boolean background) throws IOException;

    /**
     * Sends a message to the destination server.
     * @param <R> the result value type
     * @param serviceId the destination service ID
     * @param payload the message payload
     * @param processor the future response processor
     * @param background whether or not process responses in back ground
     * @return the future of response
     * @throws IOException if I/O error was occurred while requesting
     */
    <R> FutureResponse<R> send(
            int serviceId,
            @Nonnull ByteBuffer payload,
            @Nonnull ResponseProcessor<R> processor,
            boolean background) throws IOException;

    /**
     * Sends a message to the destination server.
     * @param <R> the result value type
     * @param serviceId the destination service ID
     * @param payload the message payload
     * @param processor the response processor
     * @return the future of response
     * @throws IOException if I/O error was occurred while requesting
     */
    default <R> FutureResponse<R> send(
            int serviceId,
            @Nonnull byte[] payload,
            @Nonnull ResponseProcessor<R> processor) throws IOException {
        return send(serviceId, payload, processor, false);
    }

    /**
     * Sends a message to the destination server.
     * @param <R> the result value type
     * @param serviceId the destination service ID
     * @param payload the message payload
     * @param processor the response processor
     * @return the future of response
     * @throws IOException if I/O error was occurred while requesting
     */
    default <R> FutureResponse<R> send(
            int serviceId,
            @Nonnull ByteBuffer payload,
            @Nonnull ResponseProcessor<R> processor) throws IOException {
        return send(serviceId, payload, processor, false);
    }

    /**
     * updates credential information of this session, and retries authenticate it.
     * <p>
     * This is designed for credentials with time limit, like as temporary token based credentials.
     * </p>
     * @param credential the new credential information
     * @return a future of the authentication result:
     *      it may throw {@link CoreServiceException} if authentication was failed.
     * @throws IOException if I/O error was occurred while sending message
     */
    FutureResponse<Void> updateCredential(@Nonnull Credential credential) throws IOException;

    /**
     * Requests to update the session expiration time.
     * <p>
     * The resources underlying this session will be disposed after this session was expired.
     * To extend the expiration time, clients should continue to send requests in this session, or update expiration time explicitly by using this method.
     * </p>
     * <p>
     * If the specified expiration time is too long, the server will automatically shorten it to its limit.
     * </p>
     * @param time the expiration time from now
     * @param unit the time unit of expiration time
     * @return the future response of the request;
     *     it will raise {@link CoreServiceException} if request was failure
     * @throws IOException if I/O error was occurred while sending request
     */
    FutureResponse<Void> updateExpirationTime(long time, @Nonnull TimeUnit unit) throws IOException;

    /**
     * Connect this session to the SQL server.
     *
     * Note. How to connect to a SQL server is implementation dependent.
     * This implementation assumes that the session wire connected to the database is given.
     *
     * @param sessionWire the wire that connects to the Database
     */
    void connect(Wire sessionWire);

    /**
     * Provides wire to tha caller, exists as a temporal measure for sessionLink
     * @return the wire that this session uses
     */
    Wire getWire();
}
