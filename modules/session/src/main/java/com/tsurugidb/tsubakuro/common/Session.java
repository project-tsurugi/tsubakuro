package com.tsurugidb.tsubakuro.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.ResponseProcessor;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.ServerResourceHolder;
import com.tsurugidb.tsubakuro.util.Timeout;

/**
 * Represents a connection to Tsurugi server.
 */
@ThreadSafe
public interface Session extends ServerResource {
    /**
     * The major service message version for routing service.
     */
    int SERVICE_MESSAGE_VERSION_MAJOR = 0;

    /**
     * The minor service message version for routing service.
     */
    int SERVICE_MESSAGE_VERSION_MINOR = 0;

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
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
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
     * Requests to extend the session expiration time by the server default value.
     * <p>
     * The resources underlying this session will be disposed after this session was expired.
     * To extend the expiration time, clients should continue to send requests in this session, or update expiration time explicitly by using this method.
     * </p>
     * <p>
     * If the session expiration is disabled, this does nothing.
     * </p>
     * @return the future response of the request;
     *     it will raise {@link CoreServiceException} if request was failure
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<Void> updateExpirationTime() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests to update the session expiration time.
     * <p>
     * The resources underlying this session will be disposed after this session was expired.
     * To extend the expiration time, clients should continue to send requests in this session, or update expiration time explicitly by using this method.
     * </p>
     * <p>
     * If the specified expiration time is too long, the server will automatically shorten it to its limit.
     * Or, if the time is too short or less than zero, the server sometimes omits the request.
     * </p>
     * <p>
     * If the session expiration is disabled, this does nothing.
     * </p>
     * @param time the expiration time from now
     * @param unit the time unit of expiration time
     * @return the future response of the request;
     *     it will raise {@link CoreServiceException} if request was failure
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<Void> updateExpirationTime(long time, @Nonnull TimeUnit unit) throws IOException {
        throw new UnsupportedOperationException();
    }

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

    /**
     * Provides close timeout object to tha caller
     * @return the close timeout that this session uses
     */
    Timeout getCloseTimeout();

    /**
     * Provide dead/alive information of the server to which the session is connected
     * @return true when the server is alive
     */
    default boolean isAlive() {
        var wire = getWire();
        if (wire == null) {
            return false;
        }
        return wire.isAlive();
    }

    /**
     * Request to shutdown the current session and wait for the running requests were finished.
     * <p>
     * Note that, this only send a shutdown request to the server, and some resources underlying this object may
     * be still living (e.g. JMX resources).
     * Please invoke {@link #close()} after this operation to dispose such the resources.
     * </p>
     * @param type the shutdown type
     * @return the future response of the request;
     *     it will raise {@link CoreServiceException} if request was failure
     * @throws IOException if I/O error was occurred while sending request
     * @see #close()
     */
    default FutureResponse<Void> shutdown(@Nonnull ShutdownType type) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Disposes the current session.
     * <p>
     * This may not wait for complete the ongoing requests, and it may cause the requests may still be in progress
     * after disconnected from the session.
     * You can use {@link #shutdown(ShutdownType)} to safely close this session
     * with waiting for complete the ongoing requests, if any.
     * </p>
     * <p>
     * After this operation, you cannot send any requests via this session,
     * including service clients attached to this session.
     * </p>
     * <p>
     * If this session is already disposed, this only cleanup the client-side resources corresponding to this session,
     * and never send any requests to the server.
     * </p>
     * @throws ServerException if error was occurred while disposing this session
     * @throws IOException if I/O error was occurred while disposing this session
     * @throws InterruptedException if interrupted while disposing this session
     * @see #shutdown(ShutdownType)
     */
    @Override
    void close() throws ServerException, IOException, InterruptedException;

    /**
     * Put a {@link ServerResource} to this.
     * The registered object will be closed in {@link ServerResourceHolder#close()}.
     * @param resource the resource related to the Session to be put
     */
    void put(@Nonnull ServerResource resource);

    /**
     * Remove a {@link ServerResource} from this.
     * If such the object is not registered, this does nothing.
     * @param resource the resource related to the Session to be removed
     */
    void remove(@Nonnull ServerResource resource);

    /**
     * Sends a message to the destination server.
     * @param <R> the result value type
     * @param serviceId the destination service ID
     * @param payload the message payload
     * @param processor the future response processor
     * @param background whether or not process responses in back ground
     * @return the future of response
     * @throws IOException if I/O error was occurred while requesting
     * @deprecated As BackgroudFutureResponse has been removed
     */
    @Deprecated 
    default <R> FutureResponse<R> send(
            int serviceId,
            @Nonnull byte[] payload,
            @Nonnull ResponseProcessor<R> processor,
            boolean background) throws IOException {
        if (background) {
            throw new UnsupportedOperationException();
        }
        return send(serviceId, payload, processor);
    }

    /**
     * Sends a message to the destination server.
     * @param <R> the result value type
     * @param serviceId the destination service ID
     * @param payload the message payload
     * @param processor the future response processor
     * @param background whether or not process responses in back ground
     * @return the future of response
     * @throws IOException if I/O error was occurred while requesting
     * @deprecated As BackgroudFutureResponse has been removed
     */
    @Deprecated 
    default <R> FutureResponse<R> send(
            int serviceId,
            @Nonnull ByteBuffer payload,
            @Nonnull ResponseProcessor<R> processor,
            boolean background) throws IOException {
        if (background) {
            throw new UnsupportedOperationException();
        }
        return send(serviceId, payload, processor);
    }
}