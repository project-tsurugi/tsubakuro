package com.nautilus_technologies.tsubakuro.low.datastore;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.nautilus_technologies.tateyama.proto.DatastoreRequestProtos;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.ServerResource;

/**
 * An interface to communicate with datastore service.
 * @see DatastoreClient
 */
public interface DatastoreService extends ServerResource {

    /**
     * Requests {@code BackupBegin} to datastore service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     *      If the request was succeeded, future will returns a backup session object
     *      which helps to create a new backup
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Backup> send(DatastoreRequestProtos.BackupBegin request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code BackupEnd} to datastore service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Void> send(DatastoreRequestProtos.BackupEnd request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code BackupContinue} to datastore service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed
     * @throws IOException if I/O error was occurred while sending the request
     */
    @Deprecated
    default FutureResponse<Void> send(DatastoreRequestProtos.BackupContinue request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code BackupEstimate} to datastore service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<BackupEstimate> send(DatastoreRequestProtos.BackupEstimate request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code RestoreBackup} to datastore service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Void> send(DatastoreRequestProtos.RestoreBackup request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code RestoreTag} to datastore service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Void> send(DatastoreRequestProtos.RestoreTag request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code TagList} to datastore service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     *      If the request was succeeded, future will returns the all available tag list
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<List<Tag>> send(DatastoreRequestProtos.TagList request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code TagAdd} to datastore service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     *      If the request was succeeded, future will returns the added tag
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Tag> send(DatastoreRequestProtos.TagAdd request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code TagGet} to datastore service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     *      If the request was succeeded,
     *      future will returns the target tag or empty if there is no such the tag
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Optional<Tag>> send(DatastoreRequestProtos.TagGet request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code TagRemove} to datastore service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     *      If the request was succeeded,
     *      future will returns whether or not the target tag is removed
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Boolean> send(DatastoreRequestProtos.TagRemove request) throws IOException {
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
     * </p>
     * @param time the expiration time from now
     * @param unit the time unit of expiration time
     * @return the future response of the request;
     *     it will raise {@link CoreServiceException} if request was failure
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<Void> updateExpirationTime(long time, TimeUnit unit) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    default void close() throws ServerException, IOException, InterruptedException {
        // do nothing
    }
}
