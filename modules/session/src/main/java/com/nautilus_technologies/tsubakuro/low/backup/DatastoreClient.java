package com.nautilus_technologies.tsubakuro.low.backup;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.impl.low.backup.DatastoreClientImpl;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.ServerResource;
import com.nautilus_technologies.tateyama.proto.DatastoreCommonProtos;

/**
 * A datastore service client.
 * @see #attach(Session)
 */
public interface DatastoreClient extends ServerResource {

    /**
     * Attaches to the datastore service in the current session.
     * @param session the current session
     * @return the datastore service client
     */
    static DatastoreClient attach(@Nonnull Session session) {
        Objects.requireNonNull(session);
        return new DatastoreClientImpl(session);
    }

    /**
     * Starts backup.
     * @return the future response of started backup session
     * @throws IOException if I/O error was occurred while sending request
     * @throws InterruptedException if interrupted while sending request
     */
    default FutureResponse<Backup> beginBackup() throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Stops backup.
     * @return the future of void
     * @throws IOException if I/O error was occurred while sending request
     * @throws InterruptedException if interrupted while sending request
     */
    default FutureResponse<Void> endBackup(long id) throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Continues backup.
     * @return the future of void
     * @throws IOException if I/O error was occurred while sending request
     * @throws InterruptedException if interrupted while sending request
     */
    default FutureResponse<Void> continueBackup(long id, long ms) throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Estimates backup operation magnitude.
     * @return the future of void
     * @throws IOException if I/O error was occurred while sending request
     * @throws InterruptedException if interrupted while sending request
     */
    default FutureResponse<Void> estimateBackup() throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }
    
    /**
     * RestoreBackup backup operation magnitude.
     * @return the future of void
     * @throws IOException if I/O error was occurred while sending request
     * @throws InterruptedException if interrupted while sending request
     */
    default FutureResponse<Void> restoreBackup(String path, boolean keepBackup) throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Restores datastore from Point-in-Time recovery tag.
     * @return the future of void
     * @throws IOException if I/O error was occurred while sending request
     * @throws InterruptedException if interrupted while sending request
     */
    default FutureResponse<Void> restoreTag(String name) throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieves the list of registered Point-in-Time recovery tags.
     * @return the future of TagList
     * @throws IOException if I/O error was occurred while sending request
     * @throws InterruptedException if interrupted while sending request
     */
    default FutureResponse<TagList> listTag() throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Creates a new Point-in-Time recovery tag.
     * @return the future of void
     * @throws IOException if I/O error was occurred while sending request
     * @throws InterruptedException if interrupted while sending request
     */
    default FutureResponse<DatastoreCommonProtos.Tag> addTag(String name, String comment) throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieves a Point-in-Time recovery tag.
     * @return the future of void
     * @throws IOException if I/O error was occurred while sending request
     * @throws InterruptedException if interrupted while sending request
     */
    default FutureResponse<DatastoreCommonProtos.Tag> getTag(String name) throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Removes a Point-in-Time recovery tag.
     * @return the future of void
     * @throws IOException if I/O error was occurred while sending request
     * @throws InterruptedException if interrupted while sending request
     */
    default FutureResponse<Void> removeTag(String name) throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Disposes the underlying server resources.
     * Note that, this never closes the underlying {@link Session}.
     */
    @Override
    default void close() throws ServerException, IOException, InterruptedException {
        // do nothing
    }
}
