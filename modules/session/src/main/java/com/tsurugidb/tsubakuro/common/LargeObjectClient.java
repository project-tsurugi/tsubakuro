/*
 * Copyright 2023-2026 Project Tsurugi.
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
package com.tsurugidb.tsubakuro.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Path;

import com.tsurugidb.tsubakuro.common.exception.BlobException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * An abstract super interface of clients for handling Large Object upload and download.
 * This interface provides both upload APIs and download-related APIs
 * (e.g. opening streams/readers, cache access, and copy operations)
 * in a single client abstraction.
 *
 * @since 1.11.0
 */
public interface LargeObjectClient {
    /**
     * An interface representing the context of a Large Object download operation.
     * This is used to provide the transaction handle required for the download operation.
     */
    interface ContextId {
        /**
         * An enum representing the kind of context id.
         * This indicates that the context contains a transaction handle.
         */
        enum ContextIdKind {
          /**
           * Indicates that the context contains a transaction handle.
           * In this case, the transaction handle can be obtained by calling getTransactionHandle() method.
           */
          TRANSACTION,
        }

        /**
         * Returns the contextIdKind.
         * @return contextIdKind
         */
        ContextIdKind contextIdKind();

        /**
         * Returns the transaction handle.
         * @return the transaction handle, when {@link #contextIdKind()} is {@code TRANSACTION}
         * @throws IllegalStateException if no transaction handle is available for this context
         */
        long getTransactionHandle();
    }

    /**
     * Upload a Large Object passed from an InputStream.
     * @param source the InputStream through which the Large Object to be uploaded is passed
     * @return the LargeObjectInfo of the uploaded Large Object
     * @throws IOException if I/O error occurs while uploading Large Object
     * @throws BlobException if this instance is for privileged mode blob transfer
     */
    FutureResponse<LargeObjectInfo> upload(InputStream source) throws IOException, BlobException;

    /**
     * Upload a Large Object passed from a Reader.
     * @param source the Reader through which the Large Object to be uploaded is passed
     * @return the LargeObjectInfo of the uploaded Large Object
     * @throws IOException if I/O error occurs while uploading Large Object
     * @throws BlobException if this instance is for privileged mode blob transfer
     */
    FutureResponse<LargeObjectInfo> upload(Reader source) throws IOException, BlobException;

    /**
     * Upload a Large Object file.
     * The file cannot be deleted until the SQL execution that uses the <code>LargeObjectInfo</code>
     * returned by this method is complete, that is until the <code>FutureResponse</code> returned
     * by that SQL execution is retrieved via <code>get()</code> or <code>await()</code>.
     * In privileged mode, the file specified by the source parameter must be readable by the user running tsurugidb.
     * Otherwise, executing SQL using the uploaded Large Object will result in an error.
     * @param source the file path of the Large Object to be uploaded
     * @return the LargeObjectInfo of the uploaded Large Object
     * @throws IOException if I/O error occurs while uploading Large Object
     */
    FutureResponse<LargeObjectInfo> upload(Path source) throws IOException;

    /**
     * Returns an input stream for the Large Object.
     * @param contextId the contextId in the Large Object download operation
     * @param ref the large object reference
     * @return a future response of an InputStream of the Large Object
     * @throws IOException if I/O error occurs while sending request
     * @throws BlobException If the value of LargeObjectReference.getProvider() cannot be used for the download request
     */
    FutureResponse<InputStream> openInputStream(ContextId contextId, LargeObjectReference ref) throws IOException, BlobException;

    /**
     * Returns a reader for the Large Object.
     * @param contextId the contextId in the Large Object download operation
     * @param ref the large object reference
     * @return a future response of a Reader of the Large Object
     * @throws IOException if I/O error occurs while sending request
     * @throws BlobException If the value of LargeObjectReference.getProvider() cannot be used for the download request
     */
    FutureResponse<Reader> openReader(ContextId contextId, LargeObjectReference ref) throws IOException, BlobException;

    /**
     * Returns the LargeObjectCache for the Large Object.
     * The returned LargeObjectCache may be empty if blob_relay mode is used.
     * @param contextId the contextId in the Large Object download operation
     * @param ref the large object reference
     * @return a future response of LargeObjectCache
     * @throws IOException if I/O error occurs while sending request
     * @throws BlobException If the value of LargeObjectReference.getProvider() cannot be used for the download request
     */
    FutureResponse<LargeObjectCache> getLargeObjectCache(ContextId contextId, LargeObjectReference ref) throws IOException, BlobException;

    /**
     * Copy the large object to the file indicated by the given path.
     * <P>
     * If the destination file already exists, an IOException is thrown (for example,
     * FileAlreadyExistsException), and the BLOB data is not written.
     * If an error occurs while writing BLOB data, an IOException is thrown and the partially written file is deleted.
     * </P>
     * @param contextId the contextId in the BLOB/CLOB download operation
     * @param ref the large object reference
     * @param destination the path of the destination file
     * @return a future response of Void
     * @throws IOException if I/O error occurs while sending request
     * @throws BlobException If the value of LargeObjectReference.getProvider() cannot be used for the download request
     */
    FutureResponse<Void> copyTo(ContextId contextId, LargeObjectReference ref, Path destination) throws IOException, BlobException;
}