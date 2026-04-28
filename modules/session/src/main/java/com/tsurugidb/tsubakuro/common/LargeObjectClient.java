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
import java.util.Optional;

import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * An abstract super interface of clients for handling LOB upload and download.
 * This interface provides both upload APIs and download-related APIs
 * (e.g. opening streams/readers, cache access, and copy operations)
 * in a single client abstraction.
 *
 * @since 1.10.0
 */
public interface LargeObjectClient {
    /**
     * An interface representing the context of a LOB download operation.
     * This is used to provide necessary information for the download operation,
     * such as transaction id or session id.
     */
    interface ContextId {
      /**
       * An enum representing the kind of context id.
       * This is used to indicate whether the context id is a transaction id or a session id.  
       */
        enum ContextIdKind {
          /**
           * Indicates that the context id is a session id. In this case, the session id can be obtained by calling getSessionId() method.
           */
          SESSION,
          /**
           * Indicates that the context id is a transaction id. In this case, the transaction id can be obtained by calling getTransactionId() method.
           */
          TRANSACTION,
        }

        /**
         * Returns the contextIdKind.
         * @return contextIdKind
         */
        ContextIdKind contextIdKind();

        /**
         * Returns the transaction.
         * @return transactionId if transaction id has been set
         * @throws IOException if transactionId has not been set
         */
        long getTransactionId() throws IOException;

        /**
         * Returns the session id.
         * @return the session id, used only when a transaction id has not been set
         */
        long getSessionId();
    }

    /**
     * Upload a BLOB passed from an InputStream.
     * @param source the InputStream through which the BLOB to be uploaded is passed
     * @return the LargeObjectReferenceBase of the uploaded BLOB
     * @throws IOException if I/O error was occurred while uploading BLOB
     */
    LargeObjectReferenceBase uploadBlob(InputStream source) throws IOException;

    /**
     * Upload a CLOB passed from a Reader.
     * @param source the Reader through which the CLOB to be uploaded is passed
     * @return the LargeObjectReferenceBase of the uploaded CLOB
     * @throws IOException if I/O error was occurred while uploading CLOB
     */
    LargeObjectReferenceBase uploadClob(Reader source) throws IOException;

    /**
     * Returns an input stream for the blob.
     * @param contextId the contextId in the BLOB download operation
     * @param ref the large object reference
     * @return a future response of an InputStream of the BLOB
     * @throws IOException if I/O error was occurred while sending request
     */
    FutureResponse<InputStream> openInputStream(ContextId contextId, LargeObjectReferenceBase ref) throws IOException;

    /**
     * Returns a reader for the clob.
     * @param contextId the contextId in the CLOB download operation
     * @param ref the large object reference
     * @return a future response of a Reader of the CLOB
     * @throws IOException if I/O error was occurred while sending request
     */
    FutureResponse<Reader> openReader(ContextId contextId, LargeObjectReferenceBase ref) throws IOException;

    /**
     * Returns an object cache for the blob/clob.
     * @param contextId the contextId in the BLOB/CLOB download operation
     * @param ref the large object reference
     * @return a future response of a LargeObjectCache
     * @throws IOException if I/O error was occurred while sending request
     */
    FutureResponse<Optional<Path>> getLargeObjectCache(ContextId contextId, LargeObjectReferenceBase ref) throws IOException;

    /**
     * Copy the large object to the file indicated by the given path.
     * @param contextId the contextId in the [B|C]LOB download operation
     * @param ref the large object reference
     * @param destination the path of the destination file
     * @return a future response of Void
     * @throws IOException if I/O error was occurred while sending request
     */
    FutureResponse<Void> copyTo(ContextId contextId, LargeObjectReferenceBase ref, Path destination) throws IOException;
}
