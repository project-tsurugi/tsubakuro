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

/**
  * An interface representing the uploaded large object information.
  * <p>
  * If a prepared statement execution request is made, <code>LargeObjectInfo</code> becomes unavailable (behavior is undefined if used).
  * </p>
  *
  * @since 1.11.0
  */
public interface LargeObjectInfo {
    /**
     * The information type of uploaded large object.
     *
     * @since 1.11.0
     */
    enum InfoType {
          /**
           * Indicates that the information of the uploaded large object to the blob relay service.
           */
          BLOB_RELAY_REFERENCE,
          /**
           * Indicates that the information of the large object file, that is its file path as seen from the server.
           */
          SERVER_PATH,
        }

    /**
      * Returns the InfoType.
      * @return infoType
      */
    default InfoType getInfoType() {
        throw new UnsupportedOperationException("getInfoType is not implemented");
    }

    /**
     * Returns the BlobRelayReference.
     * @return the BlobRelayReference, when {@link #getInfoType()} is {@code BLOB_RELAY_REFERENCE}
     * @throws IllegalStateException when {@link #getInfoType()} is not {@code BLOB_RELAY_REFERENCE}
     */
    default BlobRelayReference getBlobRelayReference() {
        throw new UnsupportedOperationException("getBlobRelayReference is not implemented");
    }

    /**
     * Returns the Large Object file path string as seen from the server.
     * @return the Path representing the file path of the Large Object file, when {@link #getInfoType()} is {@code SERVER_PATH}
     * @throws IllegalStateException when {@link #getInfoType()} is not {@code SERVER_PATH}
     */
    default String getServerPath() {
        throw new UnsupportedOperationException("getServerPath is not implemented");
    }
}