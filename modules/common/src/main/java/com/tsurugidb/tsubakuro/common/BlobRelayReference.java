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

import com.tsurugidb.sql.proto.SqlRequest;

/**
 * A class that stores reference of the BLOB/CLOB uploaded.
 *
 * @since 1.11.0
 */
public class BlobRelayReference {
    private final long storageId;
    private final long objectId;
    private final long tag;

    /**
     * Class constructor.
     * @param storageId the storage id value
     * @param objectId the object id value
     * @param tag the reference tag value
     */
    public BlobRelayReference(long storageId, long objectId, long tag) {
      this.storageId = storageId;
      this.objectId = objectId;
      this.tag = tag;
    }

    /**
     * Class constructor.
     * @param proto the protobuf object containing the BLOB/CLOB reference information
     */
    public BlobRelayReference(SqlRequest.BlobRelayReference proto) {
      this.storageId = proto.getStorageId();
      this.objectId = proto.getObjectId();
      this.tag = proto.getTag();
    }

    /**
     * Returns the storage id of the Large Object data.
     * @return the storage id value
     */
    public long getStorageId() {
      return storageId;
    }

    /**
     * Returns the object id of the Large Object data.
     * @return the object id value
     */
    public long getObjectId() {
      return objectId;
    }

    /**
     * Returns the reference tag of the Large Object data.
     * @return the reference tag value
     */
    public long getReferenceTag() {
      return tag;
    }
}