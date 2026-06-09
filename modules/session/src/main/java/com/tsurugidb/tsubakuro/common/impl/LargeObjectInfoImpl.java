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
package com.tsurugidb.tsubakuro.common.impl;

import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.tsubakuro.common.BlobRelayReference;
import com.tsurugidb.tsubakuro.common.LargeObjectInfo;

/**
  * A class representing the uploaded large object information.
  *
  * @since 1.16.0
  */
public class LargeObjectInfoImpl implements LargeObjectInfo {
    private final SqlRequest.ClientOnlyLargeObjectInfo largeObjectInfo;

    /**
     * Class constructor.
     * @param parameter the protobuf object containing the large object information
     */
    public LargeObjectInfoImpl(SqlRequest.Parameter parameter) {
        if (parameter == null) {
            throw new IllegalArgumentException("parameter must not be null");
        }
        switch (parameter.getValueCase()) {
            case LARGE_OBJECT_INFO_CLOB:
                this.largeObjectInfo = parameter.getLargeObjectInfoClob();
                break;
            case LARGE_OBJECT_INFO_BLOB:
                this.largeObjectInfo = parameter.getLargeObjectInfoBlob();
                break;
            default:
                throw new IllegalArgumentException("parameter must be of type LARGE_OBJECT");
        }
    }
    /**
     * Class constructor.
     * @param info the protobuf object containing the large object information
     */
    public LargeObjectInfoImpl(SqlRequest.ClientOnlyLargeObjectInfo info) {
        if (info == null) {
            throw new IllegalArgumentException("info must not be null");
        }
        this.largeObjectInfo = info;
    }

    @Override
    public InfoType getInfoType() {
        switch (largeObjectInfo.getDataCase()) {
            case BLOB_RELAY_REFERENCE:
                return InfoType.BLOB_RELAY_REFERENCE;
            case SERVER_PATH:
                return InfoType.SERVER_PATH;

            default:
                throw new IllegalStateException("Unknown large object type");
        }
    }

    @Override
    public BlobRelayReference getBlobRelayReference() {
        if (getInfoType() != InfoType.BLOB_RELAY_REFERENCE) {
            throw new IllegalStateException("Large object info type is not BLOB_RELAY_REFERENCE");
        }
        BlobRelayReference blobRelayReference = new BlobRelayReference(largeObjectInfo.getBlobRelayReference());
        return blobRelayReference;
    }

    @Override
    public String getServerPath() {
        if (getInfoType() != InfoType.SERVER_PATH) {
            throw new IllegalStateException("Large object info type is not SERVER_PATH");
        }
        return largeObjectInfo.getServerPath();
    }
}