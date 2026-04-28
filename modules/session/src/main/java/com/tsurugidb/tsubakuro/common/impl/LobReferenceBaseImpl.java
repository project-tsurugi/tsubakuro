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

import com.tsurugidb.tsubakuro.common.LargeObjectReferenceBase;
import com.tsurugidb.blob_relay.proto.BlobRelayCommon;

/**
 * Represents a reference to LOB data and corresponding Response.
 */
public final class LobReferenceBaseImpl implements LargeObjectReferenceBase {
    final BlobRelayCommon.BlobReference largeObjectReference;

    /**
     * Constructor
     * @param reference the reference of the LOB data
     */
    public LobReferenceBaseImpl(BlobRelayCommon.BlobReference reference) {
        largeObjectReference = reference;
    }

    @Override
    public long getProvider() {
        return largeObjectReference.getStorageId();
    }

    @Override
    public long getObjectId() {
        return largeObjectReference.getObjectId();
    }

    @Override
    public long getReferenceTag() {
        return largeObjectReference.getTag();
    }
}