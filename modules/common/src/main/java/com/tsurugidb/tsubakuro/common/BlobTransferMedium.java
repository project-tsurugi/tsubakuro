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

import java.util.Map;

/**
 * Blob transfer medium used by the LargeObjectClient.
 */
public interface BlobTransferMedium {
    /**
     * Gets the BlobTransferType Blob transfer type used in the session.
     * @return the BlobTransferType other than BlobTransferType.DEFAULT
     */
    default BlobTransferType getBlobTransferType() {
        throw new UnsupportedOperationException("getBlobTransferType is not implemented");
    }

    /**
     * Gets the parameters for the Blob transfer medium.
     * @return the parameters for the Blob transfer medium
     */
    default Map<String, String> parameters() {
        throw new UnsupportedOperationException("parameters is not implemented");
    }
}