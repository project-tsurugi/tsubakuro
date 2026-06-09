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
 * Blob transfer type used in the session.
 *
 * @since 1.16.0
 */
public enum BlobTransferType {
    /**
     * Does not use transfer type.
     * An {@code EndpointRequest.BlobTransferMedium} entry whose type is
     * {@code EndpointRequest.BlobTransferType.DOES_NOT_USE} will be generated and sent to the endpoint
     */
    DOES_NOT_USE,

    /**
     * Privileged transfer type.
     * An {@code EndpointRequest.BlobTransferMedium} entry whose type is
     * {@code EndpointRequest.BlobTransferType.PRIVILEGED} will be generated and sent to the endpoint
     */
    PRIVILEGED,

    /**
     * Blob Relay transfer type.
     * An {@code EndpointRequest.BlobTransferMedium} entry whose type is
     * {@code EndpointRequest.BlobTransferType.RELAY} will be generated and sent to the endpoint
     */
    RELAY,

    /**
     * Indicates the default transfer policy.
     * A list of {@code EndpointRequest.BlobTransferMedium} entries will be generated and sent
     * to the endpoint in priority order: first
     * {@code EndpointRequest.BlobTransferType.RELAY}, then {@code EndpointRequest.BlobTransferType.DOES_NOT_USE}
     */
    DEFAULT;
}