/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.tsubakuro.sql;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.sql.proto.SqlResponse;

/**
 * Represents transaction status in SQL service
 *
 * @since 1.9.0
 */
public enum TransactionStatus {

    /**
     * the transaction status unknown or not provided
     */
    UNTRACKED("UNTRACKED"),

    /**
     * before the commit process begins
     */
    RUNNING("RUNNING"),

    /**
     * started processing commit but has not reached AVAILABLE
     */
    COMMITTING("COMMITTING"),

    /**
     * reached the AVAILABLE state of the commit process, but has not reached STORED
     */
    AVAILABLE("AVAILABLE"),

    /**
     * reached the STORED state of the commit process, but has not reached PROPAGATED
     */
    STORED("STORED"),

    /**
     * reached the state of PROPAGATED commit processing
     */
    PROPAGATED("PROPAGATED"),

    /**
     * initiated the abort process
     */
    ABORTING("ABORTING"),

    /**
     * completed abort processing
     */
    ABORTED("ABORTED"),

    /**
     * unknown
     */
    UNSPECIFIED("UNSPECIFIED"),
    ;

    private final String status;
    
    /**
     * Creates a new instance.
     * @param status the status label
     */
    TransactionStatus(String status) {
        this.status = status;
    }

    /**
     * Returns the status label.
     * @return the status label
     */
    public String getStatusLabel() {
        return status;
    }

    private static TransactionStatus of(SqlResponse.TransactionStatus status) {
        switch (status) {
        case UNTRACKED: return UNTRACKED;
        case RUNNING: return RUNNING;
        case COMMITTING: return COMMITTING;
        case AVAILABLE: return AVAILABLE;
        case STORED: return STORED;
        case PROPAGATED: return PROPAGATED;
        case ABORTING: return ABORTING;
        case ABORTED: return ABORTED;
        case TRANSACTION_STATUS_UNSPECIFIED: return UNSPECIFIED;
        default: throw new AssertionError("status code given is undefined: " + status);
        }
    }

    /**
     * Creates a new object.
     * @param success the SqlResponse.GetTransactionStatus.Success message
     * @return a TransactionStatusWithMessage object
     */
    public static TransactionStatusWithMessage of(@Nonnull SqlResponse.GetTransactionStatus.Success success) {
        Objects.requireNonNull(success);
        return new TransactionStatusWithMessage(TransactionStatus.of(success.getStatus()), success.getMessage());
    }

    /**
     * Immutable wrapper class for TransactionStatus with an associated message.
     */
    public static final class TransactionStatusWithMessage {
        private final TransactionStatus status;
        private final String message;
        private TransactionStatusWithMessage(TransactionStatus status, String message) {
            this.status = Objects.requireNonNull(status);
            this.message = Objects.requireNonNull(message);
        }
        /**
         * Returns the status.
         * @return the status
         */
        public TransactionStatus getStatus() {
            return status;
        }
        /**
         * Returns the message.
         * @return the message
         */
        public String getMessage() {
            return message;
        }
    }
}
