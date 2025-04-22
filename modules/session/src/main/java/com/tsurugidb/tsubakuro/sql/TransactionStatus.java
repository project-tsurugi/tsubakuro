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
 * transaction staus in SQL service
 */
public enum TransactionStatus {

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
     * @param code the diagnostic code
     */
    TransactionStatus(String status) {
        this.status = status;
    }

    /**
     * Returns the status lavel.
     * @return the status label
     */
    public String getStatusLabel() {
        return status;
    }

    public static TransactionStatusWithMessage of(@Nonnull SqlResponse.GetTransactionStatus.Success success) {
        Objects.requireNonNull(success);
        switch (success.getStatus()) {
        case RUNNING: return new TransactionStatusWithMessage(RUNNING, success.getMessage());
        case COMMITTING: return new TransactionStatusWithMessage(COMMITTING, success.getMessage());
        case AVAILABLE: return new TransactionStatusWithMessage(AVAILABLE, success.getMessage());
        case STORED: return new TransactionStatusWithMessage(STORED, success.getMessage());
        case PROPAGATED: return new TransactionStatusWithMessage(PROPAGATED, success.getMessage());
        case ABORTING: return new TransactionStatusWithMessage(ABORTING, success.getMessage());
        case ABORTED: return new TransactionStatusWithMessage(ABORTED, success.getMessage());
        case TRANSACTION_STATUS_UNSPECIFIED: return new TransactionStatusWithMessage(UNSPECIFIED, success.getMessage());
        default: throw new AssertionError("status code given is undefined: " + success.getStatus());
        }
    }

    /**
     * Immutable wrapper class for TransactionStatus with an associated message.
     */
    public static final class TransactionStatusWithMessage {
        private final TransactionStatus status;
        private final String message;
        public TransactionStatusWithMessage(TransactionStatus status, String message) {
            this.status = Objects.requireNonNull(status);
            this.message = Objects.requireNonNull(message);
        }
        public TransactionStatus getStatus() {
            return status;
        }
        public String getMessage() {
            return message;
        }
    }
}
