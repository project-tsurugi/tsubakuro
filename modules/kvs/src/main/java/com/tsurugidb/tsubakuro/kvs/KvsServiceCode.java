/*
 * Copyright 2023-2024 Project Tsurugi.
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
package com.tsurugidb.tsubakuro.kvs;

import java.util.HashMap;

import javax.annotation.Nonnegative;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.util.Doc;

/**
 * Code of KVS service diagnostics.
 */
@Doc(
        value = "KVS (Key Value Store) service is designed to access the database directly without using SQL.",
        note = "This feature is able to use partially, but still under development.")
public enum KvsServiceCode implements DiagnosticCode {

    /**
     * the target element does not exist.
     */
    @Doc("the target element does not exist in the database.")
    NOT_FOUND(1),

    /**
     * the target element already exists.
     */
    @Doc("the target element attempted to create already exists in the database.")
    ALREADY_EXISTS(2),

    /**
     * the transaction operation was rollbacked by user.
     */
    @Doc("the requested transaction operation was rollbacked by user.")
    USER_ROLLBACK(3),

    /**
     * the transaction operation is waiting for other transaction.
     */
    @Doc("the requested transaction operation is waiting for other transaction.")
    WAITING_FOR_OTHER_TRANSACTION(4),

    /**
     * Unknown error.
     */
    @Doc("unknown error was occurred in the kvs service.")
    UNKNOWN(100),

    /**
     * I/O error.
     */
    @Doc("I/O error was occurred in the server.")
    IO_ERROR(102),

    /**
     * API arguments are invalid.
     */
    @Doc("the service received a request message with an invalid argument.")
    INVALID_ARGUMENT(103),

    /**
     * API state is invalid.
     */
    @Doc("the operation was requested in illegal or inappropriate time.")
    INVALID_STATE(104),

    /**
     * operation is unsupported.
     */
    @Doc("the requested operation is not supported.")
    UNSUPPORTED(105),

    /**
     * transaction operation met an user-defined error.
     * <p>
     * this code is returned only from transaction_exec() and transaction_commit()
     * </p>
     */
    @Doc("the transaction operation met an user-defined error.")
    USER_ERROR(106),

    /**
     * transaction was aborted.
     */
    @Doc("the transaction was aborted.")
    ABORTED(107),

    /**
     * transaction was aborted, but retry might resolve the situation.
     */
    @Doc("the transaction was aborted, but retry might resolve the situation.")
    ABORT_RETRYABLE(108),

    /**
     * api call timed out.
     */
    @Doc("the request was timed out.")
    TIME_OUT(109),

    /**
     * the feature is not yet implemented.
     */
    @Doc("the requested feature is not yet implemented.")
    NOT_IMPLEMENTED(110),

    /**
     * the operation is not valid.
     */
    @Doc("the requested operation is not valid.")
    ILLEGAL_OPERATION(111),

    /**
     * the operation conflicted with write preserve.
     */
    @Doc("the requested operation conflicted with a write preserve.")
    CONFLICT_ON_WRITE_PRESERVE(112),

    /**
     * long tx issued write operation without write preserve.
     */
    @Doc("long transaction issued write operation without a write preserve.")
    WRITE_WITHOUT_WRITE_PRESERVE(114),

    /**
     * transaction is inactive.
     * <p>
     * transaction is inactive because it's already committed or aborted. The request is failed.
     * </p>
     */
    @Doc(
            value = "the transaction is inactive.",
            note = "the transaction is inactive because it's already committed or aborted. The request is failed.")
    INACTIVE_TRANSACTION(115),

    /**
     * requested operation was blocked by concurrent operation.
     * <p>
     * the request couldn't be fulfilled due to the operation concurrently executed by other transaction.
     * After the blocking transaction completes, re-trying the request may lead to different result.
     * </p>
     */
    @Doc(
            value = "the requested operation was blocked by concurrent operation.",
            note = "the request couldn't be fulfilled due to the operation concurrently executed by other transaction."
                    + " After the blocking transaction completes, re-trying the request may lead to different result.")
    BLOCKED_BY_CONCURRENT_OPERATION(116),

    /**
     * reached resource limit and request could not be accomplished.
     */
    @Doc("the server reached resource limit and the request could not be accomplished.")
    RESOURCE_LIMIT_REACHED(117),

    /**
     * key length passed to the API was invalid.
     */
    @Doc("key length passed to the API was invalid.")
    INVALID_KEY_LENGTH(118),

    /**
     * operation result data was too large.
     */
    @Doc("the operation result data was too large.")
    RESULT_TOO_LARGE(1_001),

    /**
     * target resource is not authorized.
     */
    @Doc("target resource is not authorized to use.")
    NOT_AUTHORIZED(2_001),


    /**
     * transaction was aborted by writing out of write preservation, or writing in read only transaction.
     */
    @Doc("the transaction was aborted by writing out of write preservation, or writing in read only transaction.")
    WRITE_PROTECTED(12_002),

    /**
     * the specified table was not found.
     */
    @Doc("the specified table was not found.")
    TABLE_NOT_FOUND(20_001),

    /**
     * the specified column was not found.
     */
    @Doc("the specified column was not found.")
    COLUMN_NOT_FOUND(20_002),

    /**
     * the column type was inconsistent.
     */
    @Doc("the column type was inconsistent.")
    COLUMN_TYPE_MISMATCH(20_003),

    /**
     * the search key was mismatch for the table or index.
     */
    @Doc("the search key was mismatch for the table or index.")
    MISMATCH_KEY(20_011),

    /**
     * several columns were not specified in {@code PUT} operation.
     */
    @Doc("several columns were not specified in PUT operation.")
    INCOMPLETE_COLUMNS(20_021),

    /**
     * operations was failed by integrity constraint violation.
     */
    @Doc("operations was failed by integrity constraint violation.")
    INTEGRITY_CONSTRAINT_VIOLATION(30_001),

    ;
    private final int codeNumber;

    KvsServiceCode(@Nonnegative int codeNumber) {
        this.codeNumber = codeNumber;
    }

    /**
     * Structured code prefix of server diagnostics (`KVS-xxxxx`).
     */
    public static final String PREFIX_STRUCTURED_CODE = "KVS"; //$NON-NLS-1$

    @Override
    public String getStructuredCode() {
        return String.format("%s-%05d", PREFIX_STRUCTURED_CODE, getCodeNumber()); //$NON-NLS-1$
    }

    @Override
    public int getCodeNumber() {
        return codeNumber;
    }

    @Override
    public String toString() {
        return String.format("%s (%s)", getStructuredCode(), name()); //$NON-NLS-1$
    }

    private static final HashMap<Integer, KvsServiceCode> MAP = new HashMap<>();

    static {
        for (var v : KvsServiceCode.values()) {
            MAP.put(v.getCodeNumber(), v);
        }
    }

    /**
     * get KvsServiceCode object with code
     * @param code the code of the KvsServiceCode
     * @return KvsServiceCode with the valid code,
     *          or KvsServiceCode.UNKNOWN if the code is unknown.
     */
    public static KvsServiceCode getInstance(int code) {
        KvsServiceCode v = MAP.get(code);
        if (v != null) {
            return v;
        } else {
            return KvsServiceCode.UNKNOWN;
        }
    }

}
