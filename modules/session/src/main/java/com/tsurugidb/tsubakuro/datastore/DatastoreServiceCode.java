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
package com.tsurugidb.tsubakuro.datastore;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.util.Doc;

/**
 * Code of datastore service diagnostics.
 */
@Doc("Datastore service provides the capability to manage persistent data, including backup and restore.")
public enum DatastoreServiceCode implements DiagnosticCode {

    /**
     * Unknown error.
     */
    @Doc("unknown error was occurred in the datastore service.")
    UNKNOWN(0),

    /**
     * Backup session is already expired.
     */
    @Doc("the current backup session has been expired.")
    BACKUP_EXPIRED(1_01),

    /**
     * The target tag is already exists.
     */
    @Doc("the tag attempted to create already exists in the database.")
    TAG_ALREADY_EXISTS(6_01),

    /**
     * The target tag name is too long.
     */
    @Doc("the tag name is too long.")
    TAG_NAME_TOO_LONG(6_02),

    ;
    private final int codeNumber;

    DatastoreServiceCode(int codeNumber) {
        this.codeNumber = codeNumber;
    }

    /**
     * Structured code prefix of server diagnostics (`DSS-xxxxx`).
     */
    public static final String PREFIX_STRUCTURED_CODE = "DSS"; //$NON-NLS-1$

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
}
