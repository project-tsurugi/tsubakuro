/*
 * Copyright 2025-2025 Project Tsurugi.
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
package com.tsurugidb.tsubakuro.system;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.util.Doc;

/**
 * Code of system service diagnostics.
 * @since 1.13.0
 */
@Doc(
    value = "System service is designed for external use of Tsurugi's system information provider service.")
public enum SystemServiceCode implements DiagnosticCode {

    /**
     * Unknown error.
     */
    @Doc("unknown error was occurred in the system service.")
    UNKNOWN(0),

    /**
     * System information is not found.
     */
    @Doc(
        value = "system information is not found in this session.",
        note = "system information may not be available when the system is booting or the requested information is missing.")
    NOT_FOUND(1_01),

    ;
    private final int codeNumber;

    SystemServiceCode(int codeNumber) {
        this.codeNumber = codeNumber;
    }

    /**
     * Structured code prefix of server diagnostics (`SYS-xxxxx`).
     */
    public static final String PREFIX_STRUCTURED_CODE = "SYS"; //$NON-NLS-1$

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
