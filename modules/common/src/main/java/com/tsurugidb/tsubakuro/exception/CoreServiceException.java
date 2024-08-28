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
package com.tsurugidb.tsubakuro.exception;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An exception which occurs if Tsurugi OLTP server core is something wrong.
 */
public class CoreServiceException extends ServerException {

    private static final long serialVersionUID = 1L;

    private final CoreServiceCode code;

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param message the error message
     * @param cause the original cause
     */
    public CoreServiceException(@Nonnull CoreServiceCode code, @Nullable String message, @Nullable Throwable cause) {
        super(buildMessage(code, message), cause);
        this.code = code;
    }

    private static String buildMessage(CoreServiceCode code, @Nullable String message) {
        Objects.requireNonNull(code);
        if (message == null) {
            return String.format("%s: %s", code.getStructuredCode(), code.name());
        }
        return String.format("%s: %s", code.getStructuredCode(), message); //$NON-NLS-1$
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     */
    public CoreServiceException(@Nonnull CoreServiceCode code) {
        this(code, null, null);
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param message the error message
     */
    public CoreServiceException(@Nonnull CoreServiceCode code, @Nullable String message) {
        this(code, message, null);
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param cause the original cause
     */
    public CoreServiceException(@Nonnull CoreServiceCode code, @Nullable Throwable cause) {
        this(code, null, cause);
    }

    @Override
    public CoreServiceCode getDiagnosticCode() {
        return code;
    }
}
