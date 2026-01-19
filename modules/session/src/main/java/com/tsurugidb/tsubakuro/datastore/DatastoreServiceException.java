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

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * An exception which occurs on datastore service.
 */
public class DatastoreServiceException extends ServerException {

    private static final long serialVersionUID = 1L;

    /**
     * The diagnostic code.
     */
    private final DatastoreServiceCode code;

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param message the error message
     * @param cause the original cause
     */
    public DatastoreServiceException(@Nonnull DatastoreServiceCode code, @Nullable String message, @Nullable Throwable cause) {
        super(buildMessage(code, message), cause);
        this.code = code;
    }

    private static String buildMessage(DatastoreServiceCode code, @Nullable String message) {
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
    public DatastoreServiceException(@Nonnull DatastoreServiceCode code) {
        this(code, null, null);
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param message the error message
     */
    public DatastoreServiceException(@Nonnull DatastoreServiceCode code, @Nullable String message) {
        this(code, message, null);
    }

    /**
     * Creates a new instance.
     * @param code the diagnostic code
     * @param cause the original cause
     */
    public DatastoreServiceException(@Nonnull DatastoreServiceCode code, @Nullable Throwable cause) {
        this(code, null, cause);
    }

    @Override
    public DatastoreServiceCode getDiagnosticCode() {
        return code;
    }
}
