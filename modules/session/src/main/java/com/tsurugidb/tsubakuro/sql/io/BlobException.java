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
package com.tsurugidb.tsubakuro.sql.io;

import java.io.IOException;

/**
 * An exception which occurs if BLOB handling fails.
 */
public class BlobException extends IOException {

    /**
     * Creates a new instance.
     * @param message the error message
     * @param cause the original cause
     */
    public BlobException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new instance.
     */
    public BlobException() {
        this(null, null);
    }

    /**
     * Creates a new instance.
     * @param message the error message
     */
    public BlobException(String message) {
        this(message, null);
    }

    /**
     * Creates a new instance.
     * @param cause the original cause
     */
    public BlobException(Throwable cause) {
        this(null, cause);
    }
}
