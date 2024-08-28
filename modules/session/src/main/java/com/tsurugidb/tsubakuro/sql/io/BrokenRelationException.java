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
package com.tsurugidb.tsubakuro.sql.io;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Exceptions for serialized data was broken.
 */
public class BrokenRelationException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Represents error kind of {@link BrokenRelationException}.
     */
    public enum Status {

        /**
         * Reached end of contents unexpectedly.
         */
        UNEXPECTED_END_OF_CONTENTS,

        /**
         * Reached end of contents unexpectedly.
         */
        UNEXPECTED_VALUE_TYPE,

        /**
         * Integer is out of range for the requested type.
         */
        INT_VALUE_OUT_OF_RANGE,
    }

    private final Status status;

    /**
     * Creates a new instance for unexpected EOF in input.
     * @return the created instance
     * @see Status#UNEXPECTED_END_OF_CONTENTS
     */
    public static BrokenRelationException sawUnexpectedEndOfContents() {
        return new BrokenRelationException(Status.UNEXPECTED_END_OF_CONTENTS,
                "relation is interruptibly closed");
    }

    /**
     * Creates a new instance for unexpected value type.
     * @param found the occurred value type
     * @param expected the expected value type
     * @return the created instance
     * @see Status#UNEXPECTED_VALUE_TYPE
     */
    public static BrokenRelationException sawUnexpectedValueType(
            @Nonnull EntryType found,
            @Nonnull EntryType expected) {
        Objects.requireNonNull(found);
        Objects.requireNonNull(expected);
        return new BrokenRelationException(Status.UNEXPECTED_VALUE_TYPE, MessageFormat.format(
                "value is type is inconsistent: found ''{0}'' but expected one is ''{1}''",
                found.name(),
                expected.name()));
    }

    /**
     * Creates a new instance for unexpected value type.
     * @param found the occurred value type
     * @param expected the expected value types
     * @return the created instance
     * @see Status#UNEXPECTED_VALUE_TYPE
     */
    public static BrokenRelationException sawUnexpectedValueType(
            @Nonnull EntryType found,
            @Nonnull Collection<? extends EntryType> expected) {
        Objects.requireNonNull(found);
        Objects.requireNonNull(expected);
        return new BrokenRelationException(Status.UNEXPECTED_VALUE_TYPE, MessageFormat.format(
                "value is type is inconsistent: found ''{0}'' but expected is one of {1}",
                found.name(),
                expected.stream()
                        .map(it -> String.format("'%s'", it.name())) //$NON-NLS-1$
                        .collect(Collectors.joining(", ", "(", ")")))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    /**
     * Creates a new instance for integer value is out of range in input.
     * @param requested the requested type
     * @param value the actual value
     * @return the created instance
     * @see Status#INT_VALUE_OUT_OF_RANGE
     */
    public static BrokenRelationException sawIntValueOutOfRange(@Nonnull Class<?> requested, long value) {
        Objects.requireNonNull(requested);
        return new BrokenRelationException(Status.INT_VALUE_OUT_OF_RANGE, MessageFormat.format(
                "value is out of range for ''{0}'': value={1,number}",
                requested.getName(),
                value));
    }

    /**
     * Creates a new instance.
     * @param status the error kind
     * @param message the error message
     */
    public BrokenRelationException(@Nonnull Status status, @Nullable String message) {
        super(message, null);
        Objects.requireNonNull(status);
        this.status = status;
    }

    /**
     * Returns the error kind.
     * @return the error kind
     */
    public Status getStatus() {
        return status;
    }
}
