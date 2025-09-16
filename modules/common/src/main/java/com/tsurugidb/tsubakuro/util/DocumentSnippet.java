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
package com.tsurugidb.tsubakuro.util;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Represents a snippet of documentation, reflects to {@link Doc}.
 * @see Doc
 * @version 1.7.0
 */
public interface DocumentSnippet {

    /**
     * Returns the description of the element.
     * @return the lines of descriptions.
     */
    List<String> getDescription();

    /**
     * Returns the additional note for the element.
     * @return a list of note text
     */
    default List<String> getNotes() {
        return List.of();
    }

    /**
     * Returns the optional references for the target element.
     * @return a list of references
     */
    default List<Reference> getReferences() {
        return List.of();
    }

    /**
     * Returns an optional code number for the target element.
     * @return the element code if defined, otherwise {@code empty}
     * @since 1.7.0
     */
    default OptionalInt getCode() {
        return OptionalInt.empty();
    }

    /**
     * Represents a reference of the {@link DocumentedElement}.
     */
    interface Reference {

        /**
         * Returns optional title of this reference.
         * @return the title of the reference, or {@code empty} if it is not sure
         */
        Optional<String> getTitle();

        /**
         * Returns the location of this reference.
         * @return the location string, generally a URL
         */
        String getLocation();
    }
}
