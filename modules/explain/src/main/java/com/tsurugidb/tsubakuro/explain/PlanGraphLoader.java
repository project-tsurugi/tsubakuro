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
package com.tsurugidb.tsubakuro.explain;

import java.text.MessageFormat;
import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Analyzes execution plan.
 */
public interface PlanGraphLoader {

    /**
     * Returns whether or not this supports the specified content format and its version.
     * @param formatId the format version ID
     * @param formatVersion the format version number
     * @return {@code true} if this supports such the format, otherwise {@code false}
     */
    default boolean isSupported(@Nonnull String formatId, long formatVersion) {
        Objects.requireNonNull(formatId);
        return true;
    }

    /**
     * Analyzes the execution plan text and returns corresponding plan graph.
     * @param text the execution plan text
     * @return the analyzed plan graph
     * @throws PlanGraphException if the plan text is not valid to extract plan graph
     */
    PlanGraph load(@Nonnull String text) throws PlanGraphException;

    /**
     * Analyzes the execution plan text and returns corresponding plan graph.
     * @param formatId the content format ID
     * @param formatVersion the content format version
     * @param contents the execution plan text
     * @return the analyzed plan graph
     * @throws PlanGraphException if the format ID or version is not supported by this loader
     * @throws PlanGraphException if the plan text is not valid to extract plan graph
     */
    default PlanGraph load(@Nonnull String formatId, long formatVersion, @Nonnull String contents)
            throws PlanGraphException {
        Objects.requireNonNull(formatId);
        Objects.requireNonNull(contents);
        if (!isSupported(formatId, formatVersion)) {
            throw new PlanGraphException(MessageFormat.format(
                    "unsupported execution plan text format: {0}:{1}",
                    formatId,
                    formatVersion));
        }
        return load(contents);
    }
}
