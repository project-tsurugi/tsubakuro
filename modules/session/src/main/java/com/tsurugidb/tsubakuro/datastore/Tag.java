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

import java.text.MessageFormat;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Estimated information of backup operation.
 */
public class Tag {

    private final String name;

    private final String comment;

    private final String author;

    private final Instant timestamp;

    /**
     * Creates a new instance.
     * @param name the tag name
     * @param comment the tag comment
     * @param author the tag author
     * @param timestamp the created time
     */
    public Tag(
            @Nonnull String name,
            @Nullable String comment,
            @Nullable String author,
            @Nonnull Instant timestamp) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(timestamp);
        this.name = name;
        this.comment = comment;
        this.author = author;
        this.timestamp = timestamp;
    }

    /**
     * Returns the tag name.
     * @return the tag name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the tag comment.
     * @return the tag comment
     */
    public Optional<String> getComment() {
        return Optional.ofNullable(comment);
    }

    /**
     * Returns the tag author name.
     * @return the author name
     */
    public Optional<String> getAuthor() {
        return Optional.ofNullable(author);
    }

    /**
     * Returns the created timestamp.
     * @return the created timestamp
     */
    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(author, comment, name, timestamp);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Tag other = (Tag) obj;
        return Objects.equals(author, other.author)
                && Objects.equals(comment, other.comment)
                && Objects.equals(name, other.name)
                && Objects.equals(timestamp, other.timestamp);
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Tag(name={0}, comment={1}, author={2}, timestamp={3})",
                name,
                comment,
                author,
                timestamp);
    }
}
