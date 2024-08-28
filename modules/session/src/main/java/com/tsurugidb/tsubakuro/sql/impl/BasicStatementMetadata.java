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
package com.tsurugidb.tsubakuro.sql.impl;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.tsubakuro.sql.StatementMetadata;

/**
 * A basic implementation of {@link StatementMetadata}.
 */
public class BasicStatementMetadata implements StatementMetadata {

    private final String formatId;

    private final long formatVersion;

    private final String contents;

    private final List<? extends SqlCommon.Column> columns;

    /**
     * Creates a new instance.
     * @param formatId the content format ID
     * @param formatVersion the content format version
     * @param contents the statement description
     * @param columns the result set columns information of the statement
     */
    public BasicStatementMetadata(
            @Nonnull String formatId,
            long formatVersion,
            @Nonnull String contents,
            @Nonnull List<? extends SqlCommon.Column> columns) {
        Objects.requireNonNull(formatId);
        Objects.requireNonNull(contents);
        Objects.requireNonNull(columns);
        this.formatId = formatId;
        this.formatVersion = formatVersion;
        this.contents = contents;
        this.columns = columns;
    }

    @Override
    public String getFormatId() {
        return formatId;
    }

    @Override
    public long getFormatVersion() {
        return formatVersion;
    }

    @Override
    public String getContents() {
        return contents;
    }

    @Override
    public List<? extends SqlCommon.Column> getColumns() {
        return columns;
    }

    @Override
    public String toString() {
        return String.format("BasicStatementMetadata [formatId=%s, formatVersion=%s, contents=%s, columns=%s]", //$NON-NLS-1$
                formatId, formatVersion, contents, columns);
    }
}
