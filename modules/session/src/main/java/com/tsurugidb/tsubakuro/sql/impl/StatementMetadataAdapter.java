package com.tsurugidb.tsubakuro.sql.impl;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.sql.proto.SqlCommon.Column;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.StatementMetadata;

/**
 * An implementation of {@link StatementMetadata} that wraps
 * result messages of {@link SqlClient#explain(String) EXPLAIN} operation.
 * @see SqlClient#explain(String)
 */
public class StatementMetadataAdapter implements StatementMetadata {

    private final SqlResponse.Explain.Success message;

    /**
     * Creates a new instance.
     * @param message the explain operation response
     */
    public StatementMetadataAdapter(@Nonnull SqlResponse.Explain.Success message) {
        Objects.requireNonNull(message);
        this.message = message;
    }

    @Override
    public String getFormatId() {
        return message.getFormatId();
    }

    @Override
    public long getFormatVersion() {
        return message.getFormatVersion();
    }

    @Override
    public String getContents() {
        return message.getContents();
    }

    @Override
    public List<? extends Column> getColumns() {
        return message.getColumnsList();
    }

    @Override
    public String toString() {
        return String.valueOf(message);
    }
}
