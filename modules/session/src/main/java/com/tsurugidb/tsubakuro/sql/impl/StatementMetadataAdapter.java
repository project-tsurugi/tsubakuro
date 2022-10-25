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

    private final SqlResponse.DescribeStatement.Success proto;

    /**
     * Creates a new instance.
     * @param proto the corresponding protocol buffers message
     */
    public StatementMetadataAdapter(@Nonnull SqlResponse.DescribeStatement.Success proto) {
        Objects.requireNonNull(proto);
        this.proto = proto;
    }

    @Override
    public String getPlanText() {
        if (Objects.nonNull(proto)) {
            return proto.getPlan();
        }
        return null;
    }

    @Override
    public List<? extends Column> getColumns() {
        if (Objects.nonNull(proto)) {
            return proto.getColumnsList();
        }
        return null;
    }

    @Override
    public String toString() {
        if (Objects.nonNull(proto)) {
            return String.valueOf(proto);
        }
        return "Neither message nor proto has not been set.";
    }
}
