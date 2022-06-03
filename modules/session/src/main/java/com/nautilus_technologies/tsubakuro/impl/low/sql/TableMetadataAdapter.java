package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.jogasaki.proto.SqlCommon;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.nautilus_technologies.tsubakuro.low.sql.TableMetadata;

/**
 * An implementation of {@link TableMetadata} which just wraps original protocol buffers' message.
 */
public class TableMetadataAdapter implements TableMetadata {

    private final SqlResponse.DescribeTable.Success proto;

    /**
     * Creates a new instance.
     * @param proto the corresponding protocol buffers message
     */
    public TableMetadataAdapter(@Nonnull SqlResponse.DescribeTable.Success proto) {
        Objects.requireNonNull(proto);
        this.proto = proto;
    }

    @Override
    public String getDatabaseName() {
        return proto.getDatabaseName();
    }

    @Override
    public String getSchemaName() {
        return proto.getSchemaName();
    }

    @Override
    public String getTableName() {
        return proto.getTableName();
    }

    @Override
    public List<? extends SqlCommon.Column> getColumns() {
        return proto.getColumnsList();
    }

    @Override
    public String toString() {
        return proto.toString();
    }
}
