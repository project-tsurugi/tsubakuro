package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.tateyama.proto.SqlCommon;
import com.tsurugidb.tateyama.proto.SqlResponse;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSetMetadata;

/**
 * An implementation of {@link ResultSetMetadata} which just wraps original protocol buffers' message.
 */
public class ResultSetMetadataAdapter implements ResultSetMetadata {

    private final SqlResponse.ResultSetMetadata proto;

    /**
     * Creates a new instance.
     * @param proto the corresponding protocol buffers message
     */
    public ResultSetMetadataAdapter(@Nonnull SqlResponse.ResultSetMetadata proto) {
        Objects.requireNonNull(proto);
        this.proto = proto;
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
