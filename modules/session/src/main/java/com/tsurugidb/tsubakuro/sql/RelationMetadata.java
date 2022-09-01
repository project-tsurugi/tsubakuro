package com.tsurugidb.tsubakuro.sql;

import java.util.List;

import com.tsurugidb.tateyama.proto.SqlCommon;

/**
 * Represents metadata of relations.
 */
public interface RelationMetadata {

    /**
     * Returns the column information of the relation.
     * @return the column descriptor list
     */
    List<? extends SqlCommon.Column> getColumns();
}
