package com.tsurugidb.tsubakuro.sql;

import java.util.List;

import com.tsurugidb.sql.proto.SqlCommon;

/**
 * Represents metadata of executable statements.
 */
public interface StatementMetadata extends RelationMetadata {

    /**
     * Returns the format ID of {@link #getContents() the content}.
     * @return the content format ID
     */
    String getFormatId();

    /**
     * Returns the {@link #getFormatId() format} version of {@link #getContents() the content}.
     * @return the content format version
     */
    long getFormatVersion();

    /**
     * Returns a textual representation of the statement description.
     * @return the statement description
     */
    String getContents();

    /**
     * Returns the result set columns information of the statement.
     * @return the result set columns information, or empty if it does not provided
     *      (the statement may not return result sets, or the feature is disabled)
     */
    @Override
    List<? extends SqlCommon.Column> getColumns();
}
