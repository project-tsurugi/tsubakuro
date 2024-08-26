package com.tsurugidb.tsubakuro.sql;

import java.util.Optional;

/**
 * Represents metadata of tables.
 */
public interface TableMetadata extends RelationMetadata {

    /**
     * <em>This method is not yet implemented:</em>
     * Returns the database name where the table defined.
     * @return the database name, or empty if it is not set
     */
    Optional<String> getDatabaseName();

    /**
     * <em>This method is not yet implemented:</em>
     * Returns the schema name where the table defined.
     * @return the schema name, or empty if it is not set
     */
    Optional<String> getSchemaName();

    /**
     * Returns simple name of the table.
     * @return the simple name
     */
    String getTableName();
}
