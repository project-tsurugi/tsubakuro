package com.nautilus_technologies.tsubakuro.low.sql;

/**
 * Represents metadata of tables.
 */
public interface TableMetadata extends RelationMetadata {

    /**
     * Returns the database name where the table defined.
     * @return the database name
     */
    String getDatabaseName();

    /**
     * Returns the schema name where the table defined.
     * @return the schema name
     */
    String getSchemaName();

    /**
     * Returns simple name of the table.
     * @return the simple name
     */
    String getTableName();
}
