package com.tsurugidb.tsubakuro.sql;

import java.util.List;

/**
 * Represents table list.
 */
public interface TableList {

    /**
     * Returns a list of the available table names in the database, except system tables.
     * @return a list of the available table names
     */
    List<String> getTableNames();

    /**
     * Returns the schema name where the table defined.
     * @param searchPath the search path
     * @return a list of SimpleNames
     */
    List<String> getSimpleNames(SearchPath searchPath);
}
