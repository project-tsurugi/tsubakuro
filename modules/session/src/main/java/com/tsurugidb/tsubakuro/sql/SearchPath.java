package com.tsurugidb.tsubakuro.sql;

import java.util.List;

/**
 * Represents search path.
 */
public interface SearchPath {

    /**
     * Returns a list of the schema name.
     * @return a list of the schema name
     */
    List<String> getSchemaNames();
}
