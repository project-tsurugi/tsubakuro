package com.tsurugidb.tsubakuro.console.model;

/**
 * Represents a SQL statement.
 */
public interface Statement {

    /**
     * Returns the statement kind.
     * @return the statement kind
     */
    StatementKind getKind();

    /**
     * Returns the text of this statement.
     * @return the text
     */
    String getText();

    /**
     * Returns the region of this statement in the document.
     * @return the region
     */
    Region getRegion();
}
