package com.tsurugidb.tsubakuro.console.model;

/**
 * Represents a SQL statement.
 */
public interface Statement {

    /**
     * A kind of {@link Statement}.
     */
    enum Kind {

        /**
         * empty statement.
         * @see SimpleStatement
         */
        EMPTY,

        /**
         * generic SQL statement.
         * @see SimpleStatement
         */
        GENERIC,

        /**
         * {@code START TRANSACTION} statement.
         * @see StartTransactionStatement
         */
        START_TRANSACTION,

        /**
         * {@code COMMIT} statement.
         * @see CommitStatement
         */
        COMMIT,

        /**
         * {@code ROLLBACK} statement.
         * @see SimpleStatement
         */
        ROLLBACK,

        /**
         * {@code CALL} statement.
         * @see CallStatement
         */
        CALL,

        /**
         * {@code SPECIAL} statement.
         * @see SpecialStatement
         */
        SPECIAL,

        /**
         * erroneous statement.
         * @see ErroneousStatement
         */
        ERRONEOUS,
    }

    /**
     * Returns the statement kind.
     * @return the statement kind
     */
    Kind getKind();

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
