package com.tsurugidb.tsubakuro.console.model;

/**
 * A kind of {@link Statement}.
 */
public enum StatementKind {

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
