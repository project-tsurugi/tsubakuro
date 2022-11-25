package com.tsurugidb.tsubakuro.datastore;

/**
 * Represents backup operation type.
 */
public enum BackupType {

    /**
     * Takes copy of all standard resources on the database, including database images.
     */
    STANDARD,

    /**
     * Takes copy of transaction log, including log archives and large object entries.
     * <p>
     * This may be suitable for incremental backup operations.
     * </p>
     */
    TRANSACTION,
}
