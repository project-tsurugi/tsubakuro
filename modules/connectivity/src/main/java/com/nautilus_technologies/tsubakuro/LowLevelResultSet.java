package com.nautilus_technologies.tsubakuro;

/**
 * LowLevelResultSet
 */
public interface LowLevelResultSet {
    /**
     * Move the current pointer to the next record
     * @return true if the next record exists
     */
    boolean next();

    /**
     * Check whether the current column is null or not
     * @return true if the current column is null
     */
    boolean isNull();

    /**
     * Get the current column value and proceed the currnet column position
     * @return the value of the current column
     */
    int getInt4();
    long getInt8();
    float getFloat4();
    double getFloat8();
    String getCharacter();
}
