package com.nautilus_technologies.tsubakuro;

/**
 * ExecutablePreparedStatementRequest type,
 *  consisting of LowLevelPreparedStatement and ParameterSet given by the application
 */
public interface ExecutablePreparedStatementRequest {
    /**
     * Set a value for the placeholder
     * @param name the name of the placeholder without colon
     * @param value the value assigned to the placeholder
     */
    void setInt4(String name, int value);
    void setInt8(String name, long value);
    void setFloat4(String name, float value);
    void setFloat8(String name, double value);
    void setCharacter(String name, String value);
}
