package com.nautilus_technologies.tsubakuro;

/**
 * PrepareRequest type,
 *  consisting of LowLevelPreparedStatement and ParameterSet given by the application
 */
public interface PrepareRequest {
    /**
     * Set sql for prepare request
     @param sql the sql text
    */
    void setSql(String sql);

    /**
     * Set name and type of the place holder
     @param name the name of the place holder
     @param type the type of the place holder
    */
    void setPlaceHolder(String name, FieldType type);
}
