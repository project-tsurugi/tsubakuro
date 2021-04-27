package com.nautilus.technologies.tsubakuro;

/**
 * ParameterSet type.
 */
public interface ParameterSet {
    /**
     * Set a value for the placeholder
     * @param name the name of the placeholder without colon
     * @param value the value assigned to the placeholder
     */
    public void setInt4(String name, int value);
    public void setInt8(String name, long value);
    public void setFloat4(String name, float value);
    public void setFloat8(String name, double value);
    public void setCharacter(String name, String value);
}
