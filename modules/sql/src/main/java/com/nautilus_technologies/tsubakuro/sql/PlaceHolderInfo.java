package com.nautilus_technologies.tsubakuro;

/**
 * PlaceHolderInfo type,
 */
public interface PlaceHolderInfo {
    /**
     * Set name and type of the place holder, called for each host variable
     @param name the name of the place holder
     @param type the type of the place holder
    */
    void setPlaceHolder(String name, FieldType type);
}
