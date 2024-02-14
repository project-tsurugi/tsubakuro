package com.tsurugidb.tsubakuro.util;

import java.util.List;
import java.util.Optional;

/**
 * Represents a snippet of documentation, reflects to {@link Doc}.
 * @see Doc
 */
public interface DocumentSnippet {

    /**
     * Returns the description of the element.
     * @return the lines of descriptions.
     */
    List<String> getDescription();

    /**
     * Returns the additional note for the element.
     * @return a list of note text
     */
    default List<String> getNotes() {
        return List.of();
    }

    /**
     * Returns the optional references for the target element.
     * @return a list of references
     */
    default List<Reference> getReferences() {
        return List.of();
    }

    /**
     * Represents a reference of the {@link DocumentedElement}.
     */
    interface Reference {

        /**
         * Returns optional title of this reference.
         * @return the title of the reference, or {@code empty} if it is not sure
         */
        Optional<String> getTitle();

        /**
         * Returns the location of this reference.
         * @return the location string, generally a URL
         */
        String getLocation();
    }
}
