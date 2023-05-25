package com.tsurugidb.tsubakuro.kvs;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Represents a result of {@code GET} request.
 */
public interface GetResult {

    /**
     * Returns the number of records in the result.
     * @return the number of records
     */
    int size();

    /**
     * Returns whether or not the result is empty.
     * @return {@code true} if the {@code GET} operation returns no records,
     *      of {@code false} if it returns one or more records
     */
    default boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Returns whether or not the result contains just one record..
     * @return {@code true} if the {@code GET} operation returns just one record,
     *      of {@code false} if it returns zero or multiple records
     */
    default boolean isSingle() {
        return size() == 1;
    }

    /**
     * Returns a record in this result.
     * @throws IllegalStateException if there are two or more records in this result
     * @return a record in this result, or {@code empty} if there are no records in this result
     * @see #asList()
     */
    Optional<? extends Record> asOptional();

    /**
     * Returns a record in this result.
     * @return a record in this result
     * @throws NoSuchElementException if there are no records in this result
     * @throws IllegalStateException if there are two or more records in this result
     * @see #isSingle()
     * @see #asList()
     */
    Record asRecord();

    /**
     * Returns records in this result.
     * @return a record in this result, or {@code empty} if there are no records in this result
     */
    List<? extends Record> asList();
}
