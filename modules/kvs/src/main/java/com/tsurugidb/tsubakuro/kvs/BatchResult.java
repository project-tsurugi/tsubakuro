package com.tsurugidb.tsubakuro.kvs;

import javax.annotation.Nonnull;

/**
 * Represents a result of batch operation.
 * @see BatchScript
 */
public interface BatchResult {

    /**
     * Returns an operation result of this batch.
     * @param <T> the operation result type
     * @param reference the reference of the operation result
     * @return the corresponding operation result
     * @throws IllegalArgumentException if the reference is not incompatible for this result
     */
    <T> T get(@Nonnull BatchScript.Ref<T> reference);

    /**
     * Returns a {@code GET} operation result of this batch.
     * @param position the result position (0-origin)
     * @return the operation result of this batch
     * @throws IndexOutOfBoundsException if the position is out of bounds
     * @throws IllegalStateException if the result on the position is not a {@link GetResult}
     * @see #get(com.tsurugidb.tsubakuro.kvs.BatchScript.Ref)
     */
    GetResult getGetResult(int position);

    /**
     * Returns a {@code PUT} operation result of this batch.
     * @param position the result position (0-origin)
     * @return the operation result of this batch
     * @throws IndexOutOfBoundsException if the position is out of bounds
     * @throws IllegalStateException if the result on the position is not a {@link PutResult}
     * @see #get(com.tsurugidb.tsubakuro.kvs.BatchScript.Ref)
     */
    PutResult getPutResult(int position);

    /**
     * Returns a {@code REMOVE} operation result of this batch.
     * @param position the result position (0-origin)
     * @return the operation result of this batch
     * @throws IndexOutOfBoundsException if the position is out of bounds
     * @throws IllegalStateException if the result on the position is not a {@link RemoveResult}
     * @see #get(com.tsurugidb.tsubakuro.kvs.BatchScript.Ref)
     */
    RemoveResult getRemoveResult(int position);
}
