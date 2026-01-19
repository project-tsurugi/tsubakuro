/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.tsubakuro.util;

import java.util.Optional;

/**
 * A pair of two values.
 *
 * @param <L> the type of the left value
 * @param <R> the type of the right value
 */
public final class Pair<L, R> {
    private L left;
    private R right;

    /**
     * Creates a new Pair instance.
     * @param <L> the type of the left value
     * @param <R> the type of the right value
     * @param left the left value
     * @param right the right value
     * @return a new Pair containing the left and right values
     */
    public static <L, R> Pair<L, R> of(L left, R right) {
        return new Pair<>(left, right);
    }

    private Pair(L left, R right) {
        this.left = Optional.of(left)
                .orElseThrow(() -> new NullPointerException());
        this.right = Optional.of(right)
                .orElseThrow(() -> new NullPointerException());
    }

    /**
     * Gets the left value.
     * @return the left value
     */
    public L getLeft() {
    return left;
    }

    /**
     * Gets the right value.
     * @return the right value
     */
    public R getRight() {
    return right;
    }
}
