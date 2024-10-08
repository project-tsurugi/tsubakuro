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

public final class Pair<L, R> {
    private L left;
    private R right;

    public static <L, R> Pair<L, R> of(L left, R right) {
        return new Pair<>(left, right);
    }

    private Pair(L left, R right) {
        this.left = Optional.of(left)
                .orElseThrow(() -> new NullPointerException());
        this.right = Optional.of(right)
                .orElseThrow(() -> new NullPointerException());
    }

    //    public class Pair<L, R> {
    //    private final L left;
    //    private final R right;

    //    public Pair(L left, R right) {
    //    this.left = left;
    //    this.right = right;
    //    }

    public L getLeft() {
    return left;
    }

    public R getRight() {
    return right;
    }
}
