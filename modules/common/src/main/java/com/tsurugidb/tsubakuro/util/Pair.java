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
