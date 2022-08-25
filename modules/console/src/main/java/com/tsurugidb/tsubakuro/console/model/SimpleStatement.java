package com.tsurugidb.tsubakuro.console.model;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * A {@link Statement} which does not have any extra information.
 */
public class SimpleStatement implements Statement {

    private final StatementKind kind;

    private final String text;

    private final Region region;

    /**
     * Creates a new instance.
     * @param kind the statement kind
     * @param text the text of this statement
     * @param region the region of this statement in the document
     */
    public SimpleStatement(
            @Nonnull StatementKind kind,
            @Nonnull String text,
            @Nonnull Region region) {
        Objects.requireNonNull(kind);
        Objects.requireNonNull(text);
        Objects.requireNonNull(region);
        this.kind = kind;
        this.text = text;
        this.region = region;
    }

    @Override
    public StatementKind getKind() {
        return kind;
    }

    @Override
    public String getText() {
        return text;
    }

    @Override
    public Region getRegion() {
        return region;
    }

    @Override
    public String toString() {
        return String.format(
                "Statement(kind=%s, text='%s', region=%s)", //$NON-NLS-1$
                kind,
                text,
                region);
    }
}
