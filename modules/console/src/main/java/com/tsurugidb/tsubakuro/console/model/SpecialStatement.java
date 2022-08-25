package com.tsurugidb.tsubakuro.console.model;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * A {@link Statement} that consists of special commands.
 */
public class SpecialStatement implements Statement {

    private final String text;

    private final Region region;

    private final Regioned<String> commandName;

    /**
     * Creates a new instance.
     * @param text the text of this statement
     * @param region the region of this statement in the document
     * @param commandName the command name, must does not start back-slash
     */
    public SpecialStatement(
            @Nonnull String text,
            @Nonnull Region region,
            @Nonnull Regioned<String> commandName) {
        Objects.requireNonNull(text);
        Objects.requireNonNull(region);
        Objects.requireNonNull(commandName);
        this.text = text;
        this.region = region;
        this.commandName = commandName;
    }

    @Override
    public StatementKind getKind() {
        return StatementKind.SPECIAL;
    }

    @Override
    public String getText() {
        return text;
    }

    @Override
    public Region getRegion() {
        return region;
    }

    /**
     * Returns the command name.
     * @return the command name
     */
    public Regioned<String> getCommandName() {
        return commandName;
    }

    @Override
    public String toString() {
        return String.format(
                "Statement(kind=%s, text='%s', region=%s, commandName=%s)", //$NON-NLS-1$
                getKind(),
                text,
                region,
                commandName);
    }
}
