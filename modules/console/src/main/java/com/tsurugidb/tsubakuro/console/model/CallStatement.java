package com.tsurugidb.tsubakuro.console.model;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * A {@link Statement} that represents {@code CALL}.
 */
public class CallStatement implements Statement {

    private final String text;

    private final Region region;

    private final Regioned<String> procedureName;

    private final List<Regioned<Value>> procedureArguments;

    /**
     * Creates a new instance.
     * @param text the text of this statement
     * @param region the region of this statement in the document
     * @param procedureName the target procedure name
     * @param procedureArguments the target procedure arguments
     */
    public CallStatement(
            @Nonnull String text,
            @Nonnull Region region,
            @Nonnull Regioned<String> procedureName,
            @Nonnull List<? extends Regioned<Value>> procedureArguments) {
        Objects.requireNonNull(text);
        Objects.requireNonNull(region);
        this.text = text;
        this.region = region;
        this.procedureName = procedureName;
        this.procedureArguments = List.copyOf(procedureArguments);
    }

    @Override
    public Kind getKind() {
        return Kind.CALL;
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
     * Returns the target procedure name.
     * @return the procedure name
     */
    public Regioned<String> getProcedureName() {
        return procedureName;
    }

    /**
     * Returns the target procedure arguments.
     * @return the procedure arguments
     */
    public List<Regioned<Value>> getProcedureArguments() {
        return procedureArguments;
    }

    @Override
    public String toString() {
        return String.format(
                "Statement(kind=%s, text='%s', region=%s, procedureName=%s, procedureArguments=%s)", //$NON-NLS-1$
                getKind(),
                text,
                region,
                procedureName,
                procedureArguments);
    }
}
