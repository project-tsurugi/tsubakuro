package com.nautilus_technologies.tsubakuro.low.sql;

import java.text.MessageFormat;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.jogasaki.proto.SqlCommon;
import com.tsurugidb.jogasaki.proto.SqlRequest;

/**
 * Utilities to build {@link com.tsurugidb.jogasaki.proto.SqlRequest.PlaceHolder Placeholder}.
 */
public final class Placeholders {

    /**
     * Returns a new place-holder definition.
     * @param name the place-holder name
     * @param aClass the place-holder type, resolved by {@link Types#typeOf(Class) Types.typeOf(aClass)}.
     * @return the created place-holder
     * @throws IllegalArgumentException if there is no corresponding type
     * @see Types#typeOf(Class)
     */
    public static SqlRequest.PlaceHolder of(@Nonnull String name, @Nonnull Class<?> aClass) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(aClass);
        return of(name, Types.typeOf(aClass));
    }

    /**
     * Returns a new place-holder definition.
     * @param name the place-holder name
     * @param type the place-holder type
     * @return the created place-holder
     */
    public static SqlRequest.PlaceHolder of(@Nonnull String name, @Nonnull SqlCommon.AtomType type) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(type);
        return SqlRequest.PlaceHolder.newBuilder()
                .setName(name)
                .setAtomType(type)
                .build();
    }

    /**
     * Returns a new place-holder definition.
     * @param name the place-holder name
     * @param type the place-holder type
     * @return the created place-holder
     */
    public static SqlRequest.PlaceHolder of(@Nonnull String name, @Nonnull SqlCommon.TypeInfo type) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(type);
        var builder = SqlRequest.PlaceHolder.newBuilder()
                .setName(name);
        switch (type.getTypeInfoCase()) {
        case ATOM_TYPE:
            builder.setAtomType(type.getAtomType());
            break;
        case ROW_TYPE:
            builder.setRowType(type.getRowType());
            break;
        case USER_TYPE:
            builder.setUserType(type.getUserType());
            break;
        case TYPEINFO_NOT_SET:
            throw new IllegalArgumentException("type is not set");
        default:
            throw new IllegalArgumentException(MessageFormat.format(
                    "unrecognized type: {0}",
                    type.getTypeInfoCase()));
        }
        builder.setDimension(type.getDimension());
        return builder.build();
    }

    private Placeholders() {
        throw new AssertionError();
    }
}
