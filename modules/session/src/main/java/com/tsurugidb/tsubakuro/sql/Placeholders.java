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
package com.tsurugidb.tsubakuro.sql;

import java.text.MessageFormat;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlRequest;

/**
 * Utilities to build {@link com.tsurugidb.sql.proto.SqlRequest.Placeholder Placeholder}.
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
    public static SqlRequest.Placeholder of(@Nonnull String name, @Nonnull Class<?> aClass) {
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
    public static SqlRequest.Placeholder of(@Nonnull String name, @Nonnull SqlCommon.AtomType type) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(type);
        return SqlRequest.Placeholder.newBuilder()
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
    public static SqlRequest.Placeholder of(@Nonnull String name, @Nonnull SqlCommon.TypeInfo type) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(type);
        var builder = SqlRequest.Placeholder.newBuilder()
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
