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
package com.tsurugidb.tsubakuro.kvs;

import java.util.Objects;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.kvs.proto.KvsTransaction;

/**
 * Represents a transaction options.
 * @see #forShortTransaction()
 * @see #forLongTransaction()
 * @see #forReadOnlyTransaction()
 */
public final class TransactionOption {

    private final KvsTransaction.Option entity;

    /**
     * Creates a new builder for short transactions.
     * @return the created builder
     */
    public static Builder forShortTransaction() {
        return new Builder().configure(it -> it.setType(KvsTransaction.Type.SHORT));
    }

    /**
     * Creates a new builder for long transactions.
     * @return the created builder
     */
    public static LongBuilder forLongTransaction() {
        return new LongBuilder().configure(it -> it.setType(KvsTransaction.Type.LONG));
    }

    /**
     * Creates a new builder for long transactions.
     * @return the created builder
     */
    public static Builder forReadOnlyTransaction() {
        return new Builder().configure(it -> it.setType(KvsTransaction.Type.READ_ONLY));
    }

    /**
     * Creates a new default instance.
     */
    public TransactionOption() {
        this.entity = KvsTransaction.Option.getDefaultInstance();
    }

    /**
     * Creates a new instance.
     * @param entity the entity
     * @see #forShortTransaction()
     * @see #forLongTransaction()
     * @see #forReadOnlyTransaction()
     */
    public TransactionOption(@Nonnull KvsTransaction.Option entity) {
        Objects.requireNonNull(entity);
        this.entity = entity;
    }

    /**
     * Creates a copy of this option with another transaction label.
     * @param label the transaction label, or {@code null} to clear it
     * @return the created copy
     */
    public TransactionOption withLabel(@Nullable String label) {
        var builder = KvsTransaction.Option.newBuilder(entity);
        if (label == null) {
            builder.clearLabel();
        } else {
            builder.setLabel(label);
        }
        return new TransactionOption(builder.build());
    }

    /**
     * Returns the wrapped entity.
     * @return the wrapped entity
     */
    public KvsTransaction.Option getEntity() {
        return entity;
    }

    @Override
    public String toString() {
        return String.valueOf(entity);
    }

    /**
     * A builder for {@link TransactionOption}.
     */
    public static class Builder {

        private final KvsTransaction.Option.Builder entity = KvsTransaction.Option.newBuilder();

        /**
         * Sets an optional label to the transaction.
         * @param label the label, or {@code null} to clear it
         * @return this
         */
        public Builder withLabel(@Nullable String label) {
            if (label == null) {
                entity.clearLabel();
            } else {
                entity.setLabel(label);
            }
            return this;
        }

        /**
         * Sets the execution priority to the transaction.
         * @param priority the priority, or {@code null} to clear it
         * @return this
         */
        public Builder withPriority(@Nullable KvsTransaction.Priority priority) {
            if (priority == null) {
                entity.clearPriority();
            } else {
                entity.setPriority(priority);
            }
            return this;
        }

        /**
         * Configures this builder.
         * @param configurator the configurator
         * @return this
         */
        public Builder configure(@Nonnull Consumer<KvsTransaction.Option.Builder> configurator) {
            Objects.requireNonNull(configurator);
            configurator.accept(entity);
            return this;
        }

        /**
         * Builds a new {@link TransactionOption}.
         * @return the built object
         */
        public TransactionOption build() {
            return new TransactionOption(entity.build());
        }
    }

    /**
     * A builder for {@link TransactionOption} that configures long transactions.
     */
    public static class LongBuilder extends Builder {

        @Override
        public LongBuilder withLabel(@Nullable String label) {
            super.withLabel(label);
            return this;
        }

        @Override
        public LongBuilder withPriority(@Nullable KvsTransaction.Priority priority) {
            super.withPriority(priority);
            return this;
        }

        /**
         * Declares a table as write preservation target.
         * @param tableName the target table name
         * @return this
         */
        public LongBuilder addWritePreserve(@Nonnull String tableName) {
            return configure(it -> it.addWritePreserves(toTableArea(tableName)));
        }

        /**
         * Declares a table as inclusive read area.
         * @param tableName the target table name
         * @return this
         */
        public LongBuilder addInclusiveReadArea(@Nonnull String tableName) {
            return configure(it -> it.addInclusiveReadAreas(toTableArea(tableName)));
        }

        /**
         * Declares a table as inclusive read area.
         * @param tableName the target table name
         * @return this
         */
        public LongBuilder addExclusiveReadArea(@Nonnull String tableName) {
            return configure(it -> it.addExclusiveReadAreas(toTableArea(tableName)));
        }

        /**
         * Declares which the transaction will modify definitions.
         * @return this
         */
        public LongBuilder withModifyDefinitions() {
            return withModifyDefinitions(true);
        }

        /**
         * Declares whether or not the transaction will modify definitions.
         * @param enable {@code true} to modify definitions, otherwise {@code false}
         * @return this
         */
        public LongBuilder withModifyDefinitions(boolean enable) {
            return configure(it -> it.setModifiesDefinitions(enable));
        }

        @Override
        public LongBuilder configure(@Nonnull Consumer<KvsTransaction.Option.Builder> configurator) {
            super.configure(configurator);
            return this;
        }

        private static KvsTransaction.TableArea toTableArea(@Nonnull String tableName) {
            Objects.requireNonNull(tableName);
            return KvsTransaction.TableArea.newBuilder()
                    .setTableName(tableName)
                    .build();
        }
    }
}
