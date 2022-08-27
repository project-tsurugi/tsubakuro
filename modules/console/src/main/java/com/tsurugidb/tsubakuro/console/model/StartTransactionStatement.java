package com.tsurugidb.tsubakuro.console.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A {@link Statement} that represents {@code START TRANSACTION}.
 */
public class StartTransactionStatement implements Statement {

    /**
     * The transaction mode.
     */
    public enum TransactionMode {

        /**
         * Long transaction.
         */
        LONG,
    }

    /**
     * The read-write mode.
     */
    public enum ReadWriteMode {

        /**
         * Read only.
         */
        READ_ONLY,

        /**
         * Read only and can read older snapshot.
         */
        READ_ONLY_DEFERRABLE,

        /**
         * Read write mode.
         */
        READ_WRITE,
    }

    /**
     * Exclusive mode.
     */
    public enum ExclusiveMode {

        /**
         * Locking out new transactions, then execute this after the running transaction were finished.
         */
        PRIOR_DEFERRABLE,

        /**
         * Remove the running transactions, then execute this.
         */
        PRIOR_IMMEDIATE,

        /**
         * Behaves like as {@link #PRIOR_DEFERRABLE}, and locking out new transactions until this transaction was finished.
         */
        EXCLUDING_DEFERRABLE,

        /**
         * Behaves like as {@link #PRIOR_IMMEDIATE}, and locking out new transactions until this transaction was finished.
         */
        EXCLUDING_IMMEDIATE,
    }

    private final String text;

    private final Region region;

    private final Regioned<TransactionMode> transactionMode;

    private final Regioned<ReadWriteMode> readWriteMode;

    private final Regioned<ExclusiveMode> exclusiveMode;

    private final List<Regioned<String>> writePreserve;

    private final List<Regioned<String>> readAreaInclude;

    private final List<Regioned<String>> readAreaExclude;

    private final Regioned<String> label;

    private final Map<Regioned<String>, Optional<Regioned<Value>>> properties;

    /**
     * Creates a new instance.
     * @param text the text of this statement
     * @param region the region of this statement in the document
     * @param transactionMode the transaction mode (nullable)
     * @param readWriteMode the read-write mode (nullable)
     * @param exclusiveMode the exclusive mode (nullable)
     * @param writePreserve the write preserve (nullable)
     * @param readAreaInclude the inclusive tables of read area (nullable)
     * @param readAreaExclude the exclusive tables of read area (nullable)
     * @param label the optional transaction label (nullable)
     * @param properties the extra transaction properties (nullable)
     */
    public StartTransactionStatement(
            @Nonnull String text,
            @Nonnull Region region,
            @Nullable Regioned<TransactionMode> transactionMode,
            @Nullable Regioned<ReadWriteMode> readWriteMode,
            @Nullable Regioned<ExclusiveMode> exclusiveMode,
            @Nullable List<Regioned<String>> writePreserve,
            @Nullable List<Regioned<String>> readAreaInclude,
            @Nullable List<Regioned<String>> readAreaExclude,
            @Nullable Regioned<String> label,
            @Nullable Map<Regioned<String>, Optional<Regioned<Value>>> properties) {
        Objects.requireNonNull(text);
        Objects.requireNonNull(region);
        this.text = text;
        this.region = region;
        this.transactionMode = transactionMode;
        this.readWriteMode = readWriteMode;
        this.exclusiveMode = exclusiveMode;
        this.writePreserve = copyOrNull(writePreserve);
        this.readAreaInclude = copyOrNull(readAreaInclude);
        this.readAreaExclude = copyOrNull(readAreaExclude);
        this.label = label;
        this.properties = copyOrNull(properties);
    }

    private static <T> List<T> copyOrNull(@Nullable List<? extends T> list) {
        if (list == null) {
            return null;
        }
        return List.copyOf(list);
    }

    private static <K, V> Map<K, V> copyOrNull(@Nullable Map<? extends K, ? extends V> map) {
        if (map == null) {
            return null;
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(map));
    }

    @Override
    public Kind getKind() {
        return Kind.START_TRANSACTION;
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
     * Returns the transaction mode.
     * @return the transaction mode, or empty if it was not set
     */
    public Optional<Regioned<TransactionMode>> getTransactionMode() {
        return Optional.ofNullable(transactionMode);
    }

    /**
     * Returns the read-write mode.
     * @return the read-write mode, or empty if it was not set
     */
    public Optional<Regioned<ReadWriteMode>> getReadWriteMode() {
        return Optional.ofNullable(readWriteMode);
    }

    /**
     * Returns the exclusive mode.
     * @return the exclusive mode, or empty if it was not set
     */
    public Optional<Regioned<ExclusiveMode>> getExclusiveMode() {
        return Optional.ofNullable(exclusiveMode);
    }

    /**
     * Returns the write preserve tables.
     * @return the write preserve tables, or empty if it was not set
     */
    public Optional<List<Regioned<String>>> getWritePreserve() {
        return Optional.ofNullable(writePreserve);
    }

    /**
     * Returns the inclusive tables of read area.
     * @return the inclusive tables of read area, or empty if it was not set
     */
    public Optional<List<Regioned<String>>> getReadAreaInclude() {
        return Optional.ofNullable(readAreaInclude);
    }

    /**
     * Returns the exclusive tables of read area.
     * @return the exclusive tables of read area, or empty if it was not set
     */
    public Optional<List<Regioned<String>>> getReadAreaExclude() {
        return Optional.ofNullable(readAreaExclude);
    }

    /**
     * Returns the transaction label.
     * @return the label, or empty if it was not set
     */
    public Optional<Regioned<String>> getLabel() {
        return Optional.ofNullable(label);
    }

    /**
     * Returns the extra transaction properties.
     * @return the key-value pairs of transaction properties.
     */
    public Optional<Map<Regioned<String>, Optional<Regioned<Value>>>> getProperties() {
        return Optional.ofNullable(properties);
    }

    @Override
    public String toString() {
        return String.format(
                "Statement(kind=%s, text='%s', region=%s, " //$NON-NLS-1$
                + "transactionMode=%s, readWriteMode=%s, exclusiveMode=%s, " //$NON-NLS-1$
                + "writePreserve=%s, readAreaInclude=%s, readAreaExclude=%s, " //$NON-NLS-1$
                + "label=%s, properties=%s)", //$NON-NLS-1$
                getKind(),
                text, region,
                transactionMode, readWriteMode, exclusiveMode,
                writePreserve, readAreaInclude, readAreaExclude,
                label, properties);
    }
}
