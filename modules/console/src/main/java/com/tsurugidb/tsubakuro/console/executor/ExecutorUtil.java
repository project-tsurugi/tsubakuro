package com.tsurugidb.tsubakuro.console.executor;

import java.text.MessageFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.tsubakuro.console.model.CommitStatement;
import com.tsurugidb.tsubakuro.console.model.ErroneousStatement;
import com.tsurugidb.tsubakuro.console.model.ErroneousStatement.ErrorKind;
import com.tsurugidb.tsubakuro.console.model.Regioned;
import com.tsurugidb.tsubakuro.console.model.SpecialStatement;
import com.tsurugidb.tsubakuro.console.model.StartTransactionStatement;
import com.tsurugidb.tsubakuro.console.model.StartTransactionStatement.ReadWriteMode;
import com.tsurugidb.tsubakuro.console.model.StartTransactionStatement.TransactionMode;

/**
 * Utilities about Tsurugi SQL console executors.
 */
public final class ExecutorUtil {

    static final Logger LOG = LoggerFactory.getLogger(ExecutorUtil.class);

    private static final String COMMAND_EXIT = "exit"; //$NON-NLS-1$

    private static final String COMMAND_HALT = "halt"; //$NON-NLS-2$

    /**
     * Extracts transaction option from the {@link StartTransactionStatement}.
     * @param statement the extraction target statement
     * @return the extracted option
     */
    public static SqlRequest.TransactionOption toTransactionOption(@Nonnull StartTransactionStatement statement) {
        Objects.requireNonNull(statement);
        var options = SqlRequest.TransactionOption.newBuilder();
        computeTransactionType(statement).ifPresent(options::setType);
        computeTransactionPriority(statement).ifPresent(options::setPriority);
        statement.getLabel().ifPresent(it -> options.setLabel(it.getValue()));
        computeWritePreserve(statement).ifPresent(options::addAllWritePreserves);
        // FIXME: read area
        return options.build();
    }

    private static Optional<SqlRequest.TransactionType> computeTransactionType(StartTransactionStatement statement) {
        boolean ltx = unwrap(statement.getTransactionMode()) == TransactionMode.LONG
                || unwrap(statement.getReadWriteMode()) == ReadWriteMode.READ_ONLY
                || statement.getWritePreserve().isPresent()
                || statement.getReadAreaInclude().isPresent()
                || statement.getReadAreaExclude().isPresent();
        boolean ro = unwrap(statement.getReadWriteMode()) == ReadWriteMode.READ_ONLY_DEFERRABLE;
        if (ltx) {
            if (ro) {
                LOG.warn(MessageFormat.format(
                        "transaction type is conflicted between LTX and RO; executes as LTX (line={0}, column={1})",
                        statement.getRegion().getStartLine() + 1,
                        statement.getRegion().getStartColumn() + 1));
            }
            return Optional.of(SqlRequest.TransactionType.LONG);
        }
        if (ro) {
            return Optional.of(SqlRequest.TransactionType.READ_ONLY);
        }
        return Optional.empty();
    }

    private static Optional<SqlRequest.TransactionPriority> computeTransactionPriority(
            StartTransactionStatement statement) {
        if (statement.getExclusiveMode().isEmpty()) {
            return Optional.empty();
        }
        switch (statement.getExclusiveMode().get().getValue()) {
        case PRIOR_DEFERRABLE:
            return Optional.of(SqlRequest.TransactionPriority.WAIT);
        case PRIOR_IMMEDIATE:
            return Optional.of(SqlRequest.TransactionPriority.INTERRUPT);
        case EXCLUDING_DEFERRABLE:
            return Optional.of(SqlRequest.TransactionPriority.WAIT_EXCLUDE);
        case EXCLUDING_IMMEDIATE:
            return Optional.of(SqlRequest.TransactionPriority.INTERRUPT_EXCLUDE);
        }
        throw new AssertionError();
    }

    private static Optional<List<SqlRequest.WritePreserve>> computeWritePreserve(StartTransactionStatement statement) {
        if (statement.getWritePreserve().isEmpty()) {
            return Optional.empty();
        }
        var wps = statement.getWritePreserve().get().stream()
                .map(Regioned::getValue)
                .map(it -> SqlRequest.WritePreserve.newBuilder().setTableName(it).build())
                .collect(Collectors.toList());
        return Optional.of(wps);
    }

    /**
     * Extracts commit option from the {@link CommitStatement}.
     * @param statement the extraction target statement
     * @return the extracted option
     */
    public static Optional<SqlRequest.CommitStatus> toCommitStatus(@Nonnull CommitStatement statement) {
        Objects.requireNonNull(statement);
        if (statement.getCommitStatus().isEmpty()) {
            return Optional.empty();
        }
        switch (statement.getCommitStatus().get().getValue()) {
        case ACCEPTED:
            return Optional.of(SqlRequest.CommitStatus.ACCEPTED);
        case AVAILABLE:
            return Optional.of(SqlRequest.CommitStatus.AVAILABLE);
        case STORED:
            return Optional.of(SqlRequest.CommitStatus.STORED);
        case PROPAGATED:
            return Optional.of(SqlRequest.CommitStatus.PROPAGATED);
        }
        throw new AssertionError();
    }

    /**
     * Returns whether or not the statement represents {@code '\exit'} command.
     * @param statement the extraction target statement
     * @return {@code true} if the statement represents such the command, or {@code false} otherwise
     */
    public static boolean isExitCommand(@Nonnull SpecialStatement statement) {
        Objects.requireNonNull(statement);
        return isCommand(COMMAND_EXIT, statement);
    }


    /**
     * Returns whether or not the statement represents {@code '\halt'} command.
     * @param statement the extraction target statement
     * @return {@code true} if the statement represents such the command, or {@code false} otherwise
     */
    public static boolean isHaltCommand(@Nonnull SpecialStatement statement) {
        Objects.requireNonNull(statement);
        return isCommand(COMMAND_HALT, statement);
    }

    /**
     * Returns whether or not the statement represents the command.
     * @param name the command name
     * @param statement the extraction target statement
     * @return {@code true} if the statement represents such the command, or {@code false} otherwise
     */
    public static boolean isCommand(@Nonnull String name, @Nonnull SpecialStatement statement) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(statement);
        String extracted = statement.getCommandName().getValue();
        return name.equalsIgnoreCase(extracted);
    }

    /**
     * Returns an {@link ErroneousStatement} from the unknown command.
     * @param statement the unknown command
     * @return corresponding {@link ErroneousStatement}
     */
    public static ErroneousStatement toUnknownError(@Nonnull SpecialStatement statement) {
        Objects.requireNonNull(statement);
        return new ErroneousStatement(
                statement.getText(),
                statement.getRegion(),
                ErrorKind.UNKNOWN_SPECIAL_COMMAND,
                statement.getCommandName().getRegion(),
                MessageFormat.format(
                        "unknown command: \"{0}\"",
                        statement.getCommandName().getValue()));
    }

    private static <T> T unwrap(Optional<Regioned<T>> value) {
        return value.map(Regioned::getValue).orElse(null);
    }

    private ExecutorUtil() {
        throw new AssertionError();
    }
}
