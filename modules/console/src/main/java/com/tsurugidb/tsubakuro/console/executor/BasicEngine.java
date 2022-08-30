package com.tsurugidb.tsubakuro.console.executor;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.console.model.CallStatement;
import com.tsurugidb.tsubakuro.console.model.CommitStatement;
import com.tsurugidb.tsubakuro.console.model.ErroneousStatement;
import com.tsurugidb.tsubakuro.console.model.SpecialStatement;
import com.tsurugidb.tsubakuro.console.model.StartTransactionStatement;
import com.tsurugidb.tsubakuro.console.model.Statement;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A basic implementation of {@link Engine}.
 * Clients must start/finish transactions manually.
 */
public class BasicEngine extends AbstractEngine {

    static final Logger LOG = LoggerFactory.getLogger(BasicEngine.class);

    private final SqlProcessor sqlProcessor;

    private final ResultProcessor resultSetProcessor;

    /**
     * Creates a new instance.
     * @param sqlProcessor the SQL processor
     * @param resultSetProcessor the result set processor
     */
    public BasicEngine(@Nonnull SqlProcessor sqlProcessor, @Nonnull ResultProcessor resultSetProcessor) {
        Objects.requireNonNull(sqlProcessor);
        Objects.requireNonNull(resultSetProcessor);
        this.sqlProcessor = sqlProcessor;
        this.resultSetProcessor = resultSetProcessor;
    }

    @Override
    protected boolean executeEmptyStatement(@Nonnull Statement statement)
            throws EngineException, ServerException, IOException, InterruptedException {
        Objects.requireNonNull(statement);
        LOG.debug("execute: kind={}, text={}", statement.getKind(), statement.getText()); //$NON-NLS-1$
        return true;
    }

    @SuppressFBWarnings(
            value = "RCN",
            justification = "misdetection: SqlProcessor.execute() may return null")
    @Override
    protected boolean executeGenericStatement(@Nonnull Statement statement)
            throws EngineException, ServerException, IOException, InterruptedException {
        Objects.requireNonNull(statement);
        LOG.debug("execute: kind={}, text={}", statement.getKind(), statement.getText()); //$NON-NLS-1$

        checkTransactionActive(statement);
        try (var rs = sqlProcessor.execute(statement.getText(), statement.getRegion())) {
            if (rs != null) {
                resultSetProcessor.process(rs);
            }
        }
        return true;
    }

    @Override
    protected boolean executeStartTransactionStatement(@Nonnull StartTransactionStatement statement)
            throws EngineException, ServerException, IOException, InterruptedException {
        Objects.requireNonNull(statement);
        LOG.debug("execute: kind={}, text={}", statement.getKind(), statement.getText()); //$NON-NLS-1$

        checkTransactionInactive(statement);
        var option = ExecutorUtil.toTransactionOption(statement);
        sqlProcessor.startTransaction(option);
        return true;
    }

    @Override
    protected boolean executeCommitStatement(@Nonnull CommitStatement statement)
            throws EngineException, ServerException, IOException, InterruptedException {
        Objects.requireNonNull(statement);
        LOG.debug("execute: kind={}, text={}", statement.getKind(), statement.getText()); //$NON-NLS-1$

        checkTransactionActive(statement);
        var status = ExecutorUtil.toCommitStatus(statement);
        sqlProcessor.commitTransaction(status.orElse(null));
        return true;
    }

    @Override
    protected boolean executeRollbackStatement(@Nonnull Statement statement)
            throws EngineException, ServerException, IOException, InterruptedException {
        Objects.requireNonNull(statement);
        LOG.debug("execute: kind={}, text={}", statement.getKind(), statement.getText()); //$NON-NLS-1$

        checkTransactionActive(statement);
        sqlProcessor.rollbackTransaction();
        return true;
    }

    @Override
    protected boolean executeCallStatement(@Nonnull CallStatement statement)
            throws EngineException, ServerException, IOException, InterruptedException {
        // fall-back
        return executeGenericStatement(statement);
    }

    @Override
    protected boolean executeSpecialStatement(@Nonnull SpecialStatement statement)
            throws EngineException, ServerException, IOException, InterruptedException {
        Objects.requireNonNull(statement);
        LOG.debug("execute: kind={}, text={}", statement.getKind(), statement.getText()); //$NON-NLS-1$

        if (ExecutorUtil.isExitCommand(statement)) {
            LOG.debug("starting shut-down"); //$NON-NLS-1$
            checkTransactionInactive(statement);
            return false;
        }
        if (ExecutorUtil.isHaltCommand(statement)) {
            LOG.debug("starting force shut-down"); //$NON-NLS-1$
            return false;
        }
        if (ExecutorUtil.isStatusCommand(statement)) {
            LOG.debug("show status"); //$NON-NLS-1$
            if (sqlProcessor.isTransactionActive()) {
                System.out.println("transaction is active");
            } else {
                System.out.println("transaction is inactive");
            }
            return true;
        }
        if (ExecutorUtil.isHelpCommand(statement)) {
            LOG.debug("show help"); //$NON-NLS-1$
            for (var s: ExecutorUtil.getHelpMessage()) {
                System.out.println(s);
            }
            return true;
        }
        // execute as erroneous
        LOG.debug("command is unrecognized: {}", statement.getCommandName()); //$NON-NLS-1$
        return execute(ExecutorUtil.toUnknownError(statement));
    }

    @Override
    protected boolean executeErroneousStatement(@Nonnull ErroneousStatement statement)
            throws EngineException, ServerException, IOException, InterruptedException {
        Objects.requireNonNull(statement);
        LOG.debug("execute: kind={}, text={}", statement.getKind(), statement.getText()); //$NON-NLS-1$

        throw new EngineException(MessageFormat.format(
                "[{0}] {1} (line={2}, column={3})",
                statement.getErrorKind(),
                statement.getMessage(),
                statement.getOccurrence().getStartLine() + 1,
                statement.getOccurrence().getStartColumn() + 1));
    }

    private void checkTransactionActive(Statement statement) throws EngineException {
        if (!sqlProcessor.isTransactionActive()) {
            throw new EngineException(MessageFormat.format(
                    "transaction is not started (line={0}, column={1})",
                    statement.getRegion().getStartLine() + 1,
                    statement.getRegion().getStartColumn() + 1));
        }
    }

    private void checkTransactionInactive(Statement statement) throws EngineException {
        if (sqlProcessor.isTransactionActive()) {
            throw new EngineException(MessageFormat.format(
                    "transaction is running (line={0}, column={1})",
                    statement.getRegion().getStartLine() + 1,
                    statement.getRegion().getStartColumn() + 1));
        }
    }
}
