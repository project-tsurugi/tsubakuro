package com.tsurugidb.tsubakuro.console;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.channel.common.connection.Credential;
import com.nautilus_technologies.tsubakuro.channel.common.connection.NullCredential;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.common.SessionBuilder;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.tsurugidb.tsubakuro.console.executor.BasicEngine;
import com.tsurugidb.tsubakuro.console.executor.BasicResultProcessor;
import com.tsurugidb.tsubakuro.console.executor.BasicSqlProcessor;
import com.tsurugidb.tsubakuro.console.executor.Engine;
import com.tsurugidb.tsubakuro.console.executor.IoSupplier;
import com.tsurugidb.tsubakuro.console.model.Statement;
import com.tsurugidb.tsubakuro.console.parser.SqlParser;

/**
 * Executes SQL scripts.
 */
public final class ScriptRunner {

    static final Logger LOG = LoggerFactory.getLogger(ScriptRunner.class);

    private static final Path PATH_STANDARD_INPUT = Path.of("-"); //$NON-NLS-1$

    private static final Charset DEFAULT_SCRIPT_ENCODING = StandardCharsets.UTF_8;

    /**
     * Executes a script file.
     * <ul>
     * <li> {@code args[0]} : path to the script file (UTF-8 encoded) </li>
     * <li> {@code args[1]} : connection URI </li>
     * </ul>
     * @param args the program arguments
     * @throws Exception if exception was occurred
     */
    public static void main(String... args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "usage: java {0} </path/to/script.sql> <connection-uri>",
                    ScriptRunner.class.getName()));
        }
        LOG.debug("script: {}", args[0]); //$NON-NLS-1$
        LOG.debug("endpoint: {}", args[1]); //$NON-NLS-1$
        Path scriptPath = Path.of(args[0]);
        URI endpoint = URI.create(args[1]);
        Credential credential = NullCredential.INSTANCE;
        boolean success = execute(toReaderSupplier(scriptPath), endpoint, credential);
        if (!success) {
            System.exit(1);
        }
    }

    /**
     * Executes the script using basic implementation.
     * @param script the script file
     * @param endpoint the connection target end-point URI
     * @param credential the connection credential information
     * @return {@code true} if successfully completed, {@code false} otherwise
     * @throws ServerException if server side error was occurred
     * @throws IOException if I/O error was occurred while establishing connection
     * @throws InterruptedException if interrupted while establishing connection
     */
    public static boolean execute(
            @Nonnull IoSupplier<? extends Reader> script,
            @Nonnull URI endpoint,
            @Nonnull Credential credential) throws ServerException, IOException, InterruptedException {
        Objects.requireNonNull(script);
        Objects.requireNonNull(endpoint);
        Objects.requireNonNull(credential);
        LOG.info(MessageFormat.format(
                "establishing connection: {0}",
                endpoint));
        try (
                var session = SessionBuilder.connect(endpoint)
                        .withCredential(credential)
                        .create();
                var sqlProcessor = new BasicSqlProcessor(SqlClient.attach(session));
                var resultProcessor = new BasicResultProcessor();
        ) {
            return execute(script, new BasicEngine(sqlProcessor, resultProcessor));
        }
    }

    /**
     * Executes the script.
     * @param script the script file
     * @param engine the statement executor
     * @return {@code true} if successfully completed, {@code false} otherwise
     * @throws ServerException if server side error was occurred
     * @throws IOException if I/O error was occurred while establishing connection
     * @throws InterruptedException if interrupted while establishing connection
     */
    public static boolean execute(
            @Nonnull IoSupplier<? extends Reader> script,
            @Nonnull Engine engine) throws ServerException, IOException, InterruptedException {
        Objects.requireNonNull(script);
        Objects.requireNonNull(engine);
        LOG.info("start processing script");
        try (var parser = new SqlParser(script.get())) {
            while (true) {
                Statement statement = parser.next();
                if (statement == null) {
                    break;
                }
                try {
                    boolean cont = engine.execute(statement);
                    if (!cont) {
                        LOG.info("shutdown was requested");
                        break;
                    }
                } catch (Exception e) {
                    LOG.error(MessageFormat.format(
                            "exception was occurred while processing statement: text=''{0}'', line={1}, column={2}",
                            statement.getText(),
                            statement.getRegion().getStartLine() + 1,
                            statement.getRegion().getStartColumn() + 1),
                            e);
                    return false;
                }
            }
        }
        LOG.info("script execution was successfully completed");
        return true;
    }

    private static IoSupplier<? extends Reader> toReaderSupplier(Path script) throws FileNotFoundException {
        if (script.equals(PATH_STANDARD_INPUT)) {
            LOG.debug("read SQL script from standard input"); //$NON-NLS-1$
            return () -> {
                var console = System.console();
                if (console != null) {
                    return console.reader();
                }
                return new InputStreamReader(System.in, Charset.defaultCharset());
            };
        }
        if (!Files.isRegularFile(script)) {
            throw new FileNotFoundException(script.toString());
        }
        LOG.debug("read SQL script from file: {}", script); //$NON-NLS-1$
        return () -> Files.newBufferedReader(script, DEFAULT_SCRIPT_ENCODING);
    }

    private ScriptRunner() {
        throw new AssertionError();
    }
}
