package com.tsurugidb.tsubakuro.console;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.Arrays;
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
import com.tsurugidb.tsubakuro.console.model.Statement;
import com.tsurugidb.tsubakuro.console.parser.SqlParser;

/**
 * Executes SQL scripts.
 */
public final class ScriptRunner {

    static final Logger LOG = LoggerFactory.getLogger(ScriptRunner.class);

    private static final Charset DEFAULT_SCRIPT_ENCODING = StandardCharsets.UTF_8;

    /**
     * Executes a script file.
     * <ul>
     * <li> {@code args[0]} : path to the script file (UTF-8 encoded) </li>
     * <li> {@code args[1]} : connection URI </li>
     * </ul>
     * This may invoke {@link System#exit(int)}, or use {@link #execute(String...)} to avoid it.
     * @param args the program arguments
     * @throws Exception if exception was occurred
     */
    public static void main(String... args) throws Exception {
        if (!execute(args)) {
            System.exit(1);
        }
    }

    /**
     * Executes a script file.
     * <ul>
     * <li> {@code args[0]} : path to the script file (UTF-8 encoded) </li>
     * <li> {@code args[1]} : connection URI </li>
     * </ul>
     * @param args the program arguments
     * @return {@code true} if successfully completed, {@code false} otherwise
     * @throws ServerException if server side error was occurred
     * @throws IOException if I/O error was occurred while executing the script
     * @throws InterruptedException if interrupted while executing the script
     */
    public static boolean execute(String... args) throws ServerException, IOException, InterruptedException {
        if (args.length != 2) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "usage: java {0} </path/to/script.sql> <connection-uri>  ## {1}",
                    ScriptRunner.class.getName(),
                    Arrays.asList(args)));
        }
        LOG.debug("script: {}", args[0]); //$NON-NLS-1$
        LOG.debug("endpoint: {}", args[1]); //$NON-NLS-1$
        Path script = Path.of(args[0]);
        Charset scriptEncoding = DEFAULT_SCRIPT_ENCODING;
        URI endpoint = URI.create(args[1]);
        Credential credential = NullCredential.INSTANCE;
        return execute(script, scriptEncoding, endpoint, credential);
    }

    /**
     * Executes the script using basic implementation.
     * @param script the script file
     * @param scriptEncoding the script file charset
     * @param endpoint the connection target end-point URI
     * @param credential the connection credential information
     * @return {@code true} if successfully completed, {@code false} otherwise
     * @throws ServerException if server side error was occurred
     * @throws IOException if I/O error was occurred while executing the script
     * @throws InterruptedException if interrupted while executing the script
     */
    public static boolean execute(
            @Nonnull Path script,
            @Nonnull Charset scriptEncoding,
            @Nonnull URI endpoint,
            @Nonnull Credential credential) throws ServerException, IOException, InterruptedException {
        Objects.requireNonNull(script);
        Objects.requireNonNull(scriptEncoding);
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
            return execute(script, scriptEncoding, new BasicEngine(sqlProcessor, resultProcessor));
        }
    }

    /**
     * Executes the script.
     * @param script the script file
     * @param scriptEncoding the script file charset
     * @param engine the statement executor
     * @return {@code true} if successfully completed, {@code false} otherwise
     * @throws ServerException if server side error was occurred
     * @throws IOException if I/O error was occurred while executing the script
     * @throws InterruptedException if interrupted while executing the script
     */
    public static boolean execute(
            @Nonnull Path script,
            @Nonnull Charset scriptEncoding,
            @Nonnull Engine engine) throws ServerException, IOException, InterruptedException {
        Objects.requireNonNull(script);
        Objects.requireNonNull(scriptEncoding);
        Objects.requireNonNull(engine);
        LOG.info(MessageFormat.format(
                "start processing script: {0}",
                script));
        try (var parser = new SqlParser(Files.newBufferedReader(script, scriptEncoding))) {
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

    private ScriptRunner() {
        throw new AssertionError();
    }
}
