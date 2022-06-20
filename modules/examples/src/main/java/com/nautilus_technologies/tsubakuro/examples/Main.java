package com.nautilus_technologies.tsubakuro.examples;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.nautilus_technologies.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.common.SessionBuilder;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;

public final class Main {
    private Main(String[] args) {
    }

    private static String url = "ipc:tateyama";
    private static boolean selectOnly = false;

    public static void main(String[] args) {
        // コマンドラインオプションの設定
        Options options = new Options();

        options.addOption(Option.builder("s").argName("select").desc("Select only mode.").build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("s")) {
                System.err.println("select only");
                selectOnly = true;
            }
        } catch (ParseException e) {
            System.err.println("cmd parser failed." + e);
        }

        try (
            Session session = SessionBuilder.connect(url)
            .withCredential(new UsernamePasswordCredential("user", "pass"))
            .create(10, TimeUnit.SECONDS);
            SqlClient sqlClient = SqlClient.attach(session);) {

            if (!selectOnly) {
                (new Insert(sqlClient)).prepareAndInsert();
            }
            (new Select(sqlClient)).prepareAndSelect();
        } catch (IOException | ServerException | InterruptedException | TimeoutException e) {
            System.out.println(e);
        }
    }
}
