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

import  com.nautilus_technologies.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import  com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.common.SessionBuilder;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;

public final class Main {
    private Main(String[] args) {
    }

    private static String url = "ipc:tateyama";
    private static boolean selectOnly = false;
    private static int selectCount = 1;

    public static void main(String[] args) {
        // コマンドラインオプションの設定
        Options options = new Options();

        options.addOption(Option.builder("s").argName("select").desc("Select only mode.").build());
        options.addOption(Option.builder("c").argName("count").hasArg().desc("Specify the execution count of the select operation.").build());
        options.addOption(Option.builder("t").argName("stream").desc("Connect via stream endpoint.").build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("s")) {
                selectOnly = true;
                System.err.println("select only");
            }
            if (cmd.hasOption("c")) {
                selectCount = Integer.parseInt(cmd.getOptionValue("c"));
                System.err.println("select count = " + selectCount);
            }
            if (cmd.hasOption("t")) {
                url = "tcp://localhost:12345/";
                System.err.println("connect via " + url);
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
            (new Select(sqlClient)).prepareAndSelect(selectCount);
        } catch (IOException | ServerException | InterruptedException | TimeoutException e) {
            System.out.println(e);
        }
    }
}
