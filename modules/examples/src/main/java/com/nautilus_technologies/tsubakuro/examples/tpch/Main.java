package com.nautilus_technologies.tsubakuro.examples.tpch;

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
import com.tsurugidb.jogasaki.proto.SqlRequest;

public final class Main {
    private static String url = "ipc:tateyama";

    static long scale()  throws IOException, ServerException, InterruptedException, TimeoutException {
        try (
            Session session = SessionBuilder.connect(url)
            .withCredential(new UsernamePasswordCredential("user", "pass"))
            .create(10, TimeUnit.SECONDS);
            SqlClient client = SqlClient.attach(session);) {

            var transaction = client.createTransaction().get();
            var future = transaction.executeQuery("SELECT COUNT(S_SUPPKEY) FROM SUPPLIER");
            var resultSet = future.get();
            long count = 0;
            if (resultSet.nextRow()) {
                if (resultSet.nextColumn()) {
                count = resultSet.fetchInt8Value();
                }
            }
            resultSet.close();
            resultSet.getResponse().get();
            transaction.commit().get();
            session.close();
            return count / 10000;
        }
    }

    private Main() {
    }

    public static void main(String[] args) {
    var profile = new Profile();

        // コマンドラインオプションの設定
        Options options = new Options();

        options.addOption(Option.builder("l").argName("loop").hasArg().desc("Number of loops.").build());
        options.addOption(Option.builder("q").argName("query_number").hasArg().desc("Query number to be executed.").build());
        options.addOption(Option.builder("e").argName("query_string").hasArg().desc("Execute this query.").build());
        options.addOption(Option.builder("f").argName("file_contains_query").hasArg().desc("Execute the query descrived in the file specified.").build());
        options.addOption(Option.builder("v").argName("query_validation").desc("Set query validation mode.").build());
        options.addOption(Option.builder("o").argName("reqd_only_transaction").desc("Use read only transaction.").build());
        options.addOption(Option.builder("b").argName("batch(long)_transaction").desc("Use long transaction.").build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("o")) {
                profile.transactionOption.setType(SqlRequest.TransactionType.READ_ONLY);
                System.out.println("use read only transaction");
            }
            if (cmd.hasOption("b")) {
                profile.transactionOption.setType(SqlRequest.TransactionType.LONG);
                System.out.println("use long transaction type");
            }
            if (cmd.hasOption("v")) {
                profile.queryValidation = true;
            }

            profile.scales = scale();
            System.out.println("benchmark started, scale = " + profile.scales);

            try (
                Session session = SessionBuilder.connect(url)
                .withCredential(new UsernamePasswordCredential("user", "pass"))
                .create(10, TimeUnit.SECONDS);
                SqlClient client = SqlClient.attach(session);) {

                if (cmd.hasOption("q")) {
                    var queryNum = cmd.getOptionValue("q");
                    if (queryNum.equals("2_1")) {
                        var query = new Q2(client);
                        query.run21(profile);
                        System.out.println("elapsed: " + profile.q21 + " mS");
                    } else if (queryNum.equals("2")) {
                        var query = new Q2(client);
                        query.run2(profile);
                        System.out.println("elapsed: " + profile.q22 + " mS");
                    } else if (queryNum.equals("6")) {
                        var query = new Q6(client);
                        query.run(profile);
                        System.out.println("elapsed: " + profile.q6 + " mS");
                    } else if (queryNum.equals("14")) {
                        var query = new Q14(client);
                        query.run(profile);
                        System.out.println("elapsed: " + profile.q14 + " mS");
                    } else if (queryNum.equals("19")) {
                        var query = new Q19(client);
                        query.run(profile);
                        System.out.println("elapsed: " + profile.q19 + " mS");
                    } else {
                        System.out.println("no such query " + cmd.getOptionValue("q"));
                    }
                }
            }
        } catch (IOException | ServerException | InterruptedException | TimeoutException e) {
            System.out.println(e);
            e.printStackTrace();
        } catch (ParseException e) {
            System.err.printf("cmd parser failed." + e);
        }
    }
}
