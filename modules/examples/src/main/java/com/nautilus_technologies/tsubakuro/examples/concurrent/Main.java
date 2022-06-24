package com.nautilus_technologies.tsubakuro.examples.concurrent;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.Objects;

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
import com.tsurugidb.jogasaki.proto.SqlResponse;

public final class Main {
    private static String url = "ipc:tateyama";
    private static int concurrency = 1;

    private Main() {
    }

    static long orderId() throws IOException, ServerException, InterruptedException, TimeoutException {
        try (
            Session session = SessionBuilder.connect(url)
            .withCredential(new UsernamePasswordCredential("user", "pass"))
            .create(10, TimeUnit.SECONDS);
            SqlClient sqlClient = SqlClient.attach(session);) {

                var transaction = sqlClient.createTransaction().get();
                var future = transaction.executeQuery("SELECT no_o_id FROM NEW_ORDER WHERE no_w_id = 1 AND no_d_id = 1 ORDER by no_o_id DESC");
                var resultSet = future.get();
                long count = 0;
                if (resultSet.nextRecord()) {
                    if (resultSet.nextColumn()) {
                        count = resultSet.getInt8();
                    }
                }
                resultSet.close();
                var r = resultSet.getResponse().get();
                if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(r.getResultCase())) {
                    throw new IOException("select error");
                }
                var commitResponse = transaction.commit().get();
                if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(commitResponse.getResultCase())) {
                    throw new IOException("commit (select) error");
                }
                session.close();
                return count;

        } catch (IOException | ServerException | InterruptedException | TimeoutException e) {
            System.out.println(e);
            throw e;
        }
    }

    public static void main(String[] args) {
        // コマンドラインオプションの設定
        Options options = new Options();

        options.addOption(Option.builder("c").argName("concurrency").hasArg().desc("concurrnency level").build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("c")) {
                concurrency = Integer.parseInt(cmd.getOptionValue("c"));
            }

            try (
                Session session = SessionBuilder.connect(url)
                .withCredential(new UsernamePasswordCredential("user", "pass"))
                .create(10, TimeUnit.SECONDS);
                SqlClient sqlClient = SqlClient.attach(session);) {

                    var client = new Insert(sqlClient, concurrency, orderId());
                    if (!Objects.isNull(client)) {
                        client.start();
                        client.join();
                    }
            }

        } catch (IOException | ServerException | InterruptedException | TimeoutException e) {
            System.out.println(e);
        } catch (ParseException e) {
            System.err.printf("cmd parser failed." + e);
        }
    }
}
