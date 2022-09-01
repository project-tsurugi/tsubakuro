package com.tsurugidb.tsubakuro.examples.measurement;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.exception.ServerException;

public final class Main {
    private static String url = "ipc:tateyama";

    static long warehouses()  throws IOException, ServerException, InterruptedException, TimeoutException {
        try (
                Session session = SessionBuilder.connect(url)
                .withCredential(new UsernamePasswordCredential("user", "pass"))
                .create(10, TimeUnit.SECONDS);
                SqlClient client = SqlClient.attach(session);) {

            try (Transaction transaction = client.createTransaction().await()) {
                var future = transaction.executeQuery("SELECT COUNT(w_id) FROM WAREHOUSE");
                var resultSet = future.get();
                long count = 0;
                if (resultSet.nextRow()) {
                    if (resultSet.nextColumn()) {
                        count = resultSet.fetchInt8Value();
                    }
                }
                resultSet.close();
                transaction.commit().get();
                return count;
            }
        }
    }

    private Main() {
    }

    enum Type {
    SELECT_ONE,
    SELECT_MULTI,
    SELECT_LIMIT_ONE,
    INSERT,
    UPDATE,
    };

    private static int pattern = 1;
    private static int duration = 30;
    private static Type type = Type.SELECT_ONE;

    public static void main(String[] args) {

        try (
            Session session = SessionBuilder.connect(url)
            .withCredential(new UsernamePasswordCredential("user", "pass"))
            .create(10, TimeUnit.SECONDS);
            SqlClient sqlClient = SqlClient.attach(session);) {

            // コマンドラインオプションの設定
            Options options = new Options();

            options.addOption(Option.builder("d").argName("duration").hasArg().desc("duration in seconds").build());
            options.addOption(Option.builder("t").argName("SQL type").hasArg().desc("type of SQL").build());

            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = null;

            cmd = parser.parse(options, args);

            if (cmd.hasOption("d")) {
                duration = Integer.parseInt(cmd.getOptionValue("d"));
            }
            if (cmd.hasOption("t")) {
                var givenType = cmd.getOptionValue("t");
                if (givenType.equals("select")) {
                    type = Type.SELECT_ONE;
                } else if (givenType.equals("selectm")) {
                    type = Type.SELECT_MULTI;
                } else if (givenType.equals("selectl")) {
                    type = Type.SELECT_LIMIT_ONE;
                } else if (givenType.equals("insert")) {
                    type = Type.INSERT;
                } else if (givenType.equals("update")) {
                    type = Type.UPDATE;
                } else {
                    throw new ParseException("illegal type");
                }
            }

            var warehouses = warehouses();
            CyclicBarrier barrier = new CyclicBarrier(2);
            AtomicBoolean stop = new AtomicBoolean();
            var profile = new Profile(warehouses);

            Thread client = null;
            switch (type) {
                case SELECT_ONE:
                    client = new SelectOne(sqlClient, profile, barrier, stop);
                    break;
                case SELECT_MULTI:
                    client = new SelectMulti(sqlClient, profile, barrier, stop);
                    break;
                case SELECT_LIMIT_ONE:
                    client = new SelectLimitOne(sqlClient, profile, barrier, stop);
                    break;
                case INSERT:
                    client = new Insert(sqlClient, profile, barrier, stop);
                    break;
                case UPDATE:
                    client = new Update(sqlClient, profile, barrier, stop);
                    break;
                default:
                    System.out.println("illegal type");
                    return;
            }
            if (!Objects.isNull(client)) {
                client.start();
                barrier.await();
                System.out.println("benchmark started, warehouse = " + warehouses);
                Thread.sleep(duration * 1000);
                stop.set(true);
                client.join();
                System.out.println("benchmark stoped");
                profile.print();
            }

         } catch (IOException | ServerException | InterruptedException | BrokenBarrierException | TimeoutException e) {
            System.out.println(e);
        } catch (ParseException e) {
            System.err.printf("cmd parser failed." + e);
        }
    }
}
