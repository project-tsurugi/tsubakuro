package com.nautilus_technologies.tsubakuro.examples.tpcc;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.ArrayList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import  com.nautilus_technologies.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import  com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.common.SessionBuilder;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;

public final class Main {
    private static String url = "ipc:tateyama";

    static long warehouses()  throws IOException, ServerException, InterruptedException, TimeoutException {
        try (
            Session session = SessionBuilder.connect(url)
            .withCredential(new UsernamePasswordCredential("user", "pass"))
            .create(10, TimeUnit.SECONDS);
            SqlClient sqlClient = SqlClient.attach(session);) {

                var transaction = sqlClient.createTransaction().get();
                var future = transaction.executeQuery("SELECT COUNT(w_id) FROM WAREHOUSE");
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
                return count;
        } catch (IOException | ServerException | InterruptedException | TimeoutException e) {
            System.out.println(e);
            throw e;
        }
    }

    private Main() {
    }

    static int threads = 8;
    static int duration = 30;

    public static void main(String[] args) {
        int argl = args.length;
        boolean fixThreadMapping = false;
        if (argl > 0) {
            threads = Integer.parseInt(args[0]);
            if (threads < 0) {
                threads = -threads;
                fixThreadMapping = true;
            }
            if (argl > 1) {
                duration = Integer.parseInt(args[1]);
            }
        }

        try {
            var warehouses = warehouses();
            if (fixThreadMapping) {
            if (threads != warehouses) {
                if (threads > warehouses) {
                System.out.printf("threads (%d) is greater than warehouses (%d)%n", threads, warehouses);
                return;
                }
                warehouses = (warehouses / threads) * threads;
                System.out.printf("changed warehouses to %d%n", warehouses);
            }
            System.out.println("fixThreadMapping is true");
            }
            DeferredHelper doingDelivery = new DeferredHelper((int) warehouses);
            ArrayList<Client> clients = new ArrayList<>();
            ArrayList<Profile> profiles = new ArrayList<>();
            CyclicBarrier barrier = new CyclicBarrier(threads + 1);
            AtomicBoolean stop = new AtomicBoolean();

            for (int i = 0; i < threads; i++) {
                var profile = new Profile();
                profile.warehouses = warehouses;
                profile.threads = threads;
                profile.index = i;
                profile.fixThreadMapping = fixThreadMapping;
                profiles.add(profile);
                clients.add(new Client(url, profile, barrier, stop, doingDelivery));
            }

            for (int i = 0; i < clients.size(); i++) {
                clients.get(i).start();
            }
            barrier.await();
            System.out.println("benchmark started, warehouse = " + warehouses + ", threads = " + threads);
            Thread.sleep(duration * 1000);
            stop.set(true);
            System.out.println("benchmark stoped");
            var total = new Profile();
            for (int i = 0; i < clients.size(); i++) {
                clients.get(i).join();
                total.add(profiles.get(i));
            }
            total.print(threads);
        } catch (IOException | ServerException | InterruptedException | BrokenBarrierException | TimeoutException e) {
            System.out.println(e);

        }
    }
}
