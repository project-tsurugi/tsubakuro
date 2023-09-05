package com.tsurugidb.tsubakuro.kvs.ycsb;

import java.net.URI;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.tsurugidb.tsubakuro.kvs.util.RunManager;

/**
 * simple benchmark program like YCSB-A etc. see shirakami/bench/ycsb/ycsb.cpp
 */
public class YCSBlikeBenchmark {

    private final URI endpoint;

    private final String[] numClients;
    private final String[] rratios;
    private final long warmupMsec;
    private final long benchMsec;
    private final boolean createDB;
    private final boolean useKvsClient;

    YCSBlikeBenchmark(String[] args) {
        this.endpoint = URI.create(args[0]);
        this.numClients = args[1].split(",");
        this.createDB = args.length > 2 && args[2].equals("createDB");
        if (createDB) {
            this.rratios = new String[]{};
            this.warmupMsec = 0;
            this.benchMsec = 0;
        } else {
            this.rratios = args[2].split(",");
            this.warmupMsec = 1000 * Long.parseLong(args[3]);
            this.benchMsec = 1000 * Long.parseLong(args[4]);
        }
        this.useKvsClient = !System.getProperty("useSqlClient", "false").equals("true");
    }

    private void createTables() throws Exception {
        int numTable = Constants.USE_SAME_TABLE ? 1 : Integer.parseInt(numClients[0]);
        ExecutorService executor = Executors.newFixedThreadPool(numTable);
        LinkedList<Future<Void>> futures = new LinkedList<>();
        try {
            for (int i = 0; i < numTable; i++) {
                futures.add(executor.submit(new CreateTableWorker(endpoint, i)));
            }
            for (var future : futures) {
                future.get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }

    private void showCvsHeader() {
        System.out.println("# KEY_SIZE, " + Constants.KEY_SIZE);
        System.out.println("# VALUE_SIZE, " + Constants.VALUE_SIZE);
        System.out.println("# OPs/tx, " + Constants.OPS_PER_TX);
        System.out.println("# record/table, " + Constants.NUM_RECORDS);
        System.out.println("# clientType, " + (useKvsClient ? "KvsClient" : "SqlClient"));
        System.out.println("# useSameTable, " + Constants.USE_SAME_TABLE);
        System.out.println("# num_client, read_ratio, sec, num_tx, tx/sec, usec/tx");
    }

    private static void result(int numClient, int rratio, long elapseMsec, long numTx) {
        double sec = elapseMsec / 1000.0;
        System.out.printf("%d,%d,%.1f,%d,%.1f,%.2f", numClient, rratio, sec, numTx, numTx / sec, 1e+6 * sec / numTx);
        System.out.println();
    }

    private Worker newWorker(RunManager mgr, int numClient, int clientId, int rratio) throws Exception {
        return useKvsClient ? new KvsWorker(mgr, endpoint, numClient, clientId, rratio)
                : new SqlWorker(mgr, endpoint, numClient, clientId, rratio);
    }

    private void warmup() throws Exception {
        final int numClient = 1;
        final int rratio = Integer.parseInt(rratios[0]);
        var mgr = new RunManager(numClient);
        Worker worker = newWorker(mgr, numClient, 0, rratio);
        ExecutorService executor = Executors.newFixedThreadPool(numClient);
        try {
            var future = executor.submit(worker);
            mgr.setWorkerStartTime();
            long start = System.currentTimeMillis();
            Thread.sleep(warmupMsec);
            mgr.setQuit();
            long end = System.currentTimeMillis();
            var numTx = future.get();
            result(numClient, rratio, end - start, numTx);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }

    private void bench(int numClient, int rratio) {
        var clients = new LinkedList<Future<Long>>();
        ExecutorService executor = Executors.newFixedThreadPool(numClient);
        // System.err.println(numClient + " threads start");
        long sumTx = 0;
        var mgr = new RunManager(numClient);
        try {
            for (int i = 0; i < numClient; i++) {
                var worker = newWorker(mgr, numClient, i, rratio);
                clients.add(executor.submit(worker));
            }
            if (!createDB) {
                mgr.setWorkerStartTime();
                Thread.sleep(benchMsec);
                mgr.setQuit();
            }
            for (var future : clients) {
                sumTx += future.get();
            }
            result(numClient, rratio, benchMsec, sumTx);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }

    private void bench() {
        showCvsHeader();
        for (var n : numClients) {
            for (var r : rratios) {
                bench(Integer.parseInt(n), Integer.parseInt(r));
            }
        }
    }

    /**
     * @param args command arguments
     * @throws Exception some exceptional situation occurred
     */
    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].contains("help")) {
            System.err.println("Usage: java YCSBlikeBenchmark endpoint num_client createDB");
            System.err.println("\tex\tjava YCSBlikeBenchmark ipc:tsurugi 8 createDB");
            System.err.println("Usage: java YCSBlikeBenchmark endpoint num_client(s) rratio(s) warmupSec benchSec");
            System.err.println("\tex\\tjava YCSBlikeBenchmark ipc:tsurugi 1 50 10 30");
            System.err.println("\tex\\tjava YCSBlikeBenchmark ipc:tsurugi 1,2,4,8 50,95 10 30");
            return;
        }
        YCSBlikeBenchmark app = new YCSBlikeBenchmark(args);
        if (app.createDB) {
            app.createTables();
        } else {
            app.warmup();
            app.bench();
        }
    }

}
