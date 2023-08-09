package com.tsurugidb.tsubakuro.kvs.bench;

import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.tsurugidb.tsubakuro.kvs.impl.KvsClientImpl;
import com.tsurugidb.tsubakuro.kvs.util.RunManager;

/**
 * Simple transaction benchmark of KvsClient without real connection.
 */
final class TransactionBench {

    private static final int DEFUALT_RUN_SEC = 30;

    private final boolean bFullBench;
    private final long minRunMsec;

    private TransactionBench(String[] args) {
        this.bFullBench = (args.length > 0 && args[0].equals("full"));
        this.minRunMsec = (bFullBench ? DEFUALT_RUN_SEC : 10) * 1000L;
    }

    private static class SimpleTransaction implements Callable<TxStatus> {

        private final RunManager mgr;
        private final RecordInfo info;

        SimpleTransaction(RunManager mgr, RecordInfo info) {
            this.mgr = mgr;
            this.info = info;
        }

        @Override
        public TxStatus call() throws Exception {
            var status = new TxStatus();
            var recBuilder = new RecordBuilder(info);
            var table = "TABLE1";
            var service = new KvsServiceStubForBench(info);
            long numTx = 0;
            try (var kvs = new KvsClientImpl(service)) {
                mgr.addReadyWorker();
                mgr.waitUntilWorkerStartTime();
                while (!mgr.isQuit()) {
                    var handle = kvs.beginTransaction().await();
                    long n = kvs.get(handle, table, recBuilder.makeRecordBuffer()).await().size();
                    n += kvs.put(handle, table, recBuilder.makeRecordBuffer()).await().size();
                    n += kvs.get(handle, table, recBuilder.makeRecordBuffer()).await().size();
                    status.addNumRecord(n);
                    kvs.commit(handle).await();
                    numTx++;
                }
            }
            status.addNumLoop(numTx);
            return status;
        }

    }

    private void bench(int nclient, RecordInfo info) throws InterruptedException, ExecutionException {
        System.out.printf("%d,%s,%d", nclient, info.type().toString(), info.num());
        System.out.printf(",%d", new RecordBuilder(info).makeRecordBuffer().toRecord().size());
        //
        RunManager mgr = new RunManager(nclient);
        ExecutorService executor = Executors.newFixedThreadPool(nclient);
        var clients = new LinkedList<Future<TxStatus>>();
        TxStatus allStatus = new TxStatus();
        for (int i = 0; i < nclient; i++) {
            clients.add(executor.submit(new SimpleTransaction(mgr, info)));
        }
        mgr.setWorkerStartTime();
        Thread.sleep(minRunMsec);
        mgr.setQuit();
        for (var future : clients) {
            var status = future.get();
            allStatus.addNumLoop(status.getNumLoop());
            allStatus.addNumRecord(status.getNumRecord());
        }
        executor.shutdown();
        //
        double sec = minRunMsec / 1000.0;
        long allNloop = allStatus.getNumLoop();
        long allNrec = allStatus.getNumRecord();
        System.out.printf(",%d,%d,%.1f,%.1f,%.2f", allNloop, allNrec, sec, allNloop / sec, 1e+6 * sec / allNloop);
        System.out.println();
    }

    private static void showCSVheader() {
        System.out.println("# nclient, valType, nvalue/rec, ncolumn/rec, numTx, numRec, elapseSec, tx/sec, usec/tx");
    }

    private void fullBench() throws InterruptedException, ExecutionException {
        showCSVheader();
        final int[] clientNums = { 1, 2, 4, 8, 16, 32, 64, 100 };
        // NOTE: first nvalue=1 is for warming up (JIT compile etc.).
        // use second nvalue=1 performance data for benchmark result.
        final int[] nvalues = { 1, 1, 10, 100 };
        for (var clientNum : clientNums) {
            for (var type : ValueType.values()) {
                for (var nvalue : nvalues) {
                    bench(clientNum, new RecordInfo(type, nvalue));
                }
            }
        }
    }

    private void shortBench() throws InterruptedException, ExecutionException {
        showCSVheader();
        final int[] clientNums = { 1, 2, 4, 8, 16, 32, 64, 100 };
        // NOTE: first nvalue=1 is for warming up (JIT compile etc.).
        // use second nvalue=1 performance data for benchmark result.
        final int[] nvalues = { 1, 1 };
        for (var clientNum : clientNums) {
            for (var nvalue : nvalues) {
                bench(clientNum, new RecordInfo(ValueType.LONG, nvalue));
            }
        }
    }

    private void bench() throws Exception {
        if (bFullBench) {
            fullBench();
        } else {
            shortBench();
        }
    }

    /**
     * main.
     * @param args program arguments. Only one integer value can be specified. It
     *        will be the default loop running second. Default value will be use if
     *        not specified.
     */
    public static void main(String[] args) {
        if (args.length > 0 && args[0].contains("help")) {
            System.err.println("Usage: java TransactionBench {short|full}");
        }
        TransactionBench app = new TransactionBench(args);
        try {
            app.bench();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
