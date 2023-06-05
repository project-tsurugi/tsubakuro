package com.tsurugidb.tsubakuro.kvs.bench;

import java.net.URI;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.kvs.KvsClient;

/**
 * Simple transaction benchmark of KvsClient without real connection.
 */
final class RealTransactionBench {

    private static final int DEFUALT_RUN_SEC = 30;
    private final Credential credential = NullCredential.INSTANCE;

    private final boolean bFullBench;
    private final URI endpoint;
    private final long minRunMsec;

    private RealTransactionBench(String[] args) {
        this.bFullBench = args.length >= 1 && args[0].equals("full");
        boolean bStream = args.length >= 2 && args[1].equals("stream");
        String point = bStream ? "tcp://localhost:12345" : "ipc:tateyama";
        this.endpoint = URI.create(point);
        this.minRunMsec = (bFullBench ? DEFUALT_RUN_SEC : 10) * 1000L;
        System.err.println("bFull=" + bFullBench + ", endpoint=" + endpoint + ", minSec=" + minRunMsec / 1000);
    }

    private class SimpleTransaction implements Callable<TxStatus> {

        private final RecordInfo info;

        SimpleTransaction(RecordInfo info) {
            this.info = info;
        }

        @Override
        public TxStatus call() throws Exception {
            var status = new TxStatus();
            var recBuilder = new RecordBuilder(info);
            var table = "TABLE1";
            try (var session = SessionBuilder.connect(endpoint).withCredential(credential).create();
                var kvs = KvsClient.attach(session)) {
                Elapse elapse = new Elapse();
                final int loopblock = 10_000;
                long msec;
                do {
                    for (var i = 0; i < loopblock; i++) {
                        try (var handle = kvs.beginTransaction().await()) {
                            kvs.get(handle, table, recBuilder.makeRecordBuffer());
                            kvs.put(handle, table, recBuilder.makeRecordBuffer());
                            kvs.get(handle, table, recBuilder.makeRecordBuffer());
                            status.addNumRecord(3);
                            kvs.commit(handle);
                        }
                    }
                    status.addNumLoop(loopblock);
                    msec = elapse.msec();
                } while (msec < minRunMsec);
                status.setElapseMsec(msec);
            }
            return status;
        }
    }

    private void bench(int nclient, RecordInfo info) throws InterruptedException, ExecutionException {
        System.out.printf("%d,%s,%d", nclient, info.type().toString(), info.num());
        System.out.printf(",%d", new RecordBuilder(info).makeRecordBuffer().toRecord().size());
        //
        ExecutorService executor = Executors.newFixedThreadPool(nclient);
        var clients = new LinkedList<Future<TxStatus>>();
        TxStatus allStatus = new TxStatus();
        Elapse e = new Elapse();
        for (int i = 0; i < nclient; i++) {
            clients.add(executor.submit(new SimpleTransaction(info)));
        }
        for (var future : clients) {
            var status = future.get();
            allStatus.addNumLoop(status.getNumLoop());
            allStatus.addNumRecord(status.getNumRecord());
            allStatus.addElapseMsec(status.getElapseMsec());
        }
        executor.shutdown();
        //
        long allMsec = e.msec();
        double sec = allMsec / 1000.0;
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
     * @param args program arguments.
     */
    public static void main(String[] args) {
        if (args.length > 0 && args[0].contains("help")) {
            System.err.println("Usage: java TransactionBench {short|full} {ipc|stream}");
            System.err.println("   without args means \"TransactionBench short ipc\"");
            return;
        }
        RealTransactionBench app = new RealTransactionBench(args);
        try {
            app.bench();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
