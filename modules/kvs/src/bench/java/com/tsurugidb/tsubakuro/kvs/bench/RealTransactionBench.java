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
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.sql.SqlClient;

/**
 * Simple transaction benchmark of KvsClient without real connection.
 */
final class RealTransactionBench {

    private static final int DEFUALT_RUN_SEC = 30;
    private final Credential credential = NullCredential.INSTANCE;

    private final boolean bFullBench;
    private final URI endpoint;
    private final boolean useSameTable;
    private final LinkedList<String> tableNames = new LinkedList<>();
    private final long minRunMsec;

    private RealTransactionBench(String[] args) {
        this.bFullBench = args[0].equals("full");
        this.endpoint = URI.create(args[1]);
        this.useSameTable = args[2].equals("sameTable");
        this.minRunMsec = (bFullBench ? DEFUALT_RUN_SEC : 10) * 1000L;
        System.err.println("bFull=" + bFullBench + ", endpoint=" + endpoint + ", minSec=" + minRunMsec / 1000
                + ", useSameTable=" + useSameTable);
    }

    private class SimpleTransaction implements Callable<TxStatus> {

        private final String tableName;
        private final RecordBuilder recBuilder;

        SimpleTransaction(String tableName, RecordInfo info, int clientId) {
            this.tableName = tableName;
            this.recBuilder = new RecordBuilder(info, clientId);
        }

        @Override
        public TxStatus call() throws Exception {
            var status = new TxStatus();
            try (var session = SessionBuilder.connect(endpoint).withCredential(credential).create();
                var kvs = KvsClient.attach(session)) {
                Elapse elapse = new Elapse();
                final int loopblock = 10_000;
                long msec;
                do {
                    for (var i = 0; i < loopblock; i++) {
                        try (var handle = kvs.beginTransaction().await()) {
                            var record = recBuilder.makeRecordBuffer();
                            var key = new RecordBuffer();
                            var r = record.toRecord();
                            key.add(r.getName(0), r.getValue(0));
                            kvs.put(handle, tableName, record).await();
                            kvs.get(handle, tableName, key).await();
                            kvs.get(handle, tableName, key).await();
                            status.addNumRecord(3);
                            kvs.commit(handle).await();
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

    void initDB(int nclient) throws Exception {
        try (var session = SessionBuilder.connect(endpoint).withCredential(credential).create();
            var client = SqlClient.attach(session)) {
            int nTable = (useSameTable ? 1 : nclient);
            for (int i = 0; i < nTable; i++) {
                String tableName = "TABLE" + i;
                String sql = String.format("CREATE TABLE %s (%s BIGINT PRIMARY KEY, %s BIGINT)", tableName,
                        RecordBuilder.FIRST_KEY_NAME, RecordBuilder.FIRST_VALUE_NAME);
                try (var tx = client.createTransaction().await()) {
                    tx.executeStatement(sql).await();
                    tx.commit().await();
                    tableNames.add(tableName);
                }
            }
        }
        System.out.println(tableNames.size() + " tables created");
    }

    void bench(int nclient, RecordInfo info) throws InterruptedException, ExecutionException {
        System.out.printf("%d,%s,%d", nclient, info.type().toString(), info.num());
        System.out.printf(",%d", new RecordBuilder(info).makeRecordBuffer().toRecord().size());
        //
        ExecutorService executor = Executors.newFixedThreadPool(nclient);
        var clients = new LinkedList<Future<TxStatus>>();
        TxStatus allStatus = new TxStatus();
        Elapse e = new Elapse();
        for (int i = 0; i < nclient; i++) {
            String tableName = useSameTable ? tableNames.get(0) : tableNames.get(i);
            clients.add(executor.submit(new SimpleTransaction(tableName, info, i)));
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
        // NOTE: first clientNums=1 is for warming up (JIT compile etc.).
        final int[] clientNums = { 1, 1, 2, 4, 8, 16, 32, 64, 100 };
        final int[] nvalues = { 1 };
        for (var clientNum : clientNums) {
            for (var nvalue : nvalues) {
                bench(clientNum, new RecordInfo(ValueType.LONG, nvalue));
            }
        }
    }

    void bench() throws Exception {
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
        if (args.length < 3 && args[0].contains("help")) {
            System.err.println("Usage: java TransactionBench {short|full} endpoint {sameTable|eachTable}");
            return;
        }
        RealTransactionBench app = new RealTransactionBench(args);
        try {
            app.initDB(100);
            app.bench();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
