package com.tsurugidb.tsubakuro.kvs.bench;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.tsurugidb.tsubakuro.kvs.impl.KvsClientImpl;
import com.tsurugidb.tsubakuro.kvs.util.RunManager;
import com.tsurugidb.tsubakuro.kvs.ycsb.Constants;

/**
 * Simple transaction benchmark of KvsClient without real connection.
 */
final class TransactionBench {

    private final List<Integer> numClients;
    private final long warmupMsec;
    private final long runningMsec;
    private final long waitLoop;

    private static final ValueType DEFAULT_VALUE_TYPE = ValueType.LONG;
    private static final int DEFAULT_VALUE_NUM = 1;
    private static final RecordInfo DEFAULT_RECORD_INFO = new RecordInfo(DEFAULT_VALUE_TYPE, DEFAULT_VALUE_NUM);

    private TransactionBench(String[] args) {
        var nums = args[0].split(",");
        this.numClients = new ArrayList<Integer>(nums.length);
        for (var s : nums) {
            this.numClients.add(Integer.parseInt(s));
        }
        this.warmupMsec = Long.parseLong(args[1]) * 1000L;
        this.runningMsec = Long.parseLong(args[2]) * 1000L;
        this.waitLoop = Long.parseLong(args[3]);
        System.out.println("numClients=" + args[0] + ", warmupSec=" + (warmupMsec / 1000) + ", runningSec="
                + (runningMsec / 1000) + ", waitLoop=" + waitLoop);
    }

    private class SimpleTransaction implements Callable<TxStatus> {

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
            var service = new KvsServiceStubForBench(info, waitLoop);
            long numTx = 0;
            try (var kvs = new KvsClientImpl(service)) {
                mgr.addReadyWorker();
                mgr.waitUntilWorkerStartTime();
                while (!mgr.isQuit()) {
                    var handle = kvs.beginTransaction().await();
                    long n = 0;
                    for (int i = 0; i < Constants.OPS_PER_TX; i += 2) {
                        n = kvs.get(handle, table, recBuilder.makeRecordBuffer()).await().size();
                        n += kvs.put(handle, table, recBuilder.makeRecordBuffer()).await().size();
                    }
                    status.addNumRecord(n);
                    kvs.commit(handle).await();
                    numTx++;
                }
            }
            status.addNumLoop(numTx);
            return status;
        }

    }

    private void bench(int nclient, long runMsec, RecordInfo info) throws InterruptedException, ExecutionException {
        System.out.printf("%d,%s,%d", nclient, info.type().toString(), info.num());
        System.out.printf(",%d", new RecordBuilder(info).makeRecordBuffer().toRecord().size());
        //
        RunManager mgr = new RunManager(nclient);
        ExecutorService executor = Executors.newFixedThreadPool(nclient);
        var clients = new ArrayList<Future<TxStatus>>(nclient);
        TxStatus allStatus = new TxStatus();
        for (int i = 0; i < nclient; i++) {
            clients.add(executor.submit(new SimpleTransaction(mgr, info)));
        }
        mgr.setWorkerStartTime();
        Thread.sleep(runMsec);
        mgr.setQuit();
        for (var future : clients) {
            var status = future.get();
            allStatus.addNumLoop(status.getNumLoop());
            allStatus.addNumRecord(status.getNumRecord());
        }
        executor.shutdown();
        //
        double sec = runMsec / 1000.0;
        long allNloop = allStatus.getNumLoop();
        long allNrec = allStatus.getNumRecord();
        double usecTx = 1e+6 * sec / allNloop;
        System.out.printf(",%d,%d,%.1f,%.1f,%.3f,%.3f", allNloop, allNrec, sec, allNloop / sec, usecTx,
                usecTx / Constants.OPS_PER_TX);
        System.out.println();
    }

    private static void showCSVheader() {
        System.out.println(
                "# nclient, valType, nvalue/rec, ncolumn/rec, numTx, numRec, elapseSec, tx/sec, usec/tx, usec/op");
    }

    private void warmup() throws Exception {
        bench(1, warmupMsec, DEFAULT_RECORD_INFO);
    }

    private void bench() throws Exception {
        showCSVheader();
        for (var clientNum : numClients) {
            bench(clientNum, runningMsec, DEFAULT_RECORD_INFO);
        }
    }

    /**
     * main.
     * @param args program arguments. Only one integer value can be specified. It
     *        will be the default loop running second. Default value will be use if
     *        not specified.
     */
    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: java TransactionBench num_client(s) warmup_sec running_sec reply_wait_msec");
            System.err.println("\tex: java TransactionBench 1 10 60 30");
            System.err.println("\tex: java TransactionBench 1,2,4,8 10 30 20");
        }
        TransactionBench app = new TransactionBench(args);
        try {
            app.warmup();
            app.bench();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
