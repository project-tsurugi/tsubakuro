package com.tsurugidb.tsubakuro.kvs.bench;

import java.io.IOException;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.impl.KvsClientImpl;
import com.tsurugidb.tsubakuro.kvs.impl.TransactionHandleImpl;

/**
 * PUT/GET benchmark of KvsClient without real connection.
 */
final class PutGetBench {

    private static final int DEFUALT_RUN_SEC = 30;

    private final long minRunMsec;

    private PutGetBench(String[] args) {
        this.minRunMsec = 1000L * (args.length > 0 ? Integer.parseInt(args[0]) : DEFUALT_RUN_SEC);
    }

    private void bench(boolean doPut, RecordInfo info) throws ServerException, IOException, InterruptedException {
        var recBuilder = new RecordBuilder(info);
        System.out.printf("%s %4d %7s : ", (doPut ? "PUT" : "GET"), info.num(), info.type().toString());
        var handle = new TransactionHandleImpl(123);
        var table = "TABLE1";
        var service = new KvsServiceStubForBench(info);
        long nloop = 0;
        long nrec = 0;
        final int loopblock = 1000;
        try (var kvs = new KvsClientImpl(service)) {
            Elapse elapse = new Elapse();
            long msec;
            do {
                for (var i = 0; i < loopblock; i++) {
                    if (doPut) {
                        var result = kvs.put(handle, table, recBuilder.makeRecordBuffer());
                        nrec += result.await().size();
                    } else {
                        var result = kvs.get(handle, table, recBuilder.makeRecordBuffer());
                        nrec += result.await().size();
                    }
                }
                nloop += loopblock;
                msec = elapse.msec();
            } while (msec < minRunMsec);
            double sec = msec / 1000.0;
            System.out.printf("%d loops, %d records, %.1f[sec], %.1f[op/sec], %.2f[usec/op]", nloop, nrec, sec,
                    nloop / sec, 1e+6 * sec / nloop);
            System.out.println();
        }
    }

    private void bench() throws ServerException, IOException, InterruptedException {
        final boolean[] putOrGet = { true, false };
        // NOTE: first colNum=1 is for warming up (JIT compile etc.).
        // use second colNum=1 performance data for benchmark result.
        final int[] colNums = { 1, 1, 10, 100, 1000 };
        for (var doPut : putOrGet) {
            for (var type : ValueType.values()) {
                for (var num : colNums) {
                    bench(doPut, new RecordInfo(type, num));
                }
            }
        }
    }

    /**
     * main.
     * @param args program arguments. Only one integer value can be specified.
     * It will be the default loop running second. Default value will be use if not specified.
     */
    public static void main(String[] args) {
        PutGetBench app = new PutGetBench(args);
        try {
            app.bench();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
