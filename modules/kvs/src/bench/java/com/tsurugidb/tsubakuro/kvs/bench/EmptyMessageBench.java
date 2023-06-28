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
import com.tsurugidb.tsubakuro.kvs.impl.KvsServiceStub;

/**
 * Empty message send/receive benchmark
 */
final class EmptyMessageBench {

    private final URI endpoint;
    private final LinkedList<Integer> numClients = new LinkedList<>();
    private final long warmupMsec;
    private final long runningMsec;
    private final Credential credential = NullCredential.INSTANCE;

    private EmptyMessageBench(String[] args) {
        this.endpoint = URI.create(args[0]);
        for (var s : args[1].split(",")) {
            this.numClients.add(Integer.parseInt(s));
        }
        this.warmupMsec = Long.parseLong(args[2]) * 1000L;
        this.runningMsec = Long.parseLong(args[3]) * 1000L;
        System.out.println("endpoint=" + endpoint + ", numClients=" + args[1] + ", warmupSec=" + (warmupMsec / 1000)
                + ", runningSec=" + (runningMsec / 1000));
    }

    private class EmptyMessageLoop implements Callable<Long> {

        private final long loopMsec;

        EmptyMessageLoop(long loopMsec) {
            this.loopMsec = loopMsec;
        }

        @Override
        public Long call() throws Exception {
            long nloop = 0;
            try (var session = SessionBuilder.connect(endpoint).withCredential(credential).create();
                var service = new KvsServiceStub(session)) {
                final long loopblock = 10_000L;
                long msec;
                Elapse elapse = new Elapse();
                do {
                    for (var i = 0; i < loopblock; i++) {
                        service.request().await();
                    }
                    msec = elapse.msec();
                    nloop += loopblock;
                } while (msec < loopMsec);
            }
            return nloop;
        }
    }

    private void bench(int numClient, long loopMsec) throws InterruptedException, ExecutionException {
        ExecutorService executor = Executors.newFixedThreadPool(numClient);
        var clients = new LinkedList<Future<Long>>();
        Elapse e = new Elapse();
        for (int i = 0; i < numClient; i++) {
            clients.add(executor.submit(new EmptyMessageLoop(loopMsec)));
        }
        long nloopSum = 0;
        for (var future : clients) {
            nloopSum += future.get();
        }
        executor.shutdown();
        //
        long allMsec = e.msec();
        double sec = allMsec / 1000.0;
        long numMsg = 2 * nloopSum; // one loop handles request and response messages
        System.out.printf("%d,%d,%.1f,%.1f,%.1f", numClient, numMsg, sec, numMsg / sec, sec / numMsg * 1e+6);
        System.out.println();
    }

    private void bench() throws InterruptedException, ExecutionException {
        System.out.println("# nclient, elapsed_sec, num_messages, message/sec, usec/message");
        bench(1, warmupMsec);
        for (int numClient : numClients) {
            bench(numClient, runningMsec);
        }
    }

    /**
     * main.
     * @param args program arguments.
     */
    public static void main(String[] args) {
        if (args.length < 4 || args[0].contains("help")) {
            System.out.println("Usage: java EmptyMessageBench endpoint num_client(s) warmup_sec running_sec");
            System.out.println("\tex: java EmptyMessageBench ipc:tsurugi 1 30 60");
            System.out.println("\tex: java EmptyMessageBench ipc:tsurugi 1,2,4,8,16 30 60");
            return;
        }
        EmptyMessageBench app = new EmptyMessageBench(args);
        try {
            app.bench();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
