package com.tsurugidb.tsubakuro.examples.measurement;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Transaction;

public class Insert extends Thread {
    CyclicBarrier barrier;
    AtomicBoolean stop;
    SqlClient sqlClient;
    Transaction transaction;
    RandomGenerator randomGenerator;
    Profile profile;

    PreparedStatement prepared5;
    long paramsWid;
    long paramsDid;
    long oid;

    public Insert(SqlClient sqlClient, Profile profile, CyclicBarrier barrier, AtomicBoolean stop) throws IOException, ServerException, InterruptedException {
        this.barrier = barrier;
        this.stop = stop;
        this.profile = profile;
        this.sqlClient = sqlClient;
        this.randomGenerator = new RandomGenerator();
        this.oid = Scale.ORDERS + 1;
        prepare();
    }

    void prepare() throws IOException, ServerException, InterruptedException {
        String sql5 = "INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)VALUES (:no_o_id, :no_d_id, :no_w_id)";
        prepared5 = sqlClient.prepare(sql5,
            Placeholders.of("dno_o_id", long.class),
            Placeholders.of("no_d_id", long.class),
            Placeholders.of("no_w_id", long.class)).await();
    }

    void setParams() {
        paramsWid = randomGenerator.uniformWithin(1, profile.warehouses);
        paramsDid = randomGenerator.uniformWithin(1, Scale.DISTRICTS);  // scale::districts
    }

    @Override
    public void run() {
        try {
            barrier.await();

            long start = System.nanoTime();
            long prev = 0;
            long now = 0;
            while (!stop.get()) {
                if (transaction == null) {
                    transaction = sqlClient.createTransaction().await();
                }
                setParams();
                now = System.nanoTime();
                if (prev != 0) {
                    profile.commit += (now - prev);
                }
                prev = now;

                // INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)VALUES (:no_o_id, :no_d_id, :no_w_id
                var future5 = transaction.executeStatement(prepared5,
                    Parameters.of("no_o_id", oid++),
                    Parameters.of("no_d_id", paramsDid),
                    Parameters.of("no_w_id", paramsWid));
                try {
                    future5.await();
                } catch (ServerException e) {
                    transaction.rollback().get();
                    transaction = null;
                    continue;
                }
                now = System.nanoTime();
                profile.body += (now - prev);
                prev = now;

                profile.count++;
                if ((profile.count % 1000) == 0) {
                    transaction.commit().await();
                    transaction = null;
                }
            }
            profile.elapsed = System.nanoTime() - start;


        } catch (IOException | ServerException | InterruptedException | BrokenBarrierException e) {
            System.out.println(e);
        } finally {
            try {
                prepared5.close();
            } catch (IOException | ServerException | InterruptedException e) {
                System.out.println(e);
            }
        }
    }
}
