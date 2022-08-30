package com.tsurugidb.tsubakuro.examples.measurement;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.Parameters;

public class SelectOne extends Thread {
    CyclicBarrier barrier;
    AtomicBoolean stop;
    SqlClient sqlClient;
    Transaction transaction;
    RandomGenerator randomGenerator;
    Profile profile;

    PreparedStatement prepared2;
    long paramsWid;
    long paramsDid;

    public SelectOne(SqlClient sqlClient, Profile profile, CyclicBarrier barrier, AtomicBoolean stop) throws IOException, ServerException, InterruptedException {
        this.barrier = barrier;
        this.stop = stop;
        this.profile = profile;
        this.sqlClient = sqlClient;
        this.randomGenerator = new RandomGenerator();
        prepare();
    }

    void prepare()  throws IOException, ServerException, InterruptedException {
        String sql2 = "SELECT d_next_o_id, d_tax FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id";
        prepared2 = sqlClient.prepare(sql2,
            Placeholders.of("d_w_id", long.class),
            Placeholders.of("d_id", long.class)).get();
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
                if (Objects.isNull(transaction)) {
                    transaction = sqlClient.createTransaction().await();
                }
                setParams();
                now = System.nanoTime();
                if (prev != 0) {
                    profile.commit += (now - prev);
                }
                prev = now;

                // SELECT d_next_o_id, d_tax FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id
                var future2 = transaction.executeQuery(prepared2,
                    Parameters.of("d_w_id", (long) paramsWid),
                    Parameters.of("d_id", (long) paramsDid));
                try (var resultSet2 = future2.get()) {
                    now = System.nanoTime();
                    profile.head += (now - prev);
                    prev = now;
                    try {
                        if (resultSet2.nextRow()) {
                            resultSet2.nextColumn();
                            var dNextOid = resultSet2.fetchInt8Value();
                            resultSet2.nextColumn();
                            var dTax = resultSet2.fetchFloat8Value();
                        }
                    } catch (ServerException e) {
                        transaction.rollback().get();
                        transaction = null;
                        continue;
                    } finally {
                        resultSet2.close();
                    }
                }
                now = System.nanoTime();
                profile.body += (now - prev);
                prev = now;

                profile.count++;
                if ((profile.count % 1000) == 0) {
                    transaction.commit().get();
                    transaction = null;
                }
            }
            profile.elapsed = System.nanoTime() - start;

        } catch (IOException | ServerException | InterruptedException | BrokenBarrierException e) {
            System.out.println(e);
        } finally {
            try {
                prepared2.close();
            } catch (IOException | ServerException | InterruptedException e) {
                System.out.println(e);
            }
        }
    }
}
