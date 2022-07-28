package com.nautilus_technologies.tsubakuro.examples.measurement;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;

public class SelectLimitOne extends Thread {
    CyclicBarrier barrier;
    AtomicBoolean stop;
    SqlClient sqlClient;
    Transaction transaction;
    RandomGenerator randomGenerator;
    Profile profile;

    //    PreparedStatement prepared2;
    PreparedStatement prepared1;
    long paramsWid;
    long paramsDid;
    long paramsCid;

    public SelectLimitOne(SqlClient sqlClient, Profile profile, CyclicBarrier barrier, AtomicBoolean stop) throws IOException, ServerException, InterruptedException {
        this.barrier = barrier;
        this.stop = stop;
        this.profile = profile;
        this.sqlClient = sqlClient;
        this.randomGenerator = new RandomGenerator();
        prepare();
    }

    void prepare()  throws IOException, ServerException, InterruptedException {
        String sql1 = "SELECT no_o_id FROM NEW_ORDER WHERE no_d_id = :no_d_id AND no_w_id = :no_w_id ORDER BY no_o_id";
        prepared1 = sqlClient.prepare(sql1,
            Placeholders.of("no_d_id", long.class),
            Placeholders.of("no_w_id", long.class)).get();
    }

    void setParams() {
        paramsWid = randomGenerator.uniformWithin(1, profile.warehouses);
        paramsDid = randomGenerator.uniformWithin(1, Scale.DISTRICTS);  // scale::districts
        paramsCid = randomGenerator.uniformWithin(1, Scale.CUSTOMERS);  // scale::customers
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

                // "SELECT no_o_id FROM NEW_ORDER WHERE no_d_id = :no_d_id AND no_w_id = :no_w_id ORDER BY no_o_id"
                var future1 = transaction.executeQuery(prepared1,
                    Parameters.of("no_d_id", (long) paramsDid),
                    Parameters.of("no_w_id", (long) paramsWid));
                var resultSet1 = future1.get();
                now = System.nanoTime();
                profile.head += (now - prev);
                prev = now;
                try {
                    if (!Objects.isNull(resultSet1)) {
                        if (!resultSet1.nextRow()) {
                            resultSet1.getResponse().get();
                            continue;  // noOid is exhausted, it's OK and continue this transaction
                        }
                        resultSet1.nextColumn();
                        var noOid = resultSet1.fetchInt8Value();
                    }
                    resultSet1.getResponse().get();
                } catch (ServerException e) {
                    transaction.rollback().get();
                    transaction = null;
                    continue;
                } finally {
                    if (!Objects.isNull(resultSet1)) {
                        resultSet1.close();
                        resultSet1 = null;
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
                prepared1.close();
            } catch (IOException | ServerException | InterruptedException e) {
                System.out.println(e);
            }
        }
    }
}
