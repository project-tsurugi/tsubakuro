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
import com.tsurugidb.jogasaki.proto.SqlResponse;

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
                var resultSet2 = future2.get();
                now = System.nanoTime();
                profile.head += (now - prev);
                prev = now;
                try {
                    if (!Objects.isNull(resultSet2)) {
                        if (!resultSet2.nextRecord()) {
                            if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet2.getResponse().get().getResultCase())) {
                                throw new IOException("SQL error");
                            }
                            throw new IOException("no record");
                        }
                        resultSet2.nextColumn();
                        var dNextOid = resultSet2.getInt8();
                        resultSet2.nextColumn();
                        var dTax = resultSet2.getFloat8();
                        if (resultSet2.nextRecord()) {
                            if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet2.getResponse().get().getResultCase())) {
                                throw new IOException("SQL error");
                            }
                            throw new IOException("found multiple records");
                        }
                    }
                    if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet2.getResponse().get().getResultCase())) {
                        throw new IOException("SQL error");
                    }
                } catch (ServerException e) {
                    if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(transaction.rollback().get().getResultCase())) {
                        throw new IOException("error in rollback");
                    }
                    transaction = null;
                    continue;
                } finally {
                    if (!Objects.isNull(resultSet2)) {
                        resultSet2.close();
                        resultSet2 = null;
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
