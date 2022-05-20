package com.nautilus_technologies.tsubakuro.low.measurement;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;
import com.tsurugidb.jogasaki.proto.SqlCommon;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;

public class SelectMulti extends Thread {
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

    public SelectMulti(SqlClient sqlClient, Profile profile, CyclicBarrier barrier, AtomicBoolean stop) throws IOException, ServerException, InterruptedException {
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
                        while (resultSet1.nextRecord()) {
                            resultSet1.nextColumn();
                            var noOid = resultSet1.getInt8();
                            profile.records++;
                        }
                    }
                    if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet1.getResponse().get().getResultCase())) {
                        throw new IOException("SQL error");
                    }
                } catch (ServerException e) {
                    if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(transaction.rollback().get().getResultCase())) {
                        throw new IOException("error in rollback");
                    }
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
