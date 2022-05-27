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

public class Update extends Thread {
    CyclicBarrier barrier;
    AtomicBoolean stop;
    SqlClient sqlClient;
    Transaction transaction;
    RandomGenerator randomGenerator;
    Profile profile;

    PreparedStatement prepared8;
    long paramsDid;
    long olSupplyWid;
    long sQuantity;
    long olIid;
    long oid;

    public Update(SqlClient sqlClient, Profile profile, CyclicBarrier barrier, AtomicBoolean stop) throws IOException, ServerException, InterruptedException {
        this.barrier = barrier;
        this.stop = stop;
        this.profile = profile;
        this.sqlClient = sqlClient;
        this.randomGenerator = new RandomGenerator();
        this.oid = Scale.ORDERS + 1;
        prepare();
    }

    void prepare() throws IOException, ServerException, InterruptedException {
        String sql8 = "UPDATE STOCK SET s_quantity = :s_quantity WHERE s_i_id = :s_i_id AND s_w_id = :s_w_id";
        prepared8 = sqlClient.prepare(sql8,
            Placeholders.of("s_quantity", long.class),
            Placeholders.of("s_i_id", long.class),
            Placeholders.of("s_w_id", long.class)).get();
    }

    void setParams() {
        paramsDid = randomGenerator.uniformWithin(1, Scale.DISTRICTS);  // scale::districts
        olSupplyWid = randomGenerator.uniformWithin(1, profile.warehouses);
        sQuantity = randomGenerator.uniformWithin(1, 10);
        olIid = randomGenerator.nonUniform8191Within(1, Scale.ITEMS); // scale::items
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

                // UPDATE STOCK SET s_quantity = :s_quantity WHERE s_i_id = :s_i_id AND s_w_id = :s_w_id
                var future8 = transaction.executeStatement(prepared8,
                    Parameters.of("s_quantity", (long) sQuantity),
                    Parameters.of("s_i_id", (long) olIid),
                    Parameters.of("s_w_id", (long) olSupplyWid));

                var result8 = future8.get();
                if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(result8.getResultCase())) {
                    if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(transaction.rollback().get().getResultCase())) {
                        throw new IOException("error in rollback");
                    }
                    transaction = null;
                    continue;
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
                prepared8.close();
            } catch (IOException | ServerException | InterruptedException e) {
                System.out.println(e);
            }
        }
    }
}
