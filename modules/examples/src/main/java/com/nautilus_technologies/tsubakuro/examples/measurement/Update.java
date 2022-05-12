package com.nautilus_technologies.tsubakuro.low.measurement;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

public class Update extends Thread {
    CyclicBarrier barrier;
    AtomicBoolean stop;
    Session session;
    Transaction transaction;
    RandomGenerator randomGenerator;
    Profile profile;

    PreparedStatement prepared8;
    long paramsDid;
    long olSupplyWid;
    long sQuantity;
    long olIid;
    long oid;

    public Update(Connector connector, Session session, Profile profile, CyclicBarrier barrier, AtomicBoolean stop) throws IOException, ServerException, InterruptedException {
        this.barrier = barrier;
        this.stop = stop;
        this.profile = profile;
        this.session = session;
        this.session.connect(connector.connect().get());
        this.randomGenerator = new RandomGenerator();
        this.oid = Scale.ORDERS + 1;
        prepare();
    }

    void prepare() throws IOException, ServerException, InterruptedException {
        String sql8 = "UPDATE STOCK SET s_quantity = :s_quantity WHERE s_i_id = :s_i_id AND s_w_id = :s_w_id";
        var ph8 = RequestProtos.PlaceHolder.newBuilder()
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("s_quantity").setType(CommonProtos.DataType.INT8))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("s_i_id").setType(CommonProtos.DataType.INT8))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("s_w_id").setType(CommonProtos.DataType.INT8))
                .build();
        prepared8 = session.prepare(sql8, ph8).get();
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
                    transaction = session.createTransaction().get();
                }
                setParams();
                now = System.nanoTime();
                if (prev != 0) {
                    profile.commit += (now - prev);
                }
                prev = now;

                // UPDATE STOCK SET s_quantity = :s_quantity WHERE s_i_id = :s_i_id AND s_w_id = :s_w_id
                var ps8 = RequestProtos.ParameterSet.newBuilder()
                        .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("s_quantity").setInt8Value(sQuantity))
                        .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("s_i_id").setInt8Value(olIid))
                        .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("s_w_id").setInt8Value(olSupplyWid))
                        .build();
                var future8 = transaction.executeStatement(prepared8, ps8);
                var result8 = future8.get();
                if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(result8.getResultCase())) {
                    if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(transaction.rollback().get().getResultCase())) {
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
                session.close();
            } catch (IOException | ServerException | InterruptedException e) {
                System.out.println(e);
            }
        }
    }
}
