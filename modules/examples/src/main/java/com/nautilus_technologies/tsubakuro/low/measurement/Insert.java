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

public class Insert extends Thread {
    CyclicBarrier barrier;
    AtomicBoolean stop;
    Session session;
    Transaction transaction;
    RandomGenerator randomGenerator;
    Profile profile;

    PreparedStatement prepared5;
    long paramsWid;
    long paramsDid;
    long oid;

    public Insert(Connector connector, Session session, Profile profile, CyclicBarrier barrier, AtomicBoolean stop) throws IOException, ServerException, InterruptedException {
        this.barrier = barrier;
        this.stop = stop;
        this.profile = profile;
        this.session = session;
        this.session.connect(connector.connect().await());
        this.randomGenerator = new RandomGenerator();
        this.oid = Scale.ORDERS + 1;
        prepare();
    }

    void prepare() throws IOException, ServerException, InterruptedException {
        String sql5 = "INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)VALUES (:no_o_id, :no_d_id, :no_w_id)";
        var ph5 = RequestProtos.PlaceHolder.newBuilder()
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("no_o_id").setType(CommonProtos.DataType.INT8))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("no_d_id").setType(CommonProtos.DataType.INT8))
                .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("no_w_id").setType(CommonProtos.DataType.INT8))
                .build();
        prepared5 = session.prepare(sql5, ph5).await();
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
                    transaction = session.createTransaction().await();
                }
                setParams();
                now = System.nanoTime();
                if (prev != 0) {
                    profile.commit += (now - prev);
                }
                prev = now;

                // INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)VALUES (:no_o_id, :no_d_id, :no_w_id
                var ps5 = RequestProtos.ParameterSet.newBuilder()
                        .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("no_o_id").setInt8Value(oid++))
                        .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("no_d_id").setInt8Value(paramsDid))
                        .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("no_w_id").setInt8Value(paramsWid))
                        .build();
                var future5 = transaction.executeStatement(prepared5, ps5.getParametersList());
                var result5 = future5.await();
                if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(result5.getResultCase())) {
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
                session.close();
            } catch (IOException | ServerException | InterruptedException e) {
                System.out.println(e);
            }
        }
    }
}
