package com.nautilus_technologies.tsubakuro.low.measurement;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Objects;
import com.nautilus_technologies.tsubakuro.util.Pair;
import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

public class SelectLimitOne extends Thread {
    CyclicBarrier barrier;
    AtomicBoolean stop;
    Session session;
    Transaction transaction;
    RandomGenerator randomGenerator;
    Profile profile;

    //    PreparedStatement prepared2;
    PreparedStatement prepared1;
    long paramsWid;
    long paramsDid;
    long paramsCid;
    
    public SelectLimitOne(Connector connector, Session session, Profile profile, CyclicBarrier barrier, AtomicBoolean stop) throws IOException, ExecutionException, InterruptedException {
        this.barrier = barrier;
        this.stop = stop;
        this.profile = profile;
        this.session = session;
        this.session.connect(connector.connect().get());
        this.randomGenerator = new RandomGenerator();
	prepare();
    }
    
    void prepare()  throws IOException, ExecutionException, InterruptedException {
	String sql1 = "SELECT no_o_id FROM NEW_ORDER WHERE no_d_id = :no_d_id AND no_w_id = :no_w_id ORDER BY no_o_id";
        var ph1 = RequestProtos.PlaceHolder.newBuilder()
            .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("no_d_id").setType(CommonProtos.DataType.INT8))
            .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("no_w_id").setType(CommonProtos.DataType.INT8));
        prepared1 = session.prepare(sql1, ph1).get();
    }
    
    void setParams() {
	paramsWid = randomGenerator.uniformWithin(1, profile.warehouses);
        paramsDid = randomGenerator.uniformWithin(1, Scale.DISTRICTS);  // scale::districts
        paramsCid = randomGenerator.uniformWithin(1, Scale.CUSTOMERS);  // scale::customers
    }

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

                // "SELECT no_o_id FROM NEW_ORDER WHERE no_d_id = :no_d_id AND no_w_id = :no_w_id ORDER BY no_o_id"
                var ps1 = RequestProtos.ParameterSet.newBuilder()
                    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("no_d_id").setInt8Value(paramsDid))
                    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("no_w_id").setInt8Value(paramsWid));
                var future1 = transaction.executeQuery(prepared1, ps1);
                var resultSet1 = future1.getLeft().get();
		now = System.nanoTime();
		profile.head += (now - prev);
		prev = now;
                try {
                    if (!Objects.isNull(resultSet1)) {
                        if (!resultSet1.nextRecord()) {
                            if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(future1.getRight().get().getResultCase())) {
                                throw new ExecutionException(new IOException("SQL error"));
                            }
                            continue;  // noOid is exhausted, it's OK and continue this transaction
                        }
                        resultSet1.nextColumn();
                        var noOid = resultSet1.getInt8();
                        resultSet1.close();
                        resultSet1 = null;
                    }
                    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(future1.getRight().get().getResultCase())) {
                        throw new ExecutionException(new IOException("SQL error"));
                    }
                } catch (ExecutionException e) {
		    if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(transaction.rollback().get().getResultCase())) {
			throw new IOException("error in rollback");
		    }
		    transaction = null;
		    continue;
                } finally {
                    if (!Objects.isNull(resultSet1)) {
                        resultSet1.close();
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

        } catch (IOException | ExecutionException | InterruptedException | BrokenBarrierException e) {
            System.out.println(e);
	} finally {
	    try {
		prepared1.close();
		session.close();
	    } catch (IOException e) {
		System.out.println(e);
	    }
	}
    }
}
