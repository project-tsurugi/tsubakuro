package com.nautilus_technologies.tsubakuro.low;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Objects;
import com.nautilus_technologies.tsubakuro.util.Pair;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

public class Insert extends Thread {
    CyclicBarrier barrier;
    AtomicBoolean stop;
    Session session;
    Transaction transaction;
    RandomGenerator randomGenerator;
    Profile profile;

    PreparedStatement prepared5;
    long warehouses;
    long paramsWid;
    long paramsDid;
    long oid;
    
    public Insert(Connector connector, Session session, Profile profile, CyclicBarrier barrier, AtomicBoolean stop) throws IOException, ExecutionException, InterruptedException {
        this.barrier = barrier;
        this.stop = stop;
        this.profile = profile;
        this.session = session;
        this.session.connect(connector.connect().get());
        this.randomGenerator = new RandomGenerator();
	this.oid = Scale.ORDERS + 1;
	prepare();
    }

    void prepare() throws IOException, ExecutionException, InterruptedException {
        String sql5 = "INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)VALUES (:no_o_id, :no_d_id, :no_w_id)";
        var ph5 = RequestProtos.PlaceHolder.newBuilder()
            .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("no_o_id").setType(CommonProtos.DataType.INT8))
            .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("no_d_id").setType(CommonProtos.DataType.INT8))
            .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("no_w_id").setType(CommonProtos.DataType.INT8));
        prepared5 = session.prepare(sql5, ph5).get();
    }

    void setParams() {
	paramsWid = randomGenerator.uniformWithin(1, profile.warehouses);
        paramsDid = randomGenerator.uniformWithin(1, Scale.DISTRICTS);  // scale::districts
    }

    public void run() {
	try {
	    barrier.await();
	    
            long start = System.nanoTime();
	    while (!stop.get()) {
		if (Objects.isNull(transaction)) {
		    transaction = session.createTransaction().get();
		}
		setParams();

		// INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)VALUES (:no_o_id, :no_d_id, :no_w_id
		var ps5 = RequestProtos.ParameterSet.newBuilder()
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("no_o_id").setInt8Value(oid++))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("no_d_id").setInt8Value(paramsDid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("no_w_id").setInt8Value(paramsWid));
		var future5 = transaction.executeStatement(prepared5, ps5);
		var result5 = future5.get();
		if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(result5.getResultCase())) {
		    if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(transaction.rollback().get().getResultCase())) {
			throw new IOException("error in rollback");
		    }
		    transaction = null;
		    continue;
		}

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
		prepared5.close();
		session.close();
	    } catch (IOException e) {
		System.out.println(e);
	    }
	}
    }
}
