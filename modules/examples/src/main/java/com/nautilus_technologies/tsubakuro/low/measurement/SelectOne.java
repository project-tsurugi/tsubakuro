package com.nautilus_technologies.tsubakuro.low.measurement;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Objects;
import com.nautilus_technologies.tsubakuro.util.Pair;
import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

public class SelectOne extends Thread {
    CyclicBarrier barrier;
    AtomicBoolean stop;
    Session session;
    Transaction transaction;
    RandomGenerator randomGenerator;
    Profile profile;

    PreparedStatement prepared2;
    long paramsWid;
    long paramsDid;
    
    public SelectOne(Connector connector, Session session, Profile profile, CyclicBarrier barrier, AtomicBoolean stop) throws IOException, ExecutionException, InterruptedException {
        this.barrier = barrier;
        this.stop = stop;
        this.profile = profile;
        this.session = session;
        this.session.connect(connector.connect().get());
        this.randomGenerator = new RandomGenerator();
	prepare();
    }
    
    void prepare()  throws IOException, ExecutionException, InterruptedException {
	String sql2 = "SELECT d_next_o_id, d_tax FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id";
	var ph2 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("d_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("d_id").setType(CommonProtos.DataType.INT8))
	    .build();
	prepared2 = session.prepare(sql2, ph2).get();
    }
    
    void setParams() {
	paramsWid = randomGenerator.uniformWithin(1, profile.warehouses);
        paramsDid = randomGenerator.uniformWithin(1, Scale.DISTRICTS);  // scale::districts
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
		
		// SELECT d_next_o_id, d_tax FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id
		var ps2 = RequestProtos.ParameterSet.newBuilder()
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("d_w_id").setInt8Value(paramsWid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("d_id").setInt8Value(paramsDid))
		    .build();
		var future2 = transaction.executeQuery(prepared2, ps2);
		var resultSet2 = future2.get();
		now = System.nanoTime();
		profile.head += (now - prev);
		prev = now;
		try {
		    if (!Objects.isNull(resultSet2)) {
			if (!resultSet2.nextRecord()) {
			    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet2.getResponse().get().getResultCase())) {
				throw new ExecutionException(new IOException("SQL error"));
			    }
			    throw new ExecutionException(new IOException("no record"));
			}
			resultSet2.nextColumn();
			var dNextOid = resultSet2.getInt8();
			resultSet2.nextColumn();
			var dTax = resultSet2.getFloat8();
			if (resultSet2.nextRecord()) {
			    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet2.getResponse().get().getResultCase())) {
				throw new ExecutionException(new IOException("SQL error"));
			    }
			    throw new ExecutionException(new IOException("found multiple records"));
			}
		    }
		    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet2.getResponse().get().getResultCase())) {
			throw new ExecutionException(new IOException("SQL error"));
		    }
		} catch (ExecutionException e) {
		    if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(transaction.rollback().get().getResultCase())) {
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

        } catch (IOException | ExecutionException | InterruptedException | BrokenBarrierException e) {
            System.out.println(e);
	} finally {
	    try {
		prepared2.close();
		session.close();
	    } catch (IOException e) {
		System.out.println(e);
	    }
	}
    }
}
