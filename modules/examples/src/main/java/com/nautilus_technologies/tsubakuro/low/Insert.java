package com.nautilus_technologies.tsubakuro.low;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.ArrayDeque;
import java.util.Queue;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

public class Insert  extends Thread {
    Session session;
    Transaction transaction;
    PreparedStatement preparedStatement;
    long loop;
    long pendings;
    
    public Insert(Connector connector, Session session, long loop) throws IOException, ExecutionException, InterruptedException {
	this.session = session;
	this.session.connect(connector.connect().get());
	this.loop = loop;
	this.pendings = 0;
    }
    public Insert(Connector connector, Session session, long loop, long pendings) throws IOException, ExecutionException, InterruptedException {
	this.session = session;
	this.session.connect(connector.connect().get());
	this.loop = loop;
	this.pendings = pendings;
    }

    public void run() {
	try {
	    String sql = "INSERT INTO ORDERS (o_id, o_c_id, o_d_id, o_w_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES (:o_id, :o_c_id, :o_d_id, :o_w_id, :o_entry_d, :o_carrier_id, :o_ol_cnt, :o_all_local)";
	    var ph = RequestProtos.PlaceHolder.newBuilder()
		.addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_id").setType(CommonProtos.DataType.INT8))
		.addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_c_id").setType(CommonProtos.DataType.INT8))
		.addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_d_id").setType(CommonProtos.DataType.INT8))
		.addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_w_id").setType(CommonProtos.DataType.INT8))
		.addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_entry_d").setType(CommonProtos.DataType.STRING))
		.addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_carrier_id").setType(CommonProtos.DataType.INT8))
		.addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_ol_cnt").setType(CommonProtos.DataType.INT8))
		.addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_all_local").setType(CommonProtos.DataType.INT8));
	    preparedStatement = session.prepare(sql, ph).get();

	    transaction = session.createTransaction().get();
	    var ps = RequestProtos.ParameterSet.newBuilder()
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_id").setLValue(99999999))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_c_id").setLValue(1234))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_d_id").setLValue(3))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_w_id").setLValue(1))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_entry_d").setSValue("20210620"))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_carrier_id").setLValue(3))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_ol_cnt").setLValue(7))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_all_local").setLValue(0));

	    Queue<Future<ResponseProtos.ResultOnly>> queue = new ArrayDeque<>();
	    for (long i = 0; i < loop; i++) {
		queue.add(transaction.executeStatement(preparedStatement, ps));
		if (queue.size() > pendings) {
		    queue.poll().get();
		}
	    }
	    while (queue.size() > 0) {
		queue.poll().get();
	    }
	    transaction.commit().get();
	    preparedStatement.close();
	    session.close();
	} catch (IOException e) {
	    System.out.println(e);
	} catch (ExecutionException e) {
	    System.out.println(e);
	} catch (InterruptedException e) {
	    System.out.println(e);
        }
    }
}
