package com.nautilus_technologies.tsubakuro.low;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

public class Insert {
    Session session;
    PreparedStatement preparedStatement;
    
    public Insert(Connector connector, Session session) throws IOException, ExecutionException, InterruptedException {
	this.session = session;
	this.session.connect(connector.connect().get());
    }
    
    public void insert(String sql) throws IOException, ExecutionException, InterruptedException {
	Transaction transaction = session.createTransaction().get();
	transaction.executeStatement(sql).get();
	transaction.commit().get();
    }

    public void insert() throws IOException, ExecutionException, InterruptedException {
	insert("INSERT INTO ORDERS (o_id, o_c_id, o_d_id, o_w_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES (99999999, 1234, 3, 1, '20210620', 3, 7, 0)");
    }

    public void prepareAndInsert() throws IOException, ExecutionException, InterruptedException {
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

	Transaction transaction = session.createTransaction().get();
	var ps = RequestProtos.ParameterSet.newBuilder()
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_id").setLValue(99999999))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_c_id").setLValue(1234))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_d_id").setLValue(3))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_w_id").setLValue(1))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_entry_d").setSValue("20210620"))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_carrier_id").setLValue(3))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_ol_cnt").setLValue(7))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_all_local").setLValue(0));
	transaction.executeStatement(preparedStatement, ps).get();
	transaction.commit().get();
    }
}
