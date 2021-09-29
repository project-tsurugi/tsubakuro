package com.nautilus_technologies.tsubakuro.low.tpcc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.Date;
import java.util.Locale;
import java.text.SimpleDateFormat;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

public class StockLevel {
    Session session;
    RandomGenerator randomGenerator;
    Profile profile;

    PreparedStatement prepared1;
    PreparedStatement prepared2;

    long warehouses;
    long paramsWid;
    long paramsDid;
    long paramsThreshold;

    //  local variables
    long oId;
    long queryResult;
    final long oidRange = 20;

    public StockLevel(Session session, RandomGenerator randomGenerator, Profile profile) throws IOException, ExecutionException, InterruptedException {
	this.session = session;
	this.randomGenerator = randomGenerator;
	this.warehouses = profile.warehouses;
	this.profile = profile;
    }

    public void prepare() throws IOException, ExecutionException, InterruptedException {
	String sql1 = "SELECT d_next_o_id FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id";
	var ph1 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("d_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("d_id").setType(CommonProtos.DataType.INT8));
	prepared1 = session.prepare(sql1, ph1).get();

	String sql2 = "SELECT COUNT(DISTINCT s_i_id) FROM ORDER_LINE JOIN STOCK ON s_i_id = ol_i_id WHERE ol_w_id = :ol_w_id AND ol_d_id = :ol_d_id AND ol_o_id < :ol_o_id_high AND ol_o_id >= :ol_o_id_low AND s_w_id = :s_w_id AND s_quantity < :s_quantity";
	var ph2 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_o_id_high").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_o_id_low").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("s_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("s_quantity").setType(CommonProtos.DataType.INT8));
	prepared2 = session.prepare(sql2, ph2).get();
    }

    public void setParams() {
	paramsWid = randomGenerator.uniformWithin(1, warehouses);  // FIXME warehouse_low, warehouse_high
	paramsDid = randomGenerator.uniformWithin(1, Scale.DISTRICTS);  // scale::districts
	paramsThreshold = randomGenerator.uniformWithin(10, 20);
    }

    public boolean transaction(Transaction transaction) throws IOException, ExecutionException, InterruptedException {
	profile.invocation.stockLevel++;
	// "SELECT d_next_o_id FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id"
	var ps1 = RequestProtos.ParameterSet.newBuilder()
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("d_w_id").setInt8Value(paramsWid))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("d_id").setInt8Value(paramsDid));
	var future1 = transaction.executeQuery(prepared1, ps1);
	try {
	    var resultSet1 = future1.get();
	    if (!resultSet1.nextRecord()) {
		throw new IOException("no record");
	    }
	    resultSet1.nextColumn();
	    oId = resultSet1.getInt8();
	    if (resultSet1.nextRecord()) {
		throw new IOException("extra record");
	    }
	    resultSet1.close();
	} catch (ExecutionException e) {
		throw new IOException(e);
	}

	// "SELECT COUNT(DISTINCT s_i_id) FROM ORDER_LINE JOIN STOCK ON s_i_id = ol_i_id WHERE ol_w_id = :ol_w_id AND ol_d_id = :ol_d_id AND ol_o_id < :ol_o_id_high AND ol_o_id >= :ol_o_id_low AND s_w_id = :s_w_id AND s_quantity < :s_quantity"
	var ps2 = RequestProtos.ParameterSet.newBuilder()
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_w_id").setInt8Value(paramsWid))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_d_id").setInt8Value(paramsDid))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_o_id_high").setInt8Value(oId))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_o_id_low").setInt8Value(oId - oidRange))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("s_w_id").setInt8Value(paramsWid))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("s_quantity").setInt8Value(paramsThreshold));
	var future2 = transaction.executeQuery(prepared2, ps2);
	try {
	    var resultSet2 = future2.get();
	    if (!resultSet2.nextRecord()) {
		throw new IOException("no record");
	    }
	    resultSet2.nextColumn();
	    queryResult = resultSet2.getInt8();
	    if (resultSet2.nextRecord()) {
		throw new IOException("extra record");
	    }
	    resultSet2.close();
	} catch (ExecutionException e) {
	    throw new IOException(e);
	}

	var commitResponse = transaction.commit().get();
	if (ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(commitResponse.getResultCase())) {
	    profile.completion.stockLevel++;
	    return true;
	}
	profile.retryOnCommit.stockLevel++;
	return false;
    }
}
