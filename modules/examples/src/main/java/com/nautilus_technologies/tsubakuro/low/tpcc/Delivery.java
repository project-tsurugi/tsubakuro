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

public class Delivery {
    Session session;
    RandomGenerator randomGenerator;
    Profile profile;

    PreparedStatement prepared1;
    PreparedStatement prepared2;
    PreparedStatement prepared3;
    PreparedStatement prepared4;
    PreparedStatement prepared5;
    PreparedStatement prepared6;
    PreparedStatement prepared7;

    long warehouses;
    long paramsWid;
    long paramsOcarrierId;
    String paramsOlDeliveryD;

    //  local variables
    long noOid;
    long cId;
    double olTotal;

    public Delivery(Session session, RandomGenerator randomGenerator, Profile profile) throws IOException, ExecutionException, InterruptedException {
	this.session = session;
	this.randomGenerator = randomGenerator;
	this.warehouses = profile.warehouses;
	this.profile = profile;
    }

    public void prepare() throws IOException, ExecutionException, InterruptedException {
	String sql1 = "SELECT no_o_id FROM NEW_ORDER WHERE no_d_id = :no_d_id AND no_w_id = :no_w_id ORDER BY no_o_id";
	var ph1 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("no_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("no_w_id").setType(CommonProtos.DataType.INT8));
	prepared1 = session.prepare(sql1, ph1).get();

	String sql2 = "DELETE FROM NEW_ORDER WHERE no_d_id = :no_d_id AND no_w_id = :no_w_id AND no_o_id = :no_o_id";
	var ph2 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("no_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("no_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("no_o_id").setType(CommonProtos.DataType.INT8));
	prepared2 = session.prepare(sql2, ph2).get();

	String sql3 = "SELECT o_c_id FROM ORDERS WHERE o_id = :o_id AND o_d_id = :o_d_id AND o_w_id = :o_w_id";
	var ph3 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_w_id").setType(CommonProtos.DataType.INT8));
	prepared3 = session.prepare(sql3, ph3).get();

	String sql4 = "UPDATE ORDERS SET o_carrier_id = :o_carrier_id WHERE o_id = :o_id AND o_d_id = :o_d_id AND o_w_id = :o_w_id";
	var ph4 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_carrier_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_w_id").setType(CommonProtos.DataType.INT8));
	prepared4 = session.prepare(sql4, ph4).get();

	String sql5 = "UPDATE ORDER_LINE SET ol_delivery_d = :ol_delivery_d WHERE ol_o_id = :ol_o_id AND ol_d_id = :ol_d_id AND ol_w_id = :ol_w_id";
	var ph5 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_delivery_d").setType(CommonProtos.DataType.CHARACTER))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_o_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_w_id").setType(CommonProtos.DataType.INT8));
	prepared5 = session.prepare(sql5, ph5).get();

	String sql6 = "SELECT SUM(ol_amount) FROM ORDER_LINE WHERE ol_o_id = :ol_o_id AND ol_d_id = :ol_d_id AND ol_w_id = :ol_w_id";
	var ph6 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_o_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_w_id").setType(CommonProtos.DataType.INT8));
	prepared6 = session.prepare(sql6, ph6).get();

	String sql7 = "UPDATE CUSTOMER SET c_balance = c_balance + :ol_total WHERE c_id = :c_id AND c_d_id = :c_d_id AND c_w_id = :c_w_id";
	var ph7 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_total").setType(CommonProtos.DataType.FLOAT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_w_id").setType(CommonProtos.DataType.INT8));
	prepared7 = session.prepare(sql7, ph7).get();
    }

    public void setParams() {
	paramsWid = randomGenerator.uniformWithin(1, warehouses);  // FIXME warehouse_low, warehouse_high
	paramsOcarrierId = randomGenerator.uniformWithin(1, 10);
	paramsOlDeliveryD = NewOrder.timeStamp();
    }

    public long warehouseId() {
	return paramsWid;
    }

    public void transaction(Transaction transaction) throws IOException, ExecutionException, InterruptedException {
	profile.invocation.delivery++;
	while (true) {
	    for (long dId = 1; dId <= Scale.DISTRICTS; dId++) {
		// "SELECT no_o_id FROM NEW_ORDER WHERE no_d_id = :no_d_id AND no_w_id = :no_w_id ORDER BY no_o_id"
		var ps1 = RequestProtos.ParameterSet.newBuilder()
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("no_d_id").setInt8Value(dId))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("no_w_id").setInt8Value(paramsWid));
		var future1 = transaction.executeQuery(prepared1, ps1);
		var resultSet1 = future1.get();
		if (!resultSet1.nextRecord()) {
		    throw new IOException("no record");
		}
		resultSet1.nextColumn();
		noOid = resultSet1.getInt8();
		resultSet1.close();

		// "DELETE FROM NEW_ORDER WHERE no_d_id = :no_d_id AND no_w_id = :no_w_id AND no_o_id = :no_o_id"
		var ps2 = RequestProtos.ParameterSet.newBuilder()
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("no_d_id").setInt8Value(dId))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("no_w_id").setInt8Value(paramsWid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("no_o_id").setInt8Value(noOid));
		var future2 = transaction.executeStatement(prepared2, ps2);
		var result2 = future2.get();
		if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(result2.getResultCase())) {
		    throw new IOException("error in statement execution");
		}

		// "SELECT o_c_id FROM ORDERS WHERE o_id = :o_id AND o_d_id = :o_d_id AND o_w_id = :o_w_id"
		var ps3 = RequestProtos.ParameterSet.newBuilder()
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_id").setInt8Value(noOid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_d_id").setInt8Value(dId))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_w_id").setInt8Value(paramsWid));
		var future3 = transaction.executeQuery(prepared3, ps3);
		var resultSet3 = future3.get();
		if (!resultSet3.nextRecord()) {
		    throw new IOException("no record");
		}
		resultSet3.nextColumn();
		cId = resultSet3.getInt8();
		if (resultSet3.nextRecord()) {
		    throw new IOException("extra record");
		}
		resultSet3.close();

		// "UPDATE ORDERS SET o_carrier_id = :o_carrier_id WHERE o_id = :o_id AND o_d_id = :o_d_id AND o_w_id = :o_w_id"
		var ps4 = RequestProtos.ParameterSet.newBuilder()
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_carrier_id").setInt8Value(paramsOcarrierId))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_id").setInt8Value(noOid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_d_id").setInt8Value(dId))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_w_id").setInt8Value(paramsWid));
		var future4 = transaction.executeStatement(prepared4, ps4);
		var result4 = future4.get();
		if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(result4.getResultCase())) {
		    throw new IOException("error in statement execution");
		}

		// "UPDATE ORDER_LINE SET ol_delivery_d = :ol_delivery_d WHERE ol_o_id = :ol_o_id AND ol_d_id = :ol_d_id AND ol_w_id = :ol_w_id"
		var ps5 = RequestProtos.ParameterSet.newBuilder()
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_delivery_d").setCharacterValue(paramsOlDeliveryD))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_o_id").setInt8Value(noOid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_d_id").setInt8Value(dId))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_w_id").setInt8Value(paramsWid));
		var future5 = transaction.executeStatement(prepared5, ps5);
		var result5 = future5.get();
		if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(result5.getResultCase())) {
		    throw new IOException("error in statement execution");
		}

		// "SELECT o_c_id FROM ORDERS WHERE o_id = :o_id AND o_d_id = :o_d_id AND o_w_id = :o_w_id"
		var ps6 = RequestProtos.ParameterSet.newBuilder()
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_o_id").setInt8Value(noOid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_d_id").setInt8Value(dId))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_w_id").setInt8Value(paramsWid));
		var future6 = transaction.executeQuery(prepared6, ps6);
		var resultSet6 = future6.get();
		if (!resultSet6.nextRecord()) {
		    throw new IOException("no record");
		}
		resultSet6.nextColumn();
		olTotal = resultSet6.getFloat8();
		if (resultSet6.nextRecord()) {
		    throw new IOException("extra record");
		}
		resultSet6.close();

		// "UPDATE CUSTOMER SET c_balance = c_balance + :ol_total WHERE c_id = :c_id AND c_d_id = :c_d_id AND c_w_id = :c_w_id"
		var ps7 = RequestProtos.ParameterSet.newBuilder()
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_total").setFloat8Value(olTotal))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_id").setInt8Value(cId))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_d_id").setInt8Value(dId))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_w_id").setInt8Value(paramsWid));
		var future7 = transaction.executeStatement(prepared7, ps7);
		var result7 = future7.get();
		if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(result7.getResultCase())) {
		    throw new IOException("error in statement execution");
		}
	    }
	    var commitResponse = transaction.commit().get();
	    if (ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(commitResponse.getResultCase())) {
		profile.completion.delivery++;
		break;
	    }
	    profile.retry.delivery++;
	}
    }
}
