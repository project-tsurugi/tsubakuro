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

public class OrderStatus {
    Session session;
    RandomGenerator randomGenerator;

    PreparedStatement prepared1;
    PreparedStatement prepared2;
    PreparedStatement prepared3;
    PreparedStatement prepared4;
    PreparedStatement prepared5;
    PreparedStatement prepared6;
    PreparedStatement prepared7;
    
    long warehouses;
    long paramsWid;
    long paramsDid;
    long paramsCid;
    boolean paramsByName;
    String paramsClast;

    //  local variables
    long cId;
    String cFirst;
    String cMiddle;
    String cLast;
    long oId;
    long oCarrierId;
    String oEntryD;
    long oOlCnt;
    double cBalance;
    long[] olIid;
    long[] olSupplyWid;
    long[] olQuantity;
    long[] olAmount;
    String[] olDeliveryD;

    public OrderStatus(Session session, RandomGenerator randomGenerator, long warehouses) throws IOException, ExecutionException, InterruptedException {
	this.session = session;
	this.randomGenerator = randomGenerator;
	this.warehouses = warehouses;

	int kOlMax = (int) Scale.maxOlCount();
	olIid = new long[kOlMax];
	olSupplyWid = new long[kOlMax];
	olQuantity = new long[kOlMax];
	olAmount = new long[kOlMax];
	olDeliveryD = new String[kOlMax];
    }

    public void prepare() throws IOException, ExecutionException, InterruptedException {
	String sql1 = "SELECT COUNT(c_id) FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_last = :c_last";
	var ph1 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_last").setType(CommonProtos.DataType.CHARACTER));
	prepared1 = session.prepare(sql1, ph1).get();

	String sql2 = "SELECT c_id FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_last = :c_last  ORDER by c_first";
	var ph2 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_last").setType(CommonProtos.DataType.CHARACTER));
	prepared2 = session.prepare(sql2, ph2).get();

	String sql3 = "SELECT c_balance, c_first, c_middle, c_last FROM CUSTOMER WHERE c_id = :c_id AND c_d_id = :c_d_id AND c_w_id = :c_w_id";
	var ph3 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_w_id").setType(CommonProtos.DataType.INT8));
	prepared3 = session.prepare(sql3, ph3).get();

	String sql4 = "SELECT o_id FROM ORDERS WHERE o_w_id = :o_w_id AND o_d_id = :o_d_id AND o_c_id = :o_c_id ORDER by o_id DESC";
	var ph4 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_c_id").setType(CommonProtos.DataType.INT8));
	prepared4 = session.prepare(sql4, ph4).get();

	String sql5 = "SELECT o_carrier_id, o_entry_d, o_ol_cnt FROM ORDERS WHERE o_w_id = :o_w_id AND o_d_id = :o_d_id AND o_id = :o_id";
	var ph5 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_id").setType(CommonProtos.DataType.INT8));
	prepared5 = session.prepare(sql5, ph5).get();

	String sql6 = "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM ORDER_LINE WHERE ol_o_id = :ol_o_id AND ol_d_id = :ol_d_id AND ol_w_id = :ol_w_id";
	var ph6 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_o_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_w_id").setType(CommonProtos.DataType.INT8));
	prepared6 = session.prepare(sql6, ph6).get();
    }

    void setParams() {
	paramsWid = randomGenerator.uniformWithin(1, warehouses);  // FIXME warehouse_low, warehouse_high
	paramsDid = randomGenerator.uniformWithin(1, Scale.districts());  // scale::districts
	paramsByName = randomGenerator.uniformWithin(1, 100) <= 60;
	if (paramsByName) {
	    paramsClast = Payment.lastName((int) randomGenerator.nonUniformWithin(255, 0, Scale.lnames() - 1));  // scale::lnames
	} else {
	    paramsCid = randomGenerator.nonUniformWithin(1023, 1, Scale.customers());  // scale::customers
	}
    }

    public void transaction() throws IOException, ExecutionException, InterruptedException {
	setParams();

	var transaction = session.createTransaction().get();

    	if (!paramsByName) {
            cId = paramsCid;
	} else {
	    cId = Customer.chooseCustomer(transaction, prepared1, prepared2, paramsWid, paramsDid, paramsClast);
	}

	if (cId != 0) {
	    // "SELECT c_balance, c_first, c_middle, c_last FROM CUSTOMER WHERE c_id = :c_id AND c_d_id = :c_d_id AND c_w_id = :c_w_id"
	    var ps3 = RequestProtos.ParameterSet.newBuilder()
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_id").setInt8Value(cId))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_d_id").setInt8Value(paramsDid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_w_id").setInt8Value(paramsWid));
	    var future3 = transaction.executeQuery(prepared3, ps3);
	    var resultSet3 = future3.get();
	    if (!resultSet3.nextRecord()) {
		throw new IOException("no record");
	    }
	    resultSet3.nextColumn();
	    cBalance = resultSet3.getFloat8();
	    resultSet3.nextColumn();
	    cFirst = resultSet3.getCharacter();
	    resultSet3.nextColumn();
	    cMiddle = resultSet3.getCharacter();
	    resultSet3.nextColumn();
	    cLast = resultSet3.getCharacter();
	    if (resultSet3.nextRecord()) {
		throw new IOException("extra record");
	    }
	    resultSet3.close();
	    
	    // "SELECT o_id FROM ORDERS WHERE o_w_id = :o_w_id AND o_d_id = :o_d_id AND o_c_id = :o_c_id ORDER by o_id DESC"
	    var ps4 = RequestProtos.ParameterSet.newBuilder()
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_d_id").setInt8Value(paramsDid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_w_id").setInt8Value(paramsWid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_c_id").setInt8Value(cId));
	    var future4 = transaction.executeQuery(prepared4, ps4);
	    var resultSet4 = future4.get();
	    if (!resultSet4.nextRecord()) {
		throw new IOException("no record");
	    }
	    resultSet4.nextColumn();
	    oId = resultSet4.getInt8();
	    if (resultSet4.nextRecord()) {
		throw new IOException("extra record");
	    }
	    resultSet4.close();
	    
	    // "SELECT o_carrier_id, o_entry_d, o_ol_cnt FROM ORDERS WHERE o_w_id = :o_w_id AND o_d_id = :o_d_id AND o_id = :o_id"
	    var ps5 = RequestProtos.ParameterSet.newBuilder()
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_d_id").setInt8Value(paramsDid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_w_id").setInt8Value(paramsWid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_id").setInt8Value(oId));
	    var future5 = transaction.executeQuery(prepared5, ps5);
	    var resultSet5 = future5.get();
	    if (!resultSet5.nextRecord()) {
		throw new IOException("no record");
	    }
	    resultSet5.nextColumn();
	    oCarrierId = resultSet5.getInt8();
	    resultSet5.nextColumn();
	    oEntryD = resultSet5.getCharacter();
	    resultSet5.nextColumn();
	    oOlCnt = resultSet5.getInt8();
	    if (resultSet5.nextRecord()) {
		throw new IOException("extra record");
	    }
	    resultSet5.close();
	    
	    // "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM ORDER_LINE WHERE ol_o_id = :ol_o_id AND ol_d_id = :ol_d_id AND ol_w_id = :ol_w_id"
	    var ps6 = RequestProtos.ParameterSet.newBuilder()
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_o_id").setInt8Value(oId))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_d_id").setInt8Value(paramsDid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_w_id").setInt8Value(paramsWid));
	    var future6 = transaction.executeQuery(prepared6, ps6);
	    var resultSet6 = future6.get();
	    int i = 0;
	    while (resultSet6.nextRecord()) {
		resultSet6.nextColumn();
		olIid[i] = resultSet6.getInt8();
		resultSet6.nextColumn();
		olSupplyWid[i] = resultSet6.getInt8();
		resultSet6.nextColumn();
		olQuantity[i] = resultSet6.getInt8();
		resultSet6.nextColumn();
		olAmount[i] = resultSet6.getInt8();
		resultSet6.nextColumn();
		olDeliveryD[i] = resultSet6.getCharacter();
		i++;
	    }
	    resultSet6.close();
	}

	transaction.commit().get();
    }
}
