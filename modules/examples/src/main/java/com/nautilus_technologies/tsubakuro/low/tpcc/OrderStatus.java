package com.nautilus_technologies.tsubakuro.low.tpcc;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.StatusProtos;

public class OrderStatus {
    Session session;
    Transaction transaction;
    RandomGenerator randomGenerator;
    Profile profile;

    PreparedStatement prepared1;
    PreparedStatement prepared2;
    PreparedStatement prepared3;
    PreparedStatement prepared4;
    PreparedStatement prepared5;
    PreparedStatement prepared6;

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
    double[] olAmount;
    String[] olDeliveryD;

    public OrderStatus(Session session, RandomGenerator randomGenerator, Profile profile) throws IOException, ServerException, InterruptedException {
	this.session = session;
	this.randomGenerator = randomGenerator;
	this.warehouses = profile.warehouses;
	this.profile = profile;

	int kOlMax = (int) Scale.MAX_OL_COUNT;
	olIid = new long[kOlMax];
	olSupplyWid = new long[kOlMax];
	olQuantity = new long[kOlMax];
	olAmount = new double[kOlMax];
	olDeliveryD = new String[kOlMax];
    }

    public void prepare() throws IOException, ServerException, InterruptedException {
	String sql1 = "SELECT COUNT(c_id) FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_last = :c_last";
	var ph1 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_last").setType(CommonProtos.DataType.CHARACTER))
	    .build();
	prepared1 = session.prepare(sql1, ph1).get();

	String sql2 = "SELECT c_id FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_last = :c_last  ORDER by c_first";
	var ph2 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_last").setType(CommonProtos.DataType.CHARACTER))
	    .build();
	prepared2 = session.prepare(sql2, ph2).get();

	String sql3 = "SELECT c_balance, c_first, c_middle, c_last FROM CUSTOMER WHERE c_id = :c_id AND c_d_id = :c_d_id AND c_w_id = :c_w_id";
	var ph3 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_w_id").setType(CommonProtos.DataType.INT8))
	    .build();
	prepared3 = session.prepare(sql3, ph3).get();

	String sql4 = "SELECT o_id FROM ORDERS WHERE o_w_id = :o_w_id AND o_d_id = :o_d_id AND o_c_id = :o_c_id ORDER by o_id DESC";
	var ph4 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_c_id").setType(CommonProtos.DataType.INT8))
	    .build();
	prepared4 = session.prepare(sql4, ph4).get();

	String sql5 = "SELECT o_carrier_id, o_entry_d, o_ol_cnt FROM ORDERS WHERE o_w_id = :o_w_id AND o_d_id = :o_d_id AND o_id = :o_id";
	var ph5 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_id").setType(CommonProtos.DataType.INT8))
	    .build();
	prepared5 = session.prepare(sql5, ph5).get();

	String sql6 = "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM ORDER_LINE WHERE ol_o_id = :ol_o_id AND ol_d_id = :ol_d_id AND ol_w_id = :ol_w_id";
	var ph6 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_o_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_w_id").setType(CommonProtos.DataType.INT8))
	    .build();
	prepared6 = session.prepare(sql6, ph6).get();
    }

    public void setParams() {
	if (profile.fixThreadMapping) {
	    long warehouseStep = warehouses / profile.threads;
	    paramsWid = randomGenerator.uniformWithin((profile.index * warehouseStep) + 1, (profile.index + 1) * warehouseStep);
	} else {
	    paramsWid = randomGenerator.uniformWithin(1, warehouses);
	}
	paramsDid = randomGenerator.uniformWithin(1, Scale.DISTRICTS);  // scale::districts
	paramsByName = randomGenerator.uniformWithin(1, 100) <= 60;
	if (paramsByName) {
	    paramsClast = Payment.lastName((int) randomGenerator.nonUniform255Within(0, Scale.L_NAMES - 1));  // scale::lnames
	} else {
	    paramsCid = randomGenerator.nonUniform1023Within(1, Scale.CUSTOMERS);  // scale::customers
	}
    }

    void rollback() throws IOException, ServerException, InterruptedException {
	if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(transaction.rollback().get().getResultCase())) {
	    throw new IOException("error in rollback");
	}
	transaction = null;
    }

    public void transaction(AtomicBoolean stop) throws IOException, ServerException, InterruptedException {
	while (!stop.get()) {
	    transaction = session.createTransaction().get();
	    profile.invocation.orderStatus++;
	    if (!paramsByName) {
                cId = paramsCid;
	    } else {
                cId = Customer.chooseCustomer(transaction, prepared1, prepared2, paramsWid, paramsDid, paramsClast);
                if (cId < 0) {
                    profile.retryOnStatement.orderStatus++;
                    profile.customerTable.orderStatus++;
                    rollback();
                    continue;
                }
	    }
	    if (cId != 0) {
		// "SELECT c_balance, c_first, c_middle, c_last FROM CUSTOMER WHERE c_id = :c_id AND c_d_id = :c_d_id AND c_w_id = :c_w_id"
		var ps3 = RequestProtos.ParameterSet.newBuilder()
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_id").setInt8Value(cId))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_d_id").setInt8Value(paramsDid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_w_id").setInt8Value(paramsWid))
		    .build();
		var future3 = transaction.executeQuery(prepared3, ps3.getParametersList());
		var resultSet3 = future3.get();
		try {
		    if (Objects.nonNull(resultSet3)) {
			if (!resultSet3.nextRecord()) {
			    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet3.getResponse().get().getResultCase())) {
				throw new IOException("SQL error");
			    }
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
			    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet3.getResponse().get().getResultCase())) {
				throw new IOException("SQL error");
			    }
			    throw new IOException("found multiple records");
			}
		    }
		    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet3.getResponse().get().getResultCase())) {
			throw new IOException("SQL error");
		    }
                } catch (ServerException e) {
                    profile.retryOnStatement.orderStatus++;
                    profile.ordersTable.orderStatus++;
                    rollback();
                    continue;
                } finally {
		    if (Objects.nonNull(resultSet3)) {
			resultSet3.close();
			resultSet3 = null;
		    }
                }

		// "SELECT o_id FROM ORDERS WHERE o_w_id = :o_w_id AND o_d_id = :o_d_id AND o_c_id = :o_c_id ORDER by o_id DESC"
		var ps4 = RequestProtos.ParameterSet.newBuilder()
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_d_id").setInt8Value(paramsDid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_w_id").setInt8Value(paramsWid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_c_id").setInt8Value(cId))
		    .build();
		var future4 = transaction.executeQuery(prepared4, ps4.getParametersList());
		var resultSet4 = future4.get();
		try {
		    if (Objects.nonNull(resultSet4)) {
			if (!resultSet4.nextRecord()) {
			    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet4.getResponse().get().getResultCase())) {
				throw new IOException("SQL error");
			    }
			    throw new IOException("no record");
			}
			resultSet4.nextColumn();
			oId = resultSet4.getInt8();
		    }
		    var status4 = resultSet4.getResponse().get();
		    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(status4.getResultCase())) {
			if (status4.getError().getStatus() == StatusProtos.Status.ERR_INCONSISTENT_INDEX) {
			    if (profile.inconsistentIndexCount == 0) {
				System.out.println("inconsistent_index");
			    }
			    profile.inconsistentIndexCount++;
			}
			throw new IOException("SQL error");
		    }
                } catch (ServerException e) {
                    profile.retryOnStatement.orderStatus++;
                    profile.ordersTable.orderStatus++;
                    rollback();
                    continue;
                } finally {
		    if (Objects.nonNull(resultSet4)) {
			resultSet4.close();
			resultSet4 = null;
		    }
                }

		// "SELECT o_carrier_id, o_entry_d, o_ol_cnt FROM ORDERS WHERE o_w_id = :o_w_id AND o_d_id = :o_d_id AND o_id = :o_id"
		var ps5 = RequestProtos.ParameterSet.newBuilder()
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_d_id").setInt8Value(paramsDid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_w_id").setInt8Value(paramsWid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_id").setInt8Value(oId))
		    .build();
		var future5 = transaction.executeQuery(prepared5, ps5.getParametersList());
		var resultSet5 = future5.get();
		try {
		    if (Objects.nonNull(resultSet5)) {
			if (!resultSet5.nextRecord()) {
			    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet5.getResponse().get().getResultCase())) {
				throw new IOException("SQL error");
			    }
			    throw new IOException("no record");
			}
			resultSet5.nextColumn();
			if (!resultSet5.isNull()) {
			    oCarrierId = resultSet5.getInt8();
			}
			resultSet5.nextColumn();
			oEntryD = resultSet5.getCharacter();
			resultSet5.nextColumn();
			oOlCnt = resultSet5.getInt8();
			if (resultSet5.nextRecord()) {
			    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet5.getResponse().get().getResultCase())) {
				throw new IOException("SQL error");
			    }
			    throw new IOException("found multiple records");
			}
		    }
		    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet5.getResponse().get().getResultCase())) {
			throw new IOException("SQL error");
		    }
                } catch (ServerException e) {
                    profile.retryOnStatement.orderStatus++;
                    profile.ordersTable.orderStatus++;
                    rollback();
                    continue;
                } finally {
		    if (Objects.nonNull(resultSet5)) {
			resultSet5.close();
			resultSet5 = null;
		    }
                }

		// "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM ORDER_LINE WHERE ol_o_id = :ol_o_id AND ol_d_id = :ol_d_id AND ol_w_id = :ol_w_id"
		var ps6 = RequestProtos.ParameterSet.newBuilder()
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_o_id").setInt8Value(oId))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_d_id").setInt8Value(paramsDid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_w_id").setInt8Value(paramsWid))
		    .build();
		var future6 = transaction.executeQuery(prepared6, ps6.getParametersList());
		var resultSet6 = future6.get();
		try {
		    if (Objects.nonNull(resultSet6)) {
			int i = 0;
			while (resultSet6.nextRecord()) {
			    resultSet6.nextColumn();
			    olIid[i] = resultSet6.getInt8();
			    resultSet6.nextColumn();
			    olSupplyWid[i] = resultSet6.getInt8();
			    resultSet6.nextColumn();
			    olQuantity[i] = resultSet6.getInt8();
			    resultSet6.nextColumn();
			    olAmount[i] = resultSet6.getFloat8();
			    resultSet6.nextColumn();
			    if (!resultSet6.isNull()) {
				olDeliveryD[i] = resultSet6.getCharacter();
			    }
			    i++;
			}
		    }
		    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet6.getResponse().get().getResultCase())) {
			throw new IOException("SQL error");
		    }
                } catch (ServerException e) {
                    profile.retryOnStatement.orderStatus++;
                    profile.ordersTable.orderStatus++;
                    rollback();
                    continue;
                } catch (ArrayIndexOutOfBoundsException e) {
                    System.out.println(e + ": ol_o_id = " + oId + ", ol_d_id = " + paramsDid + ", ol_w_id = " + paramsWid);
                    rollback();
                    continue;
                } finally {
		    if (Objects.nonNull(resultSet6)) {
			resultSet6.close();
			resultSet6 = null;
		    }
                }
	    }

	    var commitResponse = transaction.commit().get();
	    if (ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(commitResponse.getResultCase())) {
                profile.completion.orderStatus++;
                return;
	    }
	    profile.retryOnCommit.orderStatus++;
	    transaction = null;
	}
    }
}
