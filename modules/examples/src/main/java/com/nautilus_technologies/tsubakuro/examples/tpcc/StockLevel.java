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

public class StockLevel {
    Session session;
    Transaction transaction;
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
    static final long OID_RANGE = 20;

    public StockLevel(Session session, RandomGenerator randomGenerator, Profile profile) throws IOException, ServerException, InterruptedException {
	this.session = session;
	this.randomGenerator = randomGenerator;
	this.warehouses = profile.warehouses;
	this.profile = profile;
    }

    public void prepare() throws IOException, ServerException, InterruptedException {
	String sql1 = "SELECT d_next_o_id FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id";
	var ph1 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("d_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("d_id").setType(CommonProtos.DataType.INT8))
	    .build();
	prepared1 = session.prepare(sql1, ph1).get();

	String sql2 = "SELECT COUNT(DISTINCT s_i_id) FROM ORDER_LINE JOIN STOCK ON s_i_id = ol_i_id WHERE ol_w_id = :ol_w_id AND ol_d_id = :ol_d_id AND ol_o_id < :ol_o_id_high AND ol_o_id >= :ol_o_id_low AND s_w_id = :s_w_id AND s_quantity < :s_quantity";
	var ph2 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_o_id_high").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_o_id_low").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("s_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("s_quantity").setType(CommonProtos.DataType.INT8))
	    .build();
	prepared2 = session.prepare(sql2, ph2).get();
    }

    public void setParams() {
	if (profile.fixThreadMapping) {
	    long warehouseStep = warehouses / profile.threads;
	    paramsWid = randomGenerator.uniformWithin((profile.index * warehouseStep) + 1, (profile.index + 1) * warehouseStep);
	} else {
	    paramsWid = randomGenerator.uniformWithin(1, warehouses);
	}
	paramsDid = randomGenerator.uniformWithin(1, Scale.DISTRICTS);  // scale::districts
	paramsThreshold = randomGenerator.uniformWithin(10, 20);
    }

    void rollback() throws IOException, ServerException, InterruptedException {
	if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(transaction.rollback().get().getResultCase())) {
	    throw new IOException("error in rollback");
	}
	transaction = null;
    }

    public void transaction(AtomicBoolean stop) throws IOException, ServerException, InterruptedException {
	while (!stop.get()) {
	    profile.invocation.stockLevel++;
	    transaction = session.createTransaction().get();

	    // "SELECT d_next_o_id FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id"
	    var ps1 = RequestProtos.ParameterSet.newBuilder()
                .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("d_w_id").setInt8Value(paramsWid))
                .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("d_id").setInt8Value(paramsDid))
		.build();
	    var future1 = transaction.executeQuery(prepared1, ps1);
	    var resultSet1 = future1.get();
	    try {
		if (!Objects.isNull(resultSet1)) {
		    if (!resultSet1.nextRecord()) {
			if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet1.getResponse().get().getResultCase())) {
			    throw new IOException("SQL error");
			}
			throw new IOException("no record");
		    }
		    resultSet1.nextColumn();
		    oId = resultSet1.getInt8();
		    if (resultSet1.nextRecord()) {
			if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet1.getResponse().get().getResultCase())) {
			    throw new IOException("SQL error");
			}
			throw new IOException("found multiple records");
		    }
		}
		if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet1.getResponse().get().getResultCase())) {
		    throw new IOException("SQL error");
		}
	    } catch (ServerException e) {
                profile.retryOnStatement.stockLevel++;
                profile.districtTable.stockLevel++;
                rollback();
                continue;
	    } finally {
		if (!Objects.isNull(resultSet1)) {
		    resultSet1.close();
		    resultSet1 = null;
		}
	    }

	    // "SELECT COUNT(DISTINCT s_i_id) FROM ORDER_LINE JOIN STOCK ON s_i_id = ol_i_id WHERE ol_w_id = :ol_w_id AND ol_d_id = :ol_d_id AND ol_o_id < :ol_o_id_high AND ol_o_id >= :ol_o_id_low AND s_w_id = :s_w_id AND s_quantity < :s_quantity"
	    var ps2 = RequestProtos.ParameterSet.newBuilder()
                .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_w_id").setInt8Value(paramsWid))
                .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_d_id").setInt8Value(paramsDid))
                .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_o_id_high").setInt8Value(oId))
                .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_o_id_low").setInt8Value(oId - OID_RANGE))
                .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("s_w_id").setInt8Value(paramsWid))
                .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("s_quantity").setInt8Value(paramsThreshold))
		.build();
	    var future2 = transaction.executeQuery(prepared2, ps2);
	    var resultSet2 = future2.get();
	    try {
		if (!Objects.isNull(resultSet2)) {
		    if (!resultSet2.nextRecord()) {
			if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet2.getResponse().get().getResultCase())) {
			    throw new IOException("SQL error");
			}
			throw new IOException("no record");
		    }
		    resultSet2.nextColumn();
		    queryResult = resultSet2.getInt8();
		    if (resultSet2.nextRecord()) {
			if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet2.getResponse().get().getResultCase())) {
			    throw new IOException("SQL error");
			}
			throw new IOException("found multiple records");
		    }
		}
		if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(resultSet2.getResponse().get().getResultCase())) {
		    throw new IOException("SQL error");
		}
	    } catch (ServerException e) {
                profile.retryOnStatement.stockLevel++;
                profile.stockTable.stockLevel++;
                rollback();
                continue;
	    } finally {
		if (!Objects.isNull(resultSet2)) {
		    resultSet2.close();
		    resultSet2 = null;
		}
	    }

	    var commitResponse = transaction.commit().get();
	    if (ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(commitResponse.getResultCase())) {
                profile.completion.stockLevel++;
                return;
	    }
	    profile.retryOnCommit.stockLevel++;
	    transaction = null;
	}
    }
}
