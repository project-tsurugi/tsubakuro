package com.nautilus_technologies.tsubakuro.low.tpcc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Objects;
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

public class NewOrder {
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
    PreparedStatement prepared7;
    PreparedStatement prepared8;
    PreparedStatement prepared9;

    long warehouses;
    long paramsWid;
    long paramsDid;
    long paramsCid;
    long paramsOlCnt;
    boolean paramsRemoteWarehouse;
    long paramsSupplyWid;
    long[] paramsQty;
    long[] paramsItemId;
    boolean paramsWillRollback;
    String paramsEntryD;
    long paramsAllLocal;

    long[] stock;
    String[] bg;
    double[] amt;
    double total;
    double[] price;
    String[] iNames;

    //  local variables
    double wTax;
    double cDiscount;
    String cLast;
    String cCredit;
    long dNextOid;
    double dTax;
    long oid;
    double iPrice;
    String iName;
    String iData;
    long sQuantity;
    String sData;
    String[] sDistData;

    public NewOrder(Session session, RandomGenerator randomGenerator, Profile profile) throws IOException, ExecutionException, InterruptedException {
	this.session = session;
	this.randomGenerator = randomGenerator;
	this.warehouses = profile.warehouses;
	this.profile = profile;
	this.paramsQty = new long[15];
	this.paramsItemId = new long[15];

	this.stock = new long[15];
	this.bg = new String[15];
	this.amt = new double[15];
	this.price = new double[15];
	this.iNames = new String[15];
	this.sDistData = new String[10];
    }

    public void prepare() throws IOException, ExecutionException, InterruptedException {
	String sql1 = "SELECT w_tax, c_discount, c_last, c_credit FROM WAREHOUSE, CUSTOMER WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :c_d_id AND c_id = :c_id";
	var ph1 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("c_id").setType(CommonProtos.DataType.INT8));
	prepared1 = session.prepare(sql1, ph1).get();

	String sql2 = "SELECT d_next_o_id, d_tax FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id";
	var ph2 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("d_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("d_id").setType(CommonProtos.DataType.INT8));
	prepared2 = session.prepare(sql2, ph2).get();

	String sql3 = "UPDATE DISTRICT SET d_next_o_id = :d_next_o_id WHERE d_w_id = :d_w_id AND d_id = :d_id";
	var ph3 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("d_next_o_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("d_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("d_id").setType(CommonProtos.DataType.INT8));
	prepared3 = session.prepare(sql3, ph3).get();

	String sql4 = "INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (:o_id, :o_d_id, :o_w_id, :o_c_id, :o_entry_d, :o_ol_cnt, :o_all_local)";
	var ph4 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_c_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_entry_d").setType(CommonProtos.DataType.CHARACTER))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_ol_cnt").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("o_all_local").setType(CommonProtos.DataType.INT8));
	prepared4 = session.prepare(sql4, ph4).get();

	String sql5 = "INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)VALUES (:no_o_id, :no_d_id, :no_w_id)";
	var ph5 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("no_o_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("no_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("no_w_id").setType(CommonProtos.DataType.INT8));
	prepared5 = session.prepare(sql5, ph5).get();

	String sql6 = "SELECT i_price, i_name , i_data FROM ITEM WHERE i_id = :i_id";
	var ph6 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("i_id").setType(CommonProtos.DataType.INT8));
	prepared6 = session.prepare(sql6, ph6).get();

	String sql7 = "SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM STOCK WHERE s_i_id = :s_i_id AND s_w_id = :s_w_id";
	var ph7 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("s_i_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("s_w_id").setType(CommonProtos.DataType.INT8));
	prepared7 = session.prepare(sql7, ph7).get();

	String sql8 = "UPDATE STOCK SET s_quantity = :s_quantity WHERE s_i_id = :s_i_id AND s_w_id = :s_w_id";
	var ph8 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("s_quantity").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("s_i_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("s_w_id").setType(CommonProtos.DataType.INT8));
	prepared8 = session.prepare(sql8, ph8).get();

	String sql9 = "INSERT INTO ORDER_LINE (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)VALUES (:ol_o_id, :ol_d_id, :ol_w_id, :ol_number, :ol_i_id, :ol_supply_w_id, :ol_quantity, :ol_amount, :ol_dist_info)";
	var ph9 = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_o_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_d_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_number").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_i_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_supply_w_id").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_quantity").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_amount").setType(CommonProtos.DataType.FLOAT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("ol_dist_info").setType(CommonProtos.DataType.CHARACTER));
	prepared9 = session.prepare(sql9, ph9).get();
    }

    static String timeStamp() {
	Date date = new Date();
	SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy", new Locale("US"));
	return dateFormat.format(date);
    }

    public void setParams() {
	paramsWid = randomGenerator.uniformWithin(1, warehouses);  // FIXME warehouse_low, warehouse_high
	paramsDid = randomGenerator.uniformWithin(1, Scale.DISTRICTS);  // scale::districts
	paramsCid = randomGenerator.uniformWithin(1, Scale.CUSTOMERS);  // scale::customers
	paramsOlCnt = randomGenerator.uniformWithin(Scale.MIN_OL_COUNT, Scale.MAX_OL_COUNT); // scale::min_ol_count, scale::max_ol_count

	paramsRemoteWarehouse = (randomGenerator.uniformWithin(1, 100) <= Percent.K_NEW_ORDER_REMOTE); //kNewOrderRemotePercent
	if (paramsRemoteWarehouse && warehouses > 1) {
	    do {
                paramsSupplyWid = randomGenerator.uniformWithin(1, warehouses);
	    } while (paramsSupplyWid != paramsWid);
	    paramsAllLocal = 0;
	} else {
	    paramsSupplyWid = paramsWid;
	    paramsAllLocal = 1;
	}

	for (int ol = 1; ol <= paramsOlCnt; ++ol) {
	    paramsQty[ol - 1] = randomGenerator.uniformWithin(1, 10);
	    paramsItemId[ol - 1] = randomGenerator.nonUniform8191Within(1, Scale.ITEMS); // scale::items
	}
	paramsEntryD = timeStamp();
	paramsWillRollback = (randomGenerator.uniformWithin(1, 100) == 1);
    }

    void rollback() throws IOException, ExecutionException, InterruptedException {
	if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(transaction.rollback().get().getResultCase())) {
	    throw new IOException("error in rollback");
	}
	transaction = null;
    }

    public void transaction(AtomicBoolean stop) throws IOException, ExecutionException, InterruptedException {
	while (!stop.get()) {
	    transaction = session.createTransaction().get();
	    profile.invocation.newOrder++;
	    total = 0;

	    // SELECT w_tax, c_discount, c_last, c_credit FROM WAREHOUSE, CUSTOMER WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :c_d_id AND c_id = :c_id;
	    var ps1 = RequestProtos.ParameterSet.newBuilder()
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("w_id").setInt8Value(paramsWid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_d_id").setInt8Value(paramsDid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("c_id").setInt8Value(paramsCid));
	    var future1 = transaction.executeQuery(prepared1, ps1);
	    var resultSet1 = future1.getLeft().get();
	    try {
		if (!Objects.isNull(resultSet1)) {
		    if (!resultSet1.nextRecord()) {
			throw new ExecutionException(new IOException("no record"));
		    }
		    resultSet1.nextColumn();
		    wTax = resultSet1.getFloat8();
		    resultSet1.nextColumn();
		    cDiscount = resultSet1.getFloat8();
		    resultSet1.nextColumn();
		    cLast = resultSet1.getCharacter();
		    resultSet1.nextColumn();
		    cCredit = resultSet1.getCharacter();
		    if (resultSet1.nextRecord()) {
			future1.getRight().get();
			throw new ExecutionException(new IOException("found multiple records"));
		    }
		}
		if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(future1.getRight().get().getResultCase())) {
		    throw new ExecutionException(new IOException("SQL error"));
		}
	    } catch (ExecutionException e) {
		profile.retryOnStatement.newOrder++;
		profile.customerTable.newOrder++;
		rollback();
		continue;
	    } finally {
		if (!Objects.isNull(resultSet1)) {
		    resultSet1.close();
		}
	    }

	    // SELECT d_next_o_id, d_tax FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id
	    var ps2 = RequestProtos.ParameterSet.newBuilder()
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("d_w_id").setInt8Value(paramsWid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("d_id").setInt8Value(paramsDid));
	    var future2 = transaction.executeQuery(prepared2, ps2);
	    var resultSet2 = future2.getLeft().get();
	    try {
		if (!Objects.isNull(resultSet2)) {
		    if (!resultSet2.nextRecord()) {
			future2.getRight().get();
			throw new ExecutionException(new IOException("no record"));
		    }
		    resultSet2.nextColumn();
		    dNextOid = resultSet2.getInt8();
		    resultSet2.nextColumn();
		    dTax = resultSet2.getFloat8();
		    if (resultSet2.nextRecord()) {
			future2.getRight().get();
			throw new ExecutionException(new IOException("found multiple records"));
		    }
		}
		if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(future2.getRight().get().getResultCase())) {
		    throw new ExecutionException(new IOException("SQL error"));
		}
	    } catch (ExecutionException e) {
		profile.retryOnStatement.newOrder++;
		profile.districtTable.newOrder++;
		rollback();
		continue;
	    } finally {
		if (!Objects.isNull(resultSet2)) {
		    resultSet2.close();
		}
	    }

	    // UPDATE DISTRICT SET d_next_o_id = :d_next_o_id WHERE d_w_id = :d_w_id AND d_id = :d_id
	    var ps3 = RequestProtos.ParameterSet.newBuilder()
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("d_next_o_id").setInt8Value(dNextOid + 1))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("d_w_id").setInt8Value(paramsWid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("d_id").setInt8Value(paramsDid));
	    var future3 = transaction.executeStatement(prepared3, ps3);
	    var result3 = future3.get();
	    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(result3.getResultCase())) {
		profile.retryOnStatement.newOrder++;
		profile.districtTable.newOrder++;
		rollback();
		continue;
	    }

	    oid = dNextOid;

	    // INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (:o_id, :o_d_id, :o_w_id, :o_c_id, :o_entry_d, :o_ol_cnt, :o_all_local
	    var ps4 = RequestProtos.ParameterSet.newBuilder()
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_id").setInt8Value(oid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_d_id").setInt8Value(paramsDid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_w_id").setInt8Value(paramsWid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_c_id").setInt8Value(paramsCid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_entry_d").setCharacterValue(paramsEntryD))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_ol_cnt").setInt8Value(paramsOlCnt))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("o_all_local").setInt8Value(paramsAllLocal));
	    var future4 = transaction.executeStatement(prepared4, ps4);
	    var result4 = future4.get();
	    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(result4.getResultCase())) {
		profile.retryOnStatement.newOrder++;
		profile.ordersTable.newOrder++;
		rollback();
		continue;
	    }

	    // INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)VALUES (:no_o_id, :no_d_id, :no_w_id
	    var ps5 = RequestProtos.ParameterSet.newBuilder()
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("no_o_id").setInt8Value(oid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("no_d_id").setInt8Value(paramsDid))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("no_w_id").setInt8Value(paramsWid));
	    var future5 = transaction.executeStatement(prepared5, ps5);
	    var result5 = future5.get();
	    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(result5.getResultCase())) {
		profile.retryOnStatement.newOrder++;
		profile.ordersTable.newOrder++;
		rollback();
		continue;
	    }

	    int olNumber;
	    for (olNumber = 1; olNumber <= paramsOlCnt; olNumber++) {
		var olSupplyWid = paramsSupplyWid;
		var olIid = paramsItemId[olNumber - 1];
		var olQuantity = paramsQty[olNumber - 1];

		// SELECT i_price, i_name , i_data FROM ITEM WHERE i_id = :i_id
		var ps6 = RequestProtos.ParameterSet.newBuilder()
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("i_id").setInt8Value(olIid));
		var future6 = transaction.executeQuery(prepared6, ps6);
		var resultSet6 = future6.getLeft().get();
		try {
		    if (!Objects.isNull(resultSet6)) {
			if (!resultSet6.nextRecord()) {
			    future6.getRight().get();
			    throw new ExecutionException(new IOException("no record"));
			}
			resultSet6.nextColumn();
			iPrice = resultSet6.getFloat8();
			resultSet6.nextColumn();
			iName = resultSet6.getCharacter();
			resultSet6.nextColumn();
			iData = resultSet6.getCharacter();
			if (resultSet6.nextRecord()) {
			    future6.getRight().get();
			    throw new ExecutionException(new IOException("found multiple records"));
			}
		    }
		    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(future6.getRight().get().getResultCase())) {
			throw new ExecutionException(new IOException("SQL error"));
		    }
		} catch (ExecutionException e) {
		    break;
		} finally {
		    if (!Objects.isNull(resultSet6)) {
			resultSet6.close();
		    }
		}

		// SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM STOCK WHERE s_i_id = :s_i_id AND s_w_id = :s_w_id
		var ps7 = RequestProtos.ParameterSet.newBuilder()
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("s_i_id").setInt8Value(olIid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("s_w_id").setInt8Value(olSupplyWid));
		var future7 = transaction.executeQuery(prepared7, ps7);
		var resultSet7 = future7.getLeft().get();
		try {
		    if (!Objects.isNull(resultSet7)) {
			if (!resultSet7.nextRecord()) {
			    future7.getRight().get();
			    throw new ExecutionException(new IOException("no record"));
			}
			resultSet7.nextColumn();
			sQuantity = resultSet7.getInt8();
			resultSet7.nextColumn();
			sData = resultSet7.getCharacter();
			for (int i = 0; i < 10; i++) {
			    resultSet7.nextColumn();
			    sDistData[i] = resultSet7.getCharacter();
			}
			if (resultSet7.nextRecord()) {
			    future7.getRight().get();
			    throw new ExecutionException(new IOException("found multiple records"));
			}
		    }
		    if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(future7.getRight().get().getResultCase())) {
			throw new ExecutionException(new IOException("SQL error"));
		    }
		} catch (ExecutionException e) {
		    profile.stockTable.newOrder++;
		    break;
		} finally {
		    if (!Objects.isNull(resultSet7)) {
			resultSet7.close();
		    }
		}

		String olDistInfo = sDistData[(int) paramsDid - 1].substring(0, 24);
		stock[olNumber - 1] = sQuantity;

		if (iData.indexOf("original") >= 0 && sData.indexOf("original") >= 0) {
		    bg[olNumber - 1] = "B";
		} else {
		    bg[olNumber - 1] = "G";
		}

		if (sQuantity > olQuantity) {
		    sQuantity = sQuantity - olQuantity;
		} else {
		    sQuantity = sQuantity - olQuantity + 91;
		}

		// UPDATE STOCK SET s_quantity = :s_quantity WHERE s_i_id = :s_i_id AND s_w_id = :s_w_id
		var ps8 = RequestProtos.ParameterSet.newBuilder()
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("s_quantity").setInt8Value(sQuantity))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("s_i_id").setInt8Value(olIid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("s_w_id").setInt8Value(olSupplyWid));
		var future8 = transaction.executeStatement(prepared8, ps8);
		var result8 = future8.get();
		if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(result8.getResultCase())) {
		    profile.stockTable.newOrder++;
		    break;
		}

		double olAmount = olQuantity * iPrice * (1 + wTax + dTax) * (1 - cDiscount);
		amt[olNumber - 1] = olAmount;
		total += olAmount;

		// INSERT INTO ORDER_LINE (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)VALUES (:ol_o_id, :ol_d_id, :ol_w_id, :ol_number, :ol_i_id, :ol_supply_w_id, :ol_quantity, :ol_amount, :ol_dist_info
		var ps9 = RequestProtos.ParameterSet.newBuilder()
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_o_id").setInt8Value(oid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_d_id").setInt8Value(paramsDid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_w_id").setInt8Value(paramsWid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_number").setInt8Value(olNumber))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_i_id").setInt8Value(olIid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_supply_w_id").setInt8Value(olSupplyWid))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_quantity").setInt8Value(olQuantity))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_amount").setFloat8Value(olAmount))
		    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("ol_dist_info").setCharacterValue(olDistInfo));
		var future9 = transaction.executeStatement(prepared9, ps9);
		var result9 = future9.get();
		if (!ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(result9.getResultCase())) {
		    profile.ordersTable.newOrder++;
		    break;
		}
	    }

	    if (olNumber > paramsOlCnt) {  // completed 'for (olNumber = 1; olNumber <= paramsOlCnt; olNumber++) {'
		if (paramsWillRollback) {
		    rollback();
		    profile.newOrderIntentionalRollback++;
		    return;
		}
		var commitResponse = transaction.commit().get();
		if (ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(commitResponse.getResultCase())) {
		    profile.completion.newOrder++;
		    return;
		}
		profile.retryOnCommit.newOrder++;
		continue;
	    }

	    // break in 'for (olNumber = 1; olNumber <= paramsOlCnt; olNumber++) {'
	    profile.retryOnStatement.newOrder++;
	    rollback();
	}
    }
}
