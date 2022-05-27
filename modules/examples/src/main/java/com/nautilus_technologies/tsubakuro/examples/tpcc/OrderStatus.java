package com.nautilus_technologies.tsubakuro.examples.tpcc;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.tsurugidb.jogasaki.proto.StatusProtos;

public class OrderStatus {
    SqlClient sqlClient;
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

    public OrderStatus(SqlClient sqlClient, RandomGenerator randomGenerator, Profile profile) throws IOException, ServerException, InterruptedException {
        this.sqlClient = sqlClient;
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
		prepared1 = sqlClient.prepare(sql1,
		Placeholders.of("c_w_id", long.class),
		Placeholders.of("c_d_id", long.class),
		Placeholders.of("c_last", String.class)).get();

		String sql2 = "SELECT c_id FROM CUSTOMER WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_last = :c_last  ORDER by c_first";
		prepared2 = sqlClient.prepare(sql2,
		Placeholders.of("c_w_id", long.class),
		Placeholders.of("c_d_id", long.class),
		Placeholders.of("c_last", String.class)).get();

		String sql3 = "SELECT c_balance, c_first, c_middle, c_last FROM CUSTOMER WHERE c_id = :c_id AND c_d_id = :c_d_id AND c_w_id = :c_w_id";
		prepared3 = sqlClient.prepare(sql3,
		Placeholders.of("c_id", long.class),
		Placeholders.of("c_d_id", long.class),
		Placeholders.of("c_w_id", long.class)).get();

		String sql4 = "SELECT o_id FROM ORDERS WHERE o_w_id = :o_w_id AND o_d_id = :o_d_id AND o_c_id = :o_c_id ORDER by o_id DESC";
		prepared4 = sqlClient.prepare(sql4,
		Placeholders.of("o_w_id", long.class),
		Placeholders.of("o_d_id", long.class),
		Placeholders.of("o_c_id", long.class)).get();

		String sql5 = "SELECT o_carrier_id, o_entry_d, o_ol_cnt FROM ORDERS WHERE o_w_id = :o_w_id AND o_d_id = :o_d_id AND o_id = :o_id";
		prepared5 = sqlClient.prepare(sql5,
		Placeholders.of("o_w_id", long.class),
		Placeholders.of("o_d_id", long.class),
		Placeholders.of("o_id", long.class)).get();

		String sql6 = "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM ORDER_LINE WHERE ol_o_id = :ol_o_id AND ol_d_id = :ol_d_id AND ol_w_id = :ol_w_id";
		prepared6 = sqlClient.prepare(sql6,
		Placeholders.of("ol_o_id", long.class),
		Placeholders.of("ol_d_id", long.class),
		Placeholders.of("ol_w_id", long.class)).get();
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
    if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(transaction.rollback().get().getResultCase())) {
        throw new IOException("error in rollback");
    }
    transaction = null;
    }

    @SuppressWarnings("checkstyle:methodlength")
    public void transaction(AtomicBoolean stop) throws IOException, ServerException, InterruptedException {
    while (!stop.get()) {
        transaction = sqlClient.createTransaction().get();
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
        var future3 = transaction.executeQuery(prepared3,
		Parameters.of("c_id", (long) cId),
        Parameters.of("c_d_id", (long) paramsDid),
        Parameters.of("c_w_id", (long) paramsWid));
        var resultSet3 = future3.get();
        try {
            if (Objects.nonNull(resultSet3)) {
            if (!resultSet3.nextRecord()) {
                if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet3.getResponse().get().getResultCase())) {
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
                if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet3.getResponse().get().getResultCase())) {
                throw new IOException("SQL error");
                }
                throw new IOException("found multiple records");
            }
            }
            if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet3.getResponse().get().getResultCase())) {
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
        var future4 = transaction.executeQuery(prepared4,
		Parameters.of("o_d_id", (long) paramsDid),
        Parameters.of("o_w_id", (long) paramsWid),
        Parameters.of("o_c_id", (long) cId));
        var resultSet4 = future4.get();
        try {
            if (Objects.nonNull(resultSet4)) {
            if (!resultSet4.nextRecord()) {
                if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet4.getResponse().get().getResultCase())) {
                throw new IOException("SQL error");
                }
                throw new IOException("no record");
            }
            resultSet4.nextColumn();
            oId = resultSet4.getInt8();
            }
            var status4 = resultSet4.getResponse().get();
            if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(status4.getResultCase())) {
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
        var future5 = transaction.executeQuery(prepared5,
		Parameters.of("o_d_id", (long) paramsDid),
        Parameters.of("o_w_id", (long) paramsWid),
        Parameters.of("o_id", (long) oId));
        var resultSet5 = future5.get();
        try {
            if (Objects.nonNull(resultSet5)) {
            if (!resultSet5.nextRecord()) {
                if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet5.getResponse().get().getResultCase())) {
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
                if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet5.getResponse().get().getResultCase())) {
                throw new IOException("SQL error");
                }
                throw new IOException("found multiple records");
            }
            }
            if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet5.getResponse().get().getResultCase())) {
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
        var future6 = transaction.executeQuery(prepared6,
		Parameters.of("ol_o_id", (long) oId),
        Parameters.of("ol_d_id", (long) paramsDid),
        Parameters.of("ol_w_id", (long) paramsWid));
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
            if (!SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(resultSet6.getResponse().get().getResultCase())) {
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
        if (SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(commitResponse.getResultCase())) {
                profile.completion.orderStatus++;
                return;
        }
        profile.retryOnCommit.orderStatus++;
        transaction = null;
    }
    }
}
