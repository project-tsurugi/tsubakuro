package com.tsurugidb.tsubakuro.examples.tpcc;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.Parameters;

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
        try {
            transaction.rollback().get();
        } finally {
            transaction = null;
        }
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
                try (var resultSet3 = future3.get()) {
                    if (!resultSet3.nextRow()) {
                        throw new IOException("no record");
                    }
                    resultSet3.nextColumn();
                    cBalance = resultSet3.fetchFloat8Value();
                    resultSet3.nextColumn();
                    cFirst = resultSet3.fetchCharacterValue();
                    resultSet3.nextColumn();
                    cMiddle = resultSet3.fetchCharacterValue();
                    resultSet3.nextColumn();
                    cLast = resultSet3.fetchCharacterValue();
                    if (resultSet3.nextRow()) {
                        throw new IOException("found multiple records");
                    }
                } catch (ServerException e) {
                    profile.retryOnStatement.orderStatus++;
                    profile.ordersTable.orderStatus++;
                    rollback();
                    continue;
                }
    
                // "SELECT o_id FROM ORDERS WHERE o_w_id = :o_w_id AND o_d_id = :o_d_id AND o_c_id = :o_c_id ORDER by o_id DESC"
                var future4 = transaction.executeQuery(prepared4,
                    Parameters.of("o_d_id", (long) paramsDid),
                    Parameters.of("o_w_id", (long) paramsWid),
                    Parameters.of("o_c_id", (long) cId));
                try (var resultSet4 = future4.get()) {
                    if (!resultSet4.nextRow()) {
                        throw new IOException("no record");
                    }
                    resultSet4.nextColumn();
                    oId = resultSet4.fetchInt8Value();
                    // FIXME treat InconsistentIndex
                    //                    if (status4.getError().getStatus() == SqlStatus.Status.ERR_INCONSISTENT_INDEX) {
                    //                        if (profile.inconsistentIndexCount == 0) {
                    //                            System.out.println("inconsistent_index");
                    //                        }
                    //                        profile.inconsistentIndexCount++;
                    //                    }
                } catch (ServerException e) {
                    profile.retryOnStatement.orderStatus++;
                    profile.ordersTable.orderStatus++;
                    rollback();
                    continue;
                }
    
                // "SELECT o_carrier_id, o_entry_d, o_ol_cnt FROM ORDERS WHERE o_w_id = :o_w_id AND o_d_id = :o_d_id AND o_id = :o_id"
                var future5 = transaction.executeQuery(prepared5,
                    Parameters.of("o_d_id", (long) paramsDid),
                    Parameters.of("o_w_id", (long) paramsWid),
                    Parameters.of("o_id", (long) oId));
                try (var resultSet5 = future5.get()) {
                    if (!resultSet5.nextRow()) {
                        throw new IOException("no record");
                    }
                    resultSet5.nextColumn();
                    if (!resultSet5.isNull()) {
                        oCarrierId = resultSet5.fetchInt8Value();
                    }
                    resultSet5.nextColumn();
                    oEntryD = resultSet5.fetchCharacterValue();
                    resultSet5.nextColumn();
                    oOlCnt = resultSet5.fetchInt8Value();
                    if (resultSet5.nextRow()) {
                        throw new IOException("found multiple records");
                    }
                } catch (ServerException e) {
                    profile.retryOnStatement.orderStatus++;
                    profile.ordersTable.orderStatus++;
                    rollback();
                    continue;
                }
    
                // "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM ORDER_LINE WHERE ol_o_id = :ol_o_id AND ol_d_id = :ol_d_id AND ol_w_id = :ol_w_id"
                var future6 = transaction.executeQuery(prepared6,
                    Parameters.of("ol_o_id", (long) oId),
                    Parameters.of("ol_d_id", (long) paramsDid),
                    Parameters.of("ol_w_id", (long) paramsWid));
                try (var resultSet6 = future6.get()) {
                    int i = 0;
                    while (resultSet6.nextRow()) {
                        resultSet6.nextColumn();
                        olIid[i] = resultSet6.fetchInt8Value();
                        resultSet6.nextColumn();
                        olSupplyWid[i] = resultSet6.fetchInt8Value();
                        resultSet6.nextColumn();
                        olQuantity[i] = resultSet6.fetchInt8Value();
                        resultSet6.nextColumn();
                        olAmount[i] = resultSet6.fetchFloat8Value();
                        resultSet6.nextColumn();
                        if (!resultSet6.isNull()) {
                            olDeliveryD[i] = resultSet6.fetchCharacterValue();
                        }
                        i++;
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
                }
            }
    
            try {
                transaction.commit().get();
                profile.completion.orderStatus++;
                return;
            } catch (ServerException e) {
                profile.retryOnCommit.orderStatus++;
                transaction = null;
            }
        }
    }
}
