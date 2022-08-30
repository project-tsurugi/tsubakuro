package com.tsurugidb.tsubakuro.examples.tpcc;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.Parameters;

public class Delivery {
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
    PreparedStatement prepared7;

    long warehouses;
    long paramsWid;
    long paramsOcarrierId;
    String paramsOlDeliveryD;

    //  local variables
    long noOid;
    long cId;
    double olTotal;

    public Delivery(SqlClient sqlClient, RandomGenerator randomGenerator, Profile profile) throws IOException, ServerException, InterruptedException {
        this.sqlClient = sqlClient;
    this.randomGenerator = randomGenerator;
    this.warehouses = profile.warehouses;
    this.profile = profile;
    }

    public void prepare() throws IOException, ServerException, InterruptedException {
        String sql1 = "SELECT no_o_id FROM NEW_ORDER WHERE no_d_id = :no_d_id AND no_w_id = :no_w_id ORDER BY no_o_id";
        prepared1 = sqlClient.prepare(sql1,
            Placeholders.of("no_d_id", long.class),
            Placeholders.of("no_w_id", long.class)).get();
    
        String sql2 = "DELETE FROM NEW_ORDER WHERE no_d_id = :no_d_id AND no_w_id = :no_w_id AND no_o_id = :no_o_id";
        prepared2 = sqlClient.prepare(sql2,
            Placeholders.of("no_d_id", long.class),
            Placeholders.of("no_w_id", long.class),
            Placeholders.of("no_o_id", long.class)).get();
    
        String sql3 = "SELECT o_c_id FROM ORDERS WHERE o_id = :o_id AND o_d_id = :o_d_id AND o_w_id = :o_w_id";
        prepared3 = sqlClient.prepare(sql3,
            Placeholders.of("o_id", long.class),
            Placeholders.of("o_d_id", long.class),
            Placeholders.of("o_w_id", long.class)).get();
    
        String sql4 = "UPDATE ORDERS SET o_carrier_id = :o_carrier_id WHERE o_id = :o_id AND o_d_id = :o_d_id AND o_w_id = :o_w_id";
        prepared4 = sqlClient.prepare(sql4,
            Placeholders.of("o_carrier_id", long.class),
            Placeholders.of("o_id", long.class),
            Placeholders.of("o_d_id", long.class),
            Placeholders.of("o_w_id", long.class)).get();
    
        String sql5 = "UPDATE ORDER_LINE SET ol_delivery_d = :ol_delivery_d WHERE ol_o_id = :ol_o_id AND ol_d_id = :ol_d_id AND ol_w_id = :ol_w_id";
        prepared5 = sqlClient.prepare(sql5,
            Placeholders.of("ol_delivery_d", String.class),
            Placeholders.of("ol_o_id", long.class),
            Placeholders.of("ol_d_id", long.class),
            Placeholders.of("ol_w_id", long.class)).get();
    
        String sql6 = "SELECT SUM(ol_amount) FROM ORDER_LINE WHERE ol_o_id = :ol_o_id AND ol_d_id = :ol_d_id AND ol_w_id = :ol_w_id";
        prepared6 = sqlClient.prepare(sql6,
            Placeholders.of("ol_o_id", long.class),
            Placeholders.of("ol_d_id", long.class),
            Placeholders.of("ol_w_id", long.class)).get();
    
        String sql7 = "UPDATE CUSTOMER SET c_balance = c_balance + :ol_total WHERE c_id = :c_id AND c_d_id = :c_d_id AND c_w_id = :c_w_id";
        prepared7 = sqlClient.prepare(sql7,
            Placeholders.of("ol_total", double.class),
            Placeholders.of("c_id", long.class),
            Placeholders.of("c_d_id", long.class),
            Placeholders.of("c_w_id", long.class)).get();
    }

    public void setParams() {
        if (profile.fixThreadMapping) {
            long warehouseStep = warehouses / profile.threads;
            paramsWid = randomGenerator.uniformWithin((profile.index * warehouseStep) + 1, (profile.index + 1) * warehouseStep);
        } else {
            paramsWid = randomGenerator.uniformWithin(1, warehouses);
        }
        paramsOcarrierId = randomGenerator.uniformWithin(1, 10);
        paramsOlDeliveryD = NewOrder.timeStamp();
        }
    
        public long warehouseId() {
        return paramsWid;
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
            profile.invocation.delivery++;
            long dId;
            for (dId = 1; dId <= Scale.DISTRICTS; dId++) {

                // "SELECT no_o_id FROM NEW_ORDER WHERE no_d_id = :no_d_id AND no_w_id = :no_w_id ORDER BY no_o_id"
                var future1 = transaction.executeQuery(prepared1,
                Parameters.of("no_d_id", (long) dId),
                Parameters.of("no_w_id", (long) paramsWid));
                try (var resultSet1 = future1.get()) {
                    if (!resultSet1.nextRow()) {
                        continue;  // noOid is exhausted, it's OK and continue this transaction
                    }
                    resultSet1.nextColumn();
                    noOid = resultSet1.fetchInt8Value();
                } catch (ServerException e) {
                    profile.ordersTable.delivery++;
                    break;
                }
    
                try {
                    // "DELETE FROM NEW_ORDER WHERE no_d_id = :no_d_id AND no_w_id = :no_w_id AND no_o_id = :no_o_id"
                    var future2 = transaction.executeStatement(prepared2,
                        Parameters.of("no_d_id", (long) dId),
                        Parameters.of("no_w_id", (long) paramsWid),
                        Parameters.of("no_o_id", (long) noOid));
                    var result2 = future2.get();
                } catch (ServerException e) {
                    profile.ordersTable.delivery++;
                    break;
                }
        
                // "SELECT o_c_id FROM ORDERS WHERE o_id = :o_id AND o_d_id = :o_d_id AND o_w_id = :o_w_id"
                var future3 = transaction.executeQuery(prepared3,
                        Parameters.of("o_id", (long) noOid),
                        Parameters.of("o_d_id", (long) dId),
                        Parameters.of("o_w_id", (long) paramsWid));
                try (var resultSet3 = future3.get()) {
                    if (!resultSet3.nextRow()) {
                        throw new IOException("no record");
                    }
                    resultSet3.nextColumn();
                    cId = resultSet3.fetchInt8Value();
                    if (resultSet3.nextRow()) {
                        throw new IOException("found multiple records");
                    }
                } catch (ServerException e) {
                    profile.ordersTable.delivery++;
                    break;
                }
        
                try {
                    // "UPDATE ORDERS SET o_carrier_id = :o_carrier_id WHERE o_id = :o_id AND o_d_id = :o_d_id AND o_w_id = :o_w_id"
                    var future4 = transaction.executeStatement(prepared4,
                        Parameters.of("o_carrier_id", (long) paramsOcarrierId),
                        Parameters.of("o_id", (long) noOid),
                        Parameters.of("o_d_id", (long) dId),
                        Parameters.of("o_w_id", (long) paramsWid));
                    var result4 = future4.get();
                } catch (ServerException e) {
                    profile.ordersTable.delivery++;
                    break;
                }
        
                try {
                    // "UPDATE ORDER_LINE SET ol_delivery_d = :ol_delivery_d WHERE ol_o_id = :ol_o_id AND ol_d_id = :ol_d_id AND ol_w_id = :ol_w_id"
                    var future5 = transaction.executeStatement(prepared5,
                        Parameters.of("ol_delivery_d", paramsOlDeliveryD),
                        Parameters.of("ol_o_id", (long) noOid),
                        Parameters.of("ol_d_id", (long) dId),
                        Parameters.of("ol_w_id", (long) paramsWid));
                    var result5 = future5.get();
                } catch (ServerException e) {
                    profile.ordersTable.delivery++;
                    break;
                }
        
                // "SELECT o_c_id FROM ORDERS WHERE o_id = :o_id AND o_d_id = :o_d_id AND o_w_id = :o_w_id"
                var future6 = transaction.executeQuery(prepared6,
                        Parameters.of("ol_o_id", (long) noOid),
                        Parameters.of("ol_d_id", (long) dId),
                        Parameters.of("ol_w_id", (long) paramsWid));
                try (var resultSet6 = future6.get()) {
                    if (!resultSet6.nextRow()) {
                        continue;
                    }
                    resultSet6.nextColumn();
                    olTotal = resultSet6.fetchFloat8Value();
                    if (resultSet6.nextRow()) {
                        throw new IOException("found multiple records");
                    }
                } catch (ServerException e) {
                    profile.ordersTable.delivery++;
                    break;
                }
        
                try {
                    // "UPDATE CUSTOMER SET c_balance = c_balance + :ol_total WHERE c_id = :c_id AND c_d_id = :c_d_id AND c_w_id = :c_w_id"
                    var future7 = transaction.executeStatement(prepared7,
                        Parameters.of("ol_total", (double) olTotal),
                        Parameters.of("ol_o_id", (long) cId),
                        Parameters.of("ol_d_id", (long) dId),
                        Parameters.of("ol_w_id", (long) paramsWid));
                    var result7 = future7.get();
                } catch (ServerException e) {
                    profile.customerTable.delivery++;
                    break;
                }
            }
    
            if (dId > Scale.DISTRICTS) {  // completed 'for (dId = 1; dId <= Scale.DISTRICTS; dId++) {'
                try {
                    transaction.commit().get();
                    profile.completion.delivery++;
                    return;
                } catch (ServerException e) {
                    profile.retryOnCommit.delivery++;
                    transaction = null;
                    continue;
                }
            }
    
            // break in 'for (dId = 1; dId <= Scale.DISTRICTS; dId++) {'
            profile.retryOnStatement.delivery++;
            rollback();
        }
    }
}
