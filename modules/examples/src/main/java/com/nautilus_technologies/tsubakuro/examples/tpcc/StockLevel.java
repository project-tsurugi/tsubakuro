package com.nautilus_technologies.tsubakuro.examples.tpcc;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.Parameters;

public class StockLevel {
    SqlClient sqlClient;
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

    public StockLevel(SqlClient sqlClient, RandomGenerator randomGenerator, Profile profile) throws IOException, ServerException, InterruptedException {
        this.sqlClient = sqlClient;
        this.randomGenerator = randomGenerator;
        this.warehouses = profile.warehouses;
        this.profile = profile;
    }

    public void prepare() throws IOException, ServerException, InterruptedException {
        String sql1 = "SELECT d_next_o_id FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id";
        prepared1 = sqlClient.prepare(sql1,
            Placeholders.of("d_w_id", long.class),
            Placeholders.of("d_id", long.class)).get();


        String sql2 = "SELECT COUNT(DISTINCT s_i_id) FROM ORDER_LINE JOIN STOCK ON s_i_id = ol_i_id WHERE ol_w_id = :ol_w_id AND ol_d_id = :ol_d_id AND ol_o_id < :ol_o_id_high AND ol_o_id >= :ol_o_id_low AND s_w_id = :s_w_id AND s_quantity < :s_quantity";
        prepared2 = sqlClient.prepare(sql2,
            Placeholders.of("ol_w_id", long.class),
            Placeholders.of("ol_d_id", long.class),
            Placeholders.of("ol_o_id_high", long.class),
            Placeholders.of("ol_o_id_low", long.class),
            Placeholders.of("s_w_id", long.class),
            Placeholders.of("s_quantity", long.class)).get();
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
        try {
            transaction.rollback().get();
        } finally {
            transaction = null;
        }
    }

    public void transaction(AtomicBoolean stop) throws IOException, ServerException, InterruptedException {
        while (!stop.get()) {
            profile.invocation.stockLevel++;
            transaction = sqlClient.createTransaction().get();

            // "SELECT d_next_o_id FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id"
            var future1 = transaction.executeQuery(prepared1,
                Parameters.of("d_w_id", (long) paramsWid),
                Parameters.of("d_id", (long) paramsDid));
            try (var resultSet1 = future1.get()) {
                if (!resultSet1.nextRow()) {
                    resultSet1.getResponse().get();
                    throw new IOException("no record");
                }
                resultSet1.nextColumn();
                oId = resultSet1.fetchInt8Value();
                if (resultSet1.nextRow()) {
                    resultSet1.getResponse().get();
                    throw new IOException("found multiple records");
                }
                resultSet1.getResponse().get();
            } catch (ServerException e) {
                profile.retryOnStatement.stockLevel++;
                profile.districtTable.stockLevel++;
                rollback();
                continue;
            }

            // "SELECT COUNT(DISTINCT s_i_id) FROM ORDER_LINE JOIN STOCK ON s_i_id = ol_i_id WHERE ol_w_id = :ol_w_id AND ol_d_id = :ol_d_id AND ol_o_id < :ol_o_id_high AND ol_o_id >= :ol_o_id_low AND s_w_id = :s_w_id AND s_quantity < :s_quantity"
            var future2 = transaction.executeQuery(prepared2,
                Parameters.of("ol_w_id", (long) paramsWid),
                Parameters.of("ol_d_id", (long) paramsDid),
                Parameters.of("ol_o_id_high", (long) oId),
                Parameters.of("ol_o_id_low", (long) (oId - OID_RANGE)),
                Parameters.of("s_w_id", (long) paramsWid),
                Parameters.of("s_quantity", (long) paramsThreshold));
            try (var resultSet2 = future2.get()) {
                if (!resultSet2.nextRow()) {
                    resultSet2.getResponse().get();
                    throw new IOException("no record");
                }
                resultSet2.nextColumn();
                queryResult = resultSet2.fetchInt8Value();
                if (resultSet2.nextRow()) {
                    resultSet2.getResponse().get();
                    throw new IOException("found multiple records");
                }
                resultSet2.getResponse().get();
            } catch (ServerException e) {
                profile.retryOnStatement.stockLevel++;
                profile.stockTable.stockLevel++;
                rollback();
                continue;
            }
    
            try {
                transaction.commit().get();
                profile.completion.stockLevel++;
                return;
            } catch (ServerException e) {
                profile.retryOnCommit.stockLevel++;
                transaction = null;
            }
        }
    }
}
