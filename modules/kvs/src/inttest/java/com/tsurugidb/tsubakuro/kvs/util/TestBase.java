package com.tsurugidb.tsubakuro.kvs.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;

import com.tsurugidb.kvs.proto.KvsData;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.sql.SqlClient;

/**
 * The base class for integration test class.
 */
public class TestBase {

    private static final String SYSPROP_KVSTEST_ENDPOINT = "tsurugi.kvstest.endpoint";
    private static final URI ENDPOINT;

    static {
        String uri = System.getProperty(SYSPROP_KVSTEST_ENDPOINT, "ipc:tsurugi");
        ENDPOINT = URI.create(uri);
        System.err.println("endpoint=" + ENDPOINT);
    }

    /**
     * Retrieves a new session
     * @return a new session
     * @throws Exception failed to create a new session
     */
    public static Session getNewSession() throws Exception {
        return SessionBuilder.connect(ENDPOINT).withCredential(NullCredential.INSTANCE).create();
    }

    private static void dropTable(SqlClient client, String tableName) throws Exception {
        try {
            try (var tx = client.createTransaction().await()) {
                String sql = String.format("DROP TABLE %s", tableName);
                tx.executeStatement(sql).await();
                tx.commit().await();
            }
        } catch (Exception e) {
            var msg = e.getMessage();
            if (!msg.contains("table_not_found") && !msg.contains("not found")) {
                throw e;
            }
        }
    }

    /**
     * Drop the table.
     * @param tableName the name of the table
     * @throws Exception failed to drop the table
     * @note exception doesn't throw even if the table doesn't exists
     */
    public void dropTable(String tableName) throws Exception {
        try (var session = getNewSession(); var client = SqlClient.attach(session)) {
            dropTable(client, tableName);
        }
    }

    /**
     * Creates a new table.
     * @param tableName the name of the table
     * @param schema the schema of the table
     * @throws Exception failed to create a new table
     */
    public void createTable(String tableName, String schema) throws Exception {
        try (var session = getNewSession(); var client = SqlClient.attach(session)) {
            dropTable(client, tableName);
            try (var tx = client.createTransaction().await()) {
                String sql = String.format("CREATE TABLE %s (%s)", tableName, schema);
                tx.executeStatement(sql).await();
                tx.commit().await();
            }
        }
    }

    /**
     * Executes SQL statement.
     * @param sql SQL statement
     * @throws Exception failed to execute the SQL statement
     */
    public void executeStatement(String sql) throws Exception {
        try (var session = getNewSession(); var client = SqlClient.attach(session)) {
            try (var tx = client.createTransaction().await()) {
                tx.executeStatement(sql).await();
                tx.commit().await();
            }
        }
    }

    /**
     * retrieves line number of source
     * @return line number
     */
    public static int getLineNumber() {
        return Thread.currentThread().getStackTrace()[2].getLineNumber();
    }

    /**
     * retrieves BigDecimal value
     * @param value value contains Decimal value
     * @param scale scale of return BigDecimal
     * @return BigDecimalValue
     */
    public static BigDecimal toBigDecimal(KvsData.Value value, int scale) {
        KvsData.Decimal dec = value.getDecimalValue();
        var big = new BigDecimal(new BigInteger(dec.getUnscaledValue().toByteArray()), -dec.getExponent());
        if (big.scale() < scale) {
            // "12.3" -> "12.30" etc
            big = big.setScale(scale);
        }
        return big;
    }

}
