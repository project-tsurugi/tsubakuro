package com.tsurugidb.tsubakuro.kvs.realbench;

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.kvs.GetResult;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.bench.RecordBuilder;
import com.tsurugidb.tsubakuro.kvs.bench.RecordInfo;
import com.tsurugidb.tsubakuro.kvs.bench.ValueType;
import com.tsurugidb.tsubakuro.sql.SqlClient;

/**
 * An transaction test with real session.
 */
public class RealTransactionTest {

    static final Logger LOG = LoggerFactory.getLogger(RealTransactionTest.class);

    private final URI endpoint;
    private final Credential credential = NullCredential.INSTANCE;
    private final String tableName;

    RealTransactionTest(String[] args) {
        String name = (args.length > 0 ? args[0] : "ipc:tsurugi");
        LOG.debug("endpoint: {}", name); //$NON-NLS-1$
        this.endpoint = URI.create(name);
        this.tableName = "TABLE" + System.currentTimeMillis();
    }

    private void initDB() throws Exception {
        try (var session = SessionBuilder.connect(endpoint).withCredential(credential).create();
            var client = SqlClient.attach(session); var tx = client.createTransaction().await()) {
            {
                String sql = String.format("CREATE TABLE %s (%s BIGINT PRIMARY KEY, %s BIGINT)", tableName,
                        RecordBuilder.FIRST_KEY_NAME, RecordBuilder.FIRST_VALUE_NAME);
                tx.executeStatement(sql).await();
            }
            {
                String sql = String.format("INSERT INTO %s (%s,%s) VALUES(%d, %d)", tableName,
                        RecordBuilder.FIRST_KEY_NAME, RecordBuilder.FIRST_VALUE_NAME, 1, 100);
                tx.executeStatement(sql).await();
            }
            tx.commit().await();
            System.out.println("table " + tableName + " created");
        }
    }

    private void test() throws Exception {
        var builder = new RecordBuilder(new RecordInfo(ValueType.LONG, 1));
        try (var session = SessionBuilder.connect(endpoint).withCredential(credential).create();
            var kvs = KvsClient.attach(session); var tx = kvs.beginTransaction().await()) {
            var record = builder.makeRecordBuffer();
            {
                var rec = record.toRecord();
                for (int i = 0; i < rec.size(); i++) {
                    System.err.println(i + "\t" + rec.getName(i) + "\t" + rec.getValue(i));
                }
            }
            System.err.println("PUT");
            int n = kvs.put(tx, tableName, record).await().size();
            System.err.println(n);
            var key = new RecordBuffer();
            var pk = record.toRecord().getValue(0);
            key.add(record.toRecord().getName(0), pk);
            System.err.println("GET " + pk);
            GetResult get = kvs.get(tx, tableName, key).await();
            System.err.println(get.size());
            for (var rec : get.asList()) {
                for (int i = 0; i < rec.size(); i++) {
                    System.err.println(i + "\t" + rec.getName(i) + "\t" + rec.getValue(i));
                }
            }
            System.err.println("REMOVE " + pk);
            n = kvs.remove(tx, tableName, key).await().size();
            System.err.println(n);
            System.err.println("COMMIT");
            kvs.commit(tx).await();
        }
    }

    /**
     * Executes a test transaction.
     * <ul>
     * <li>{@code args[0]} : connection URI</li>
     * </ul>
     * @param args the program arguments
     * @throws Exception if exception was occurred
     */
    public static void main(String[] args) throws Exception {
        RealTransactionTest app = new RealTransactionTest(args);
        app.initDB();
        app.test();
    }

}
