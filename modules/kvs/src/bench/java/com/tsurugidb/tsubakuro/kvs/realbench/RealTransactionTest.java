package com.tsurugidb.tsubakuro.kvs.realbench;

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.kvs.GetResult;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.PutType;
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

    private static final ValueType VALUE_TYPE = ValueType.LONG;
    private static final int VALUE_NUM = 1;

    private final RecordInfo RECORD_INFO = new RecordInfo(VALUE_TYPE, VALUE_NUM);

    RealTransactionTest(String[] args) {
        String name = (args.length > 0 ? args[0] : "ipc:tsurugi");
        LOG.debug("endpoint: {}", name); //$NON-NLS-1$
        this.endpoint = URI.create(name);
        this.tableName = "TABLE1";
    }

    static private String sqlValType() {
        switch (VALUE_TYPE) {
        case LONG:
            return "BIGINT";
        case STRING:
            return "STRING";
        default:
            throw new IllegalArgumentException("NOT SUPPORTED YET: " + VALUE_TYPE.name());
        }
    }

    String createTableSql() {
        String type = sqlValType();
        StringBuilder sb = new StringBuilder();
        String sql = String.format("CREATE TABLE %s (%s %s PRIMARY KEY", tableName, RecordBuilder.FIRST_KEY_NAME, type);
        sb.append(sql);
        for (int i = RecordBuilder.FIRST_COUMN_INDEX; i < RecordBuilder.FIRST_COUMN_INDEX + VALUE_NUM; i++) {
            sb.append(", ");
            String colName = RecordBuilder.VALUE_NAME_PREFIX + i;
            sb.append(colName);
            sb.append(" ");
            sb.append(type);
        }
        sb.append(")");
        return sb.toString();
    }

    void initDB() throws Exception {
        try (var session = SessionBuilder.connect(endpoint).withCredential(credential).create();
            var client = SqlClient.attach(session); var tx = client.createTransaction().await()) {
            String sql = createTableSql();
            System.out.println(sql);
            tx.executeStatement(sql).await();
            tx.commit().await();
            System.out.println("table " + tableName + " created");
        }
    }

    private static void dumpRecord(com.tsurugidb.tsubakuro.kvs.Record rec) {
        for (int i = 0; i < rec.size(); i++) {
            System.err.println(i + "\t" + rec.getName(i) + "\t" + rec.getValue(i).getClass().getSimpleName() + "\t"
                    + rec.getValue(i));
        }
    }

    void test() throws Exception {
        var builder = new RecordBuilder(RECORD_INFO);
        try (var session = SessionBuilder.connect(endpoint).withCredential(credential).create();
            var kvs = KvsClient.attach(session); var tx = kvs.beginTransaction().await()) {
            var record = builder.makeRecordBuffer();
            dumpRecord(record.toRecord());
            System.err.println("PUT");
            /*
            int n = kvs.put(tx, tableName, record, PutType.IF_PRESENT).await().size();
            System.err.println(n);
            n = kvs.put(tx, tableName, record, PutType.OVERWRITE).await().size();
            System.err.println(n);
            n = kvs.put(tx, tableName, record, PutType.IF_PRESENT).await().size();
            System.err.println(n);
            n = kvs.put(tx, tableName, record, PutType.IF_ABSENT).await().size();
            System.err.println(n);
            */
            int n = kvs.put(tx, tableName, record, PutType.OVERWRITE).await().size();
            System.err.println(n);
            var key = new RecordBuffer();
            var pk = record.toRecord().getValue(0);
            key.add(record.toRecord().getName(0), pk);
            System.err.println("GET " + pk);
            GetResult get = kvs.get(tx, tableName, key).await();
            System.err.println(get.size());
            for (var rec : get.asList()) {
                dumpRecord(rec);
            }
            System.err.println("REMOVE " + pk);
            n = kvs.remove(tx, tableName, key).await().size();
            System.err.println(n);
            // n = kvs.put(tx, tableName, record, PutType.IF_ABSENT).await().size();
            // System.err.println(n);
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
