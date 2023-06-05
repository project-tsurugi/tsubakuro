package com.tsurugidb.tsubakuro.kvs.realbench;

import java.net.URI;
import java.text.MessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.bench.RecordBuilder;
import com.tsurugidb.tsubakuro.kvs.bench.RecordInfo;
import com.tsurugidb.tsubakuro.kvs.bench.ValueType;

/**
 * An transaction test with real session.
 */
public class RealTransactionTest {

    static final Logger LOG = LoggerFactory.getLogger(RealTransactionTest.class);

    private final URI endpoint;
    private final Credential credential = NullCredential.INSTANCE;

    RealTransactionTest(String[] args) {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    MessageFormat.format("usage: java {0} <connection-uri>", RealTransactionTest.class.getName()));
        }
        LOG.debug("endpoint: {}", args[0]); //$NON-NLS-1$
        this.endpoint = URI.create(args[0]);
    }

    private void test() throws Exception {
        var builder = new RecordBuilder(new RecordInfo(ValueType.LONG, 1));
        final String table = "TABLE1";
        try (var session = SessionBuilder.connect(endpoint).withCredential(credential).create();
            var kvs = KvsClient.attach(session); var tx = kvs.beginTransaction().await()) {
            int n = kvs.get(tx, table, builder.makeRecordBuffer()).await().asList().size();
            System.err.println(n);
            n = kvs.put(tx, table, builder.makeRecordBuffer()).await().size();
            System.err.println(n);
            n = kvs.get(tx, table, builder.makeRecordBuffer()).await().size();
            System.err.println(n);
            n = kvs.remove(tx, table, builder.makeRecordBuffer()).await().size();
            System.err.println(n);
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
        app.test();
    }

}
