package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.nautilus_technologies.tsubakuro.channel.common.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.common.ResponseWireHandle;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.channel.common.wire.Response;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionImpl;
import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.tsurugidb.jogasaki.proto.SqlCommon;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.nautilus_technologies.tsubakuro.session.ProtosForTest;
import com.nautilus_technologies.tsubakuro.util.Pair;

class DumpLoadTest {
    SqlResponse.Response nextResponse;
    
    private class PreparedStatementMock implements PreparedStatement {
        PreparedStatementMock() {
        }
        public SqlCommon.PreparedStatement getHandle() throws IOException {
            return null;
        }
        @Override
        public boolean hasResultRecords() {
            return false;
        }
        @Override
        public void setCloseTimeout(long t, TimeUnit u) {
        }
        
        @Override
        public void close() throws IOException {
        }
    }
    
    class FutureResponseTestMock<V> implements FutureResponse<V> {
        private final SessionWireTestMock wire;
        private final Distiller<V> distiller;
        private ResponseWireHandle handle; // dummy
        FutureResponseTestMock(SessionWireTestMock wire, Distiller<V> distiller) {
            this.wire = wire;
            this.distiller = distiller;
        }
        
        @Override
        public V get() throws IOException, ServerException {
            var response = wire.receive(handle);
            if (Objects.isNull(response)) {
                throw new IOException("received null response at FutureResponseTestMock, probably test program is incomplete");
            }
            return distiller.distill(response);
        }
        @Override
        public V get(long timeout, TimeUnit unit) throws IOException, ServerException {
            return get();  // FIXME need to be implemented properly, same as below
        }
        
        @Override
        public boolean isDone() {
            return true;
        }
        
        @Override
        public void close() throws IOException, ServerException, InterruptedException {
        }
    }
    
    class SessionWireTestMock implements SessionWire {
        @Override
        public <V> FutureResponse<V> send(long serviceID, SqlRequest.Request.Builder request, Distiller<V> distiller) throws IOException {
            switch (request.getRequestCase()) {
            case BEGIN:
                nextResponse = ProtosForTest.BeginResponseChecker.builder().build();
                return new FutureResponseTestMock<V>(this, distiller);
            case DISCONNECT:
                nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                return new FutureResponseTestMock<V>(this, distiller);
            default:
                return null;  // dummy as it is test for session
            }
        }
        @Override
        public Pair<FutureResponse<SqlResponse.ExecuteQuery>, FutureResponse<SqlResponse.ResultOnly>> sendQuery(long serviceID, SqlRequest.Request.Builder request) throws IOException {
            return null;  // dummy as it is test for session
        }
        @Override
        public SqlResponse.Response receive(ResponseWireHandle handle) throws IOException {
            var r = nextResponse;
            nextResponse = null;
            return r;
        }
        @Override
        public ResultSetWire createResultSetWire() throws IOException {
            return null;  // dummy as it is test for session
        }
        @Override
        public SqlResponse.Response receive(ResponseWireHandle handle, long timeout, TimeUnit unit) {
            return null;  // dummy as it is test for session
        }
        @Override
        public void unReceive(ResponseWireHandle responseWireHandle) {
        }
        @Override
        public FutureResponse<? extends Response> send(long serviceID, byte[] request) {
            return null; // dummy as it is test for session
        }
        @Override
        public FutureResponse<? extends Response> send(long serviceID, ByteBuffer request) {
            return null; // dummy as it is test for session
        }
        @Override
        public InputStream responseStream(ResponseWireHandle handle) {
            return null; // dummy as it is test for session
        }
        @Override
        public InputStream responseStream(ResponseWireHandle handle, long timeout, TimeUnit unit) {
            return null; // dummy as it is test for session
        }
        @Override
        public void close() throws IOException {
        }
    }
    
    void loadOK() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireTestMock());
        var sqlClient = SqlClient.attach(session);

        var preparedStatement = new PreparedStatementMock();

        var opts = SqlRequest.TransactionOption.newBuilder()
            .setType(SqlRequest.TransactionType.LONG)
            .addWritePreserves(SqlRequest.WritePreserve.newBuilder().setTableName("LOAD_TARGET"))
            .build();

        FutureResponse<Transaction> fTransaction = sqlClient.createTransaction(opts);

        try (Transaction transaction = fTransaction.get()) {
            List<Path> paths = new ArrayList<>();
            paths.add(Path.of("/load_directory/somefile"));
            var response = transaction.executeLoad(preparedStatement,
                                                   List.of(),
                                                   paths).get();
            
            assertTrue(ProtosForTest.ResultOnlyChecker.check(response));
            
            transaction.commit();
            session.close();
        }
    }
    
    void loadNG() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireTestMock());
        var sqlClient = SqlClient.attach(session);

        var preparedStatement = new PreparedStatementMock();

        var opts = SqlRequest.TransactionOption.newBuilder()
            .setType(SqlRequest.TransactionType.LONG)
            .addWritePreserves(SqlRequest.WritePreserve.newBuilder().setTableName("LOAD_TARGET"))
            .build();

        FutureResponse<Transaction> fTransaction = sqlClient.createTransaction(opts);

        try (Transaction transaction = fTransaction.get()) {
            List<Path> paths = new ArrayList<>();
            paths.add(Path.of("/load_directory/NGfile"));  // when file name includes "NG", executeLoad() will return error.
            var response = transaction.executeLoad(preparedStatement,
                                                   List.of(),
                                                   paths).get();
            
            assertFalse(ProtosForTest.ResultOnlyChecker.check(response));
            
            transaction.commit();
            session.close();
        }
    }
    
    void dumpOK() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireTestMock());
        var sqlClient = SqlClient.attach(session);
        
        var preparedStatement = new PreparedStatementMock();
        var target = Path.of("/dump_directory");
        
        var opts = SqlRequest.TransactionOption.newBuilder()
            .setType(SqlRequest.TransactionType.LONG)
            .addWritePreserves(SqlRequest.WritePreserve.newBuilder().setTableName("LOAD_TARGET"))
            .build();
        
        FutureResponse<Transaction> fTransaction = sqlClient.createTransaction(opts);
        
        try (Transaction transaction = fTransaction.get()) {
            FutureResponse<ResultSet> fResults = transaction.executeDump(preparedStatement,
                                                                         List.of(),
                                                                         target);
            
            var results = fResults.get();
            assertTrue(Objects.nonNull(results));
            
            int recordCount = 0;
            int columnCount = 0;
            while (results.nextRecord()) {
                while (results.nextColumn()) {
                    assertEquals(results.type(), SqlCommon.AtomType.CHARACTER);
                    assertEquals(results.getCharacter(), ResultSetMock.FILE_NAME);
                    columnCount++;
                }
                recordCount++;
            }
            assertEquals(columnCount, 1);
            assertEquals(recordCount, 1);
            
            assertTrue(ProtosForTest.ResultOnlyChecker.check(results.getResponse().get()));
            
            transaction.commit();
            session.close();
        }
    }
    
    void dumpNG() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireTestMock());
        var sqlClient = SqlClient.attach(session);
        
        var preparedStatement = new PreparedStatementMock();
        var target = Path.of("/dump_NGdirectory");  // when directory name includes "NG", executeDump() will return error.
        
        var opts = SqlRequest.TransactionOption.newBuilder()
            .setType(SqlRequest.TransactionType.LONG)
            .addWritePreserves(SqlRequest.WritePreserve.newBuilder().setTableName("LOAD_TARGET"))
            .build();
        
        FutureResponse<Transaction> fTransaction = sqlClient.createTransaction(opts);
        
        try (Transaction transaction = fTransaction.get()) {
            FutureResponse<ResultSet> fResults = transaction.executeDump(preparedStatement,
                                                                         List.of(),
                                                                         target);
            var results = fResults.get();
            assertTrue(Objects.nonNull(results));
            assertFalse(ProtosForTest.ResultOnlyChecker.check(results.getResponse().get()));
            
            transaction.commit();
            session.close();
        }
    }
}
