package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.nautilus_technologies.tsubakuro.channel.common.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.common.ResponseWireHandle;
import com.nautilus_technologies.tsubakuro.channel.common.ChannelResponse;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.channel.common.wire.Response;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionImpl;
import com.tsurugidb.jogasaki.proto.SqlCommon;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.nautilus_technologies.tsubakuro.util.Owner;
import com.nautilus_technologies.tsubakuro.session.ProtosForTest;

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

    class ChannelResponseMock implements Response {
        private final SessionWireTestMock wire;
        private ResponseWireHandle handle;

        ChannelResponseMock(SessionWireTestMock wire) {
            this.wire = wire;
        }
        @Override
        public boolean isMainResponseReady() {
            return Objects.nonNull(handle);
        }
        @Override
        public ByteBuffer waitForMainResponse() throws IOException {
            return wire.response(handle);
        }
        @Override
        public ByteBuffer waitForMainResponse(long timeout, TimeUnit unit) throws IOException {
            return wire.response(handle);
        }
        @Override
        public void close() throws IOException, InterruptedException {
        }
        @Override
        public ResponseWireHandle responseWireHandle() {
            return handle;
        }
        @Override
        public void release() {
        }
        @Override
        public void setQueryMode() {
        }
    }
    
    class SessionWireTestMock implements SessionWire {
        @Override
        public ResultSetWire createResultSetWire() throws IOException {
            return null;  // dummy as it is test for session
        }
        @Override
        public void release(ResponseWireHandle responseWireHandle) {
        }
        @Override
        public void setQueryMode(ResponseWireHandle responseWireHandle) {
        }
        @Override
        public FutureResponse<? extends Response> send(long serviceID, byte[] byteArray) throws IOException {
            var request = SqlRequest.Request.parseDelimitedFrom(new ByteArrayInputStream(byteArray));
            switch (request.getRequestCase()) {
                case BEGIN:
                    nextResponse = ProtosForTest.BeginResponseChecker.builder().build();
                    break;
                case DISCONNECT:
                    nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                    break;
                default:
                    return null;  // dummy as it is test for session
            }
            return FutureResponse.wrap(Owner.of(new ChannelResponse(this)));
        }
        @Override
        public FutureResponse<? extends Response> send(long serviceID, ByteBuffer request) {
            return null; // dummy as it is test for session
        }
        @Override
        public ByteBuffer response(ResponseWireHandle handle) throws IOException {
            return ByteBuffer.wrap(DelimitedConverter.toByteArray(nextResponse));
        }
        @Override
        public ByteBuffer response(ResponseWireHandle handle, long timeout, TimeUnit unit) throws IOException {
            return response(handle); // dummy as it is test for session
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
