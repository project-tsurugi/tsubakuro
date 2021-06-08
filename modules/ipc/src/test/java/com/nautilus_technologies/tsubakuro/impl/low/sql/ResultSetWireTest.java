package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.nautilus_technologies.tsubakuro.low.sql.ProtosForTest;

import org.junit.jupiter.api.Test;

class ResultSetTest {
    private SessionWireImpl client;
    private ServerWireImpl server;
    private String wireName = "tsubakuro-session1";

    @Test
    void resultSetWire() {
	try {
	    server = new ServerWireImpl(wireName);
	    client = new SessionWireImpl(wireName);

	    // REQUEST test begin
	    // client side send Request
	    var futureResponse = client.send(ProtosForTest.ExecuteQueryRequestChecker.builder().build(), new ExecuteQueryDistiller());
	    // server side receive Request
	    assertTrue(ProtosForTest.ExecuteQueryRequestChecker.check(server.get()));
	    // REQUEST test end

	    // RESPONSE test begin
	    // server side send Response
	    var responseToBeSent = ProtosForTest.ExecuteQueryResponseChecker.builder().build();
	    server.put(responseToBeSent);

	    // server side send SchemaMeta
	    long rsHandle = server.createRSL(responseToBeSent.getExecuteQuery().getName());
	    server.putRSL(rsHandle, ProtosForTest.SchemaProtosChecker.builder().build());

	    // client side receive Response
	    var responseReceived = futureResponse.get();
	    assertTrue(ProtosForTest.ResMessageExecuteQueryChecker.check(responseReceived));

	    // client side receive SchemaMeta
	    var resultSetWire = client.createResultSetWire(responseReceived.getName());
	    var schemaMeta = resultSetWire.recvMeta();
	    assertTrue(ProtosForTest.SchemaProtosChecker.check(schemaMeta));
	    // RESPONSE test end

	    client.close();
	    server.close();
	} catch (IOException e) {
	    fail("cought IOException");
	} catch (InterruptedException e) {
	    fail("cought IOException");
	} catch (ExecutionException e) {
	    fail("cought IOException");
	}
    }
}
