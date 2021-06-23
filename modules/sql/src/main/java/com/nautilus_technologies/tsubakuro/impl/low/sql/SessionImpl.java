package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.Objects;
import java.util.concurrent.Future;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

/**
 * SessionImpl type.
 */
public class SessionImpl implements Session {
    private SessionLinkImpl sessionLink;
    
    /**
     * Connect this session to the Database
     * @param sessionWire the wire that connects to the Database
     */
    public void connect(SessionWire sessionWire) {
	this.sessionLink = new SessionLinkImpl(sessionWire);
    }

    /**
     * Begin a new transaction
     * @param readOnly specify whether the new transaction is read-only or not
     * @return the transaction
     */
    public Future<Transaction> createTransaction(boolean readOnly) throws IOException {
	if (Objects.isNull(sessionLink)) {
	    throw new IOException("this session is not connected to the Database");
	}
	return new FutureTransactionImpl(sessionLink, sessionLink.send(RequestProtos.Begin.newBuilder()
								       .setReadOnly(readOnly)));
    }

    /**
     * Begin a new read-write transaction
     * @return the transaction
     */
    public Future<Transaction> createTransaction() throws IOException {
	return createTransaction(false);
    }

    /**
     * Request prepare to the SQL service
     * @param sql sql text for the command
     * @param placeHolder the set of place holder name and type of its variable encoded with protocol buffer
     * @return Future<PreparedStatement> holds the result of the SQL service
     */
    public Future<PreparedStatement> prepare(String sql, RequestProtos.PlaceHolder.Builder placeHolder) throws IOException {
	if (Objects.isNull(sessionLink)) {
	    throw new IOException("this session is not connected to the Database");
	}
	return sessionLink.send(RequestProtos.Prepare.newBuilder()
				.setSql(sql)
				.setHostVariables(placeHolder));
    }

    /**
     * Close the Session
     */
    /**
     * Close the Transaction
     */
    public void close() throws IOException {  // FIXME
    }
}
