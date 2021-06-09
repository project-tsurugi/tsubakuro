package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.RequestProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * SessionImpl type.
 */
public class SessionImpl implements Session {
    private SessionLinkImpl sessionLink;
    
    /**
     * Creates a new instance.
     * @param name the neme of this session
     */
    public SessionImpl(SessionWire sessionWire) {
	this.sessionLink = new SessionLinkImpl(sessionWire);
    }

    /**
     * Begin the new transaction
     * @param readOnly specify whether the new transaction is read-only or not
     * @return the transaction
     */
    public Future<Transaction> createTransaction(boolean readOnly) throws IOException {
	return new FutureTransactionImpl(sessionLink, sessionLink.send(RequestProtos.Begin.newBuilder()
								       .setReadOnly(readOnly)
								       .build()));
    }

    /**
     * Begin the new transaction
     * @return the transaction
     */
    public Future<Transaction> createTransaction() throws IOException {
	return new FutureTransactionImpl(sessionLink, sessionLink.send(RequestProtos.Begin.newBuilder()
								       .setReadOnly(false)
								       .build()));
    }

    /**
     * Request prepare to the SQL service
     * @param sql sql text for the command
     * @param placeHolder the set of place holder name and type of its variable encoded with protocol buffer
     * @return Future<PreparedStatement> holds the result of the SQL service
     */
    public Future<PreparedStatement> prepare(String sql, RequestProtos.PlaceHolder placeHolder) throws IOException {
	return sessionLink.send(RequestProtos.Prepare.newBuilder()
				.setSql(sql)
				.setHostVariables(placeHolder)
				.build());
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
