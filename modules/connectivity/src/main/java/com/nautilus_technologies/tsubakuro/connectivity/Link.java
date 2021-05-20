package com.nautilus_technologies.tsubakuro.connectivity;

import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.connectivity.Protos.RequestConnect;
import com.nautilus_technologies.tsubakuro.connectivity.Protos.RequestPrepare;
import com.nautilus_technologies.tsubakuro.connectivity.Protos.RequestExecuteStatement;
import com.nautilus_technologies.tsubakuro.connectivity.Protos.RequestExecuteQuery;
import com.nautilus_technologies.tsubakuro.connectivity.Protos.RequestExecutePreparedStatement;
import com.nautilus_technologies.tsubakuro.connectivity.Protos.RequestExecutePreparedQuery;
import com.nautilus_technologies.tsubakuro.connectivity.Protos.RequestBegin;
import com.nautilus_technologies.tsubakuro.connectivity.Protos.RequestCommit;
import com.nautilus_technologies.tsubakuro.connectivity.Protos.RequestRollback;

import com.nautilus_technologies.tsubakuro.connectivity.Protos.ResponseConnect;
import com.nautilus_technologies.tsubakuro.connectivity.Protos.ResponseBegin;
import com.nautilus_technologies.tsubakuro.connectivity.Protos.ResponseResultOnly;
import com.nautilus_technologies.tsubakuro.connectivity.Protos.ResponsePrepare;
import com.nautilus_technologies.tsubakuro.connectivity.Protos.ResponseExecuteQuery;


/**
 * Link type.
 */
public interface Link {
    public interface SessionLink {
	/**
	 * Send prepare request to the SQL server.
	 @param request the request message encoded with protocol buffer
	 @return Future<ResponsePrepare> contains prepared statement handle
	*/
	Future<ResponsePrepare> send(RequestPrepare request);

	/**
	 * Send execute sql statement request to the SQL server
	 @param request the request message encoded with protocol buffer
	 @return Future<ResponseResultOnly> indicate whether the command is processed successfully or not
	*/
	Future<ResponseResultOnly> send(RequestExecuteStatement request);

	/**
	 * Send execute prepared statement request to the SQL server
	 @param request the request message encoded with protocol buffer
	 @return Future<ResponseResultOnly> indicate whether the command is processed successfully or not
	*/
	Future<ResponseResultOnly> send(RequestExecutePreparedStatement request);

	/**
	 * Send execute sql query request to the SQL server
	 @param request the request message encoded with protocol buffer
	 @return Future<ResponseExecuteQuery> contains the name of result set link
	*/
	Future<ResponseExecuteQuery> send(RequestExecuteQuery request);

	/**
	 * Send execute prepared query request to the SQL server
	 @param request the request message encoded with protocol buffer
	 @return Future<ResponseExecuteQuery> contains the name of result set link
	*/
	Future<ResponseExecuteQuery> send(RequestExecutePreparedQuery request);

	/**
	 * Send begin request to the SQL server.
	 @param request the request message encoded with protocol buffer
	 @return Future<ResponseBegin> contains transaction handle
	*/
	Future<ResponseBegin> send(RequestBegin request);

	/**
	 * Send commit request to the SQL server.
	 @param request the request message encoded with protocol buffer
	 @return Future<ResponseResultOnly> indicate whether the command is processed successfully or not
	*/
	Future<ResponseResultOnly> send(RequestCommit request);

	/**
	 * Send rollback request to the SQL server.
	 @param request the request message encoded with protocol buffer
	 @return Future<ResponseResultOnly> indicate whether the command is processed successfully or not
	*/
	Future<ResponseResultOnly> send(RequestRollback request);
    }

    public interface CommonLink {
	/**
	 * Send connect request to the SQL server via common link.
	 @param request the request message encoded with protocol buffer
	 @return Future<ResponseConnect> contains session handle
	*/
	Future<ResponseConnect> send(RequestConnect request);
    }
}
