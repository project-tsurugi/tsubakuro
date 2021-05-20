package com.nautilus_technologies.tsubakuro;

import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.Protos.RequestConnect;
import com.nautilus_technologies.tsubakuro.Protos.RequestPrepare;
import com.nautilus_technologies.tsubakuro.Protos.RequestExecuteStatement;
import com.nautilus_technologies.tsubakuro.Protos.RequestExecuteQuery;
import com.nautilus_technologies.tsubakuro.Protos.RequestExecutePreparedStatement;
import com.nautilus_technologies.tsubakuro.Protos.RequestExecutePreparedQuery;
import com.nautilus_technologies.tsubakuro.Protos.RequestBegin;
import com.nautilus_technologies.tsubakuro.Protos.RequestCommit;
import com.nautilus_technologies.tsubakuro.Protos.RequestRollback;

import com.nautilus_technologies.tsubakuro.Protos.ResponseConnect;
import com.nautilus_technologies.tsubakuro.Protos.ResponseBegin;
import com.nautilus_technologies.tsubakuro.Protos.ResponseResultOnly;
import com.nautilus_technologies.tsubakuro.Protos.ResponsePrepare;
import com.nautilus_technologies.tsubakuro.Protos.ResponseExecuteQuery;


/**
 * Link type.
 */
public interface Link {
    public interface SessionLink {
	/**
	 * Send prepare request to the SQL server.
	 @param request the request message encoded with protocol buffer
	 @return Future<ResponsePrepare>
	*/
	Future<ResponsePrepare> send(RequestPrepare request);

	/**
	 * Send execute sql statement request to the SQL server
	 @param request the request message encoded with protocol buffer
	 @return Future<ResponseExecuteQuery> indicate whether the command is processed successfully or not
	*/
	Future<ResponseResultOnly> send(RequestExecuteStatement request);

	/**
	 * Send execute prepared statement request to the SQL server
	 @param request the request message encoded with protocol buffer
	 @return Future<ResponseExecuteQuery> indicate whether the command is processed successfully or not
	*/
	Future<ResponseResultOnly> send(RequestExecutePreparedStatement request);

	/**
	 * Send execute sql query request to the SQL server
	 @param request the request message encoded with protocol buffer
	 @return Future<ResponseExecuteQuery>
	*/
	Future<ResponseExecuteQuery> send(RequestExecuteQuery request);

	/**
	 * Send execute prepared query request to the SQL server
	 @param request the request message encoded with protocol buffer
	 @return Future<ResponseExecuteQuery>
	*/
	Future<ResponseExecuteQuery> send(RequestExecutePreparedQuery request);

	/**
	 * Send begin request to the SQL server.
	 @param request the request message encoded with protocol buffer
	 @return Future<ResponseBegin>
	*/
	Future<ResponseBegin> send(RequestBegin request);

	/**
	 * Send commit request to the SQL server.
	 @param request the request message encoded with protocol buffer
	 @return Future<ResponseCommit>
	*/
	Future<ResponseResultOnly> send(RequestCommit request);

	/**
	 * Send rollback request to the SQL server.
	 @param request the request message encoded with protocol buffer
	 @return Future<ResponseRollback>
	*/
	Future<ResponseResultOnly> send(RequestRollback request);
    }

    public interface CommonLink {
	/**
	 * Send connect request to the SQL server via common link.
	 @param request the request message encoded with protocol buffer
	 @return Future<ResponseConnect>
	*/
	Future<ResponseConnect> send(RequestConnect request);
    }
}
