package com.nautilus_technologies.tsubakuro.low.sql;

import java.util.concurrent.Future;

/**
 * SessionLink type.
 */
public interface SessionLink {
    /**
     * Send prepare request to the SQL server.
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.Prepare> contains prepared statement handle
    */
    Future<ResponseProtos.Prepare> send(RequestProtos.Prepare request);

    /**
     * Send execute sql statement request to the SQL server
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    Future<ResponseProtos.ResultOnly> send(RequestProtos.ExecuteStatement request);

    /**
     * Send execute prepared statement request to the SQL server
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    Future<ResponseProtos.ResultOnly> send(RequestProtos.ExecutePreparedStatement request);

    /**
     * Send execute sql query request to the SQL server
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set link
    */
    Future<ResponseProtos.ExecuteQuery> send(RequestProtos.ExecuteQuery request);

    /**
     * Send execute prepared query request to the SQL server
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set link
    */
    Future<ResponseProtos.ExecuteQuery> send(RequestProtos.ExecutePreparedQuery request);

    /**
     * Send begin request to the SQL server.
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.Begin> contains transaction handle
    */
    Future<ResponseProtos.Begin> send(RequestProtos.Begin request);

    /**
     * Send commit request to the SQL server.
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    Future<ResponseProtos.ResultOnly> send(RequestProtos.Commit request);

    /**
     * Send rollback request to the SQL server.
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    Future<ResponseProtos.ResultOnly> send(RequestProtos.Rollback request);

    /**
     * Send disposePreparedStatement request to the SQL server.
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    Future<ResponseProtos.ResultOnly> send(RequestProtos.DisposePreparedStatement request);

    /**
     * Send Disconnect request to the SQL server.
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    Future<ResponseProtos.ResultOnly> send(RequestProtos.Disconnect request);
}
