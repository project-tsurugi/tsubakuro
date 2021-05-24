package com.nautilus_technologies.tsubakuro.low.sql;

import java.util.concurrent.Future;
import java.io.IOException;

/**
 * SessionLink type.
 */
public interface SessionLink {
    /**
     * Send prepare request to the SQL server.
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.Prepare> contains prepared statement handle
    */
    Future<ResponseProtos.Prepare> send(RequestProtos.Prepare request) throws IOException;

    /**
     * Send execute sql statement request to the SQL server
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    Future<ResponseProtos.ResultOnly> send(RequestProtos.ExecuteStatement request) throws IOException;

    /**
     * Send execute prepared statement request to the SQL server
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    Future<ResponseProtos.ResultOnly> send(RequestProtos.ExecutePreparedStatement request) throws IOException;

    /**
     * Send execute sql query request to the SQL server
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set link
    */
    Future<ResponseProtos.ExecuteQuery> send(RequestProtos.ExecuteQuery request) throws IOException;

    /**
     * Send execute prepared query request to the SQL server
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ExecuteQuery> contains the name of result set link
    */
    Future<ResponseProtos.ExecuteQuery> send(RequestProtos.ExecutePreparedQuery request) throws IOException;

    /**
     * Send begin request to the SQL server.
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.Begin> contains transaction handle
    */
    Future<ResponseProtos.Begin> send(RequestProtos.Begin request) throws IOException;

    /**
     * Send commit request to the SQL server.
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    Future<ResponseProtos.ResultOnly> send(RequestProtos.Commit request) throws IOException;

    /**
     * Send rollback request to the SQL server.
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    Future<ResponseProtos.ResultOnly> send(RequestProtos.Rollback request) throws IOException;

    /**
     * Send disposePreparedStatement request to the SQL server.
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    Future<ResponseProtos.ResultOnly> send(RequestProtos.DisposePreparedStatement request) throws IOException;

    /**
     * Send Disconnect request to the SQL server.
     @param request the request message encoded with protocol buffer
     @return Future<ResponseProtos.ResultOnly> indicate whether the command is processed successfully or not
    */
    Future<ResponseProtos.ResultOnly> send(RequestProtos.Disconnect request) throws IOException;
}
