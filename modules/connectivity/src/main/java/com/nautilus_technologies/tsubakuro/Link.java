package com.nautilus_technologies.tsubakuro;

import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.RequestProtos.ProtoRequestPrepare;
import com.nautilus_technologies.tsubakuro.RequestProtos.ProtoRequestExecuteStatement;
import com.nautilus_technologies.tsubakuro.RequestProtos.ProtoRequestExecuteQuery;
import com.nautilus_technologies.tsubakuro.RequestProtos.ProtoRequestExecutePreparedStatement;
import com.nautilus_technologies.tsubakuro.RequestProtos.ProtoRequestExecutePreparedQuery;
import com.nautilus_technologies.tsubakuro.RequestProtos.ProtoRequestBegin;
import com.nautilus_technologies.tsubakuro.RequestProtos.ProtoRequestCommit;
import com.nautilus_technologies.tsubakuro.RequestProtos.ProtoRequestRollback;

import com.nautilus_technologies.tsubakuro.ResponseProtos.ProtoResponseResultOnly;
import com.nautilus_technologies.tsubakuro.ResponseProtos.ProtoResponsePrepare;
import com.nautilus_technologies.tsubakuro.ResponseProtos.ProtoResponseExecuteQuery;


/**
 * Link type.
 */
public interface Link {
    /**
     * Send prepare request to the SQL server.
     @param request the request message encoded with protocol buffer
     @param hostVariables the set of host valiable definition encoded with protocol buffer
     @return Future<ProtoResponsePrepare>
    */
    Future<ProtoResponsePrepare> send(ProtoRequestPrepare request);

    /**
     * Send execute sql statement request to the SQL server
     @param request the request message encoded with protocol buffer
     @return Future<ProtoResponseExecuteQuery> indicate whether the command is processed successfully or not
    */
    Future<ProtoResponseResultOnly> send(ProtoRequestExecuteStatement request);

    /**
     * Send execute prepared statement request to the SQL server
     @param request the request message encoded with protocol buffer
     @param parameterSet the parameter set encoded with protocol buffer
     @return Future<ProtoResponseExecuteQuery> indicate whether the command is processed successfully or not
    */
    Future<ProtoResponseResultOnly> send(ProtoRequestExecutePreparedStatement request);

    /**
     * Send execute sql query request to the SQL server
     @param request the request message encoded with protocol buffer
     @return Future<ProtoResponseExecuteQuery>
    */
    Future<ProtoResponseExecuteQuery> send(ProtoRequestExecuteQuery request);

    /**
     * Send execute prepared query request to the SQL server
     @param request the request message encoded with protocol buffer
     @param parameterSet the parameter set encoded with protocol buffer
     @return Future<ProtoResponseExecuteQuery>
    */
    Future<ProtoResponseExecuteQuery> send(ProtoRequestExecutePreparedQuery request);
}
