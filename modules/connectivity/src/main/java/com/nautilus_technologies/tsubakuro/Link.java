package com.nautilus_technologies.tsubakuro;

import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.LowLevelPreparedStatement;
import com.nautilus_technologies.tsubakuro.LowLevelResultSet;
import com.nautilus_technologies.tsubakuro.RequestProtos.ProtoBufRequest;
import com.nautilus_technologies.tsubakuro.RequestProtos.ProtoBufHostVariables;
import com.nautilus_technologies.tsubakuro.RequestProtos.ProtoBufParameterSet;

/**
 * Link type.
 */
public interface Link {
    /**
     * Send prepare request to the SQL server.
     @param request the request message encoded with protocol buffer
     @param hostVariables the set of host valiable definition encoded with protocol buffer
     @return Future<LowLevelPreparedStatemet>
    */
    Future<LowLevelPreparedStatement> sendPrepare(ProtoBufRequest request, ProtoBufHostVariables hostVariables);

    /**
     * Send execute sql statement request to the SQL server
     @param request the request message encoded with protocol buffer
     @return Future<ErrorCode> indicate whether the command is processed successfully or not
    */
    Future<ErrorCode> sendStatement(ProtoBufRequest request);

    /**
     * Send execute prepared statement request to the SQL server
     @param request the request message encoded with protocol buffer
     @param parameterSet the parameter set encoded with protocol buffer
     @return Future<ErrorCode> indicate whether the command is processed successfully or not
    */
    Future<ErrorCode> sendStatement(ProtoBufRequest request, ProtoBufParameterSet parameterSet);

    /**
     * Send execute sql query request to the SQL server
     @param request the request message encoded with protocol buffer
     @return Future<LowLevelPreparedStatemet>
    */
    Future<LowLevelResultSet> sendQuery(ProtoBufRequest request);

    /**
     * Send execute prepared query request to the SQL server
     @param request the request message encoded with protocol buffer
     @param parameterSet the parameter set encoded with protocol buffer
     @return Future<LowLevelPreparedStatemet>
    */
    Future<LowLevelResultSet> sendQuery(ProtoBufRequest request, ProtoBufParameterSet parameterSet);
}
