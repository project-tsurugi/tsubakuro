package com.nautilus_technologies.tsubakuro.low.sql;

import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.tsurugidb.jogasaki.proto.SqlResponse;

/**
 * Represents a server side SQL result set.
 */
public interface ReturnsProcessingResult {

    /**
     * Get a FutureResponse of the response returned from the SQL service
     * @return a FutureResponse of SqlResponse.ResultOnly indicate whether the SQL service has successfully completed processing or not
     */
    FutureResponse<SqlResponse.ResultOnly> getResponse();
}
