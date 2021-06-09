package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.CommonProtos;

/**
 * PreparedStatementImpl type.
 */
public class PreparedStatementImpl implements PreparedStatement {
    CommonProtos.PreparedStatement handle;
    
    public PreparedStatementImpl(CommonProtos.PreparedStatement handle) {
	this.handle = handle;
    }

    public CommonProtos.PreparedStatement getHandle() {
	return handle;
    }

    /**
     * Close the PreparedStatementImpl
     */
    public void close() throws IOException {
    }
}
