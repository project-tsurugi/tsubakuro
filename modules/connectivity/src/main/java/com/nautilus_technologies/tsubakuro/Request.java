package com.nautilus_technologies.tsubakuro;

import java.util.concurrent.Future;

/**
 * Request type.
 */
public interface Request {
    public interface Prepare {
	void setSql(String sql);
	void setInt4(String name);
	void setInt8(String name);
	void setFloat4(String name);
	void setFloat8(String name);
	void setCharacter(String name);
	Future<LowLevelPreparedStatement> send();
    }
    public interface Execute {
	void setSql(String sql);
	Future<ErrorCode> sendStatement();
	Future<LowLevelResultSet> sendQuery();
    }
    public interface ExecutePrepared {
	void setPreparedStatement(LowLevelPreparedStatement prepared);
	void setInt4(String name, int value);
	void setInt8(String name, long value);
	void setFloat4(String name, float value);
	void setFloat8(String name, double value);
	void setCharacter(String name, String value);
	Future<ErrorCode> sendStatement();
	Future<LowLevelResultSet> sendQuery();
    }
    public interface Begin {
	void setReadOnly();
	Future<ErrorCode> send();
    }
    public interface Commit {
	Future<ErrorCode> send();
    }
    public interface Rollback {
	Future<ErrorCode> send();
    }
}
