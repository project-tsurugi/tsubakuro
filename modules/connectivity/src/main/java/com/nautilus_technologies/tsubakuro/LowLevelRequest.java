package com.nautilus_technologies.tsubakuro;

import java.util.concurrent.Future;

/**
 * LowLevelRequest type.
 */
public interface LowLevelRequest {
    /**
     * Prepare request type.
     */
    public interface Prepare {
	/**
	 * Set sql for prepare request
	 @param sql the sql text
	*/
	void setSql(String sql);

	/**
	 * Set name and type of the place holder
	 @param name the name of the place holder
	 @param type the type of the place holder
	*/
	void setPlaceHolder(String name, FieldType type);

	/**
	 * Send prepare request to the SQL server.
	 @return Future<LowLevelPreparedStatement>
	 */
	Future<LowLevelPreparedStatement> send();
    }

    /**
     * Execute sql statement and/or query request type.
     */
    public interface Execute {
	/**
	 * Set sql for execute statement or query request
	 @param sql the sql text
	*/
	void setSql(String sql);

	/**
	 * Send execute statement request to the SQL server.
	 @return Future<ErrorCode>
	 */
	Future<ErrorCode> sendStatement();

	/**
	 * Send execute query request to the SQL server.
	 @return Future<LowLevelPreparedStatement>
	 */
	Future<LowLevelResultSet> sendQuery();
    }

    /**
     * Execute prepared statement and/or query request type.
     */
    public interface ExecutePrepared {
	/**
	 * Set prepared sql for execute prepared statement or query request
	 @param sql the sql text
	*/
	void setPreparedStatement(LowLevelPreparedStatement prepared);

	/**
	 * Set a value for the placeholder
	 * @param name the name of the placeholder without colon
	 * @param value the value assigned to the placeholder
	 */
	void setInt4(String name, int value);
	void setInt8(String name, long value);
	void setFloat4(String name, float value);
	void setFloat8(String name, double value);
	void setCharacter(String name, String value);

	/**
	 * Send execute prepared statement request to the SQL server.
	 @return Future<ErrorCode>
	 */
	Future<ErrorCode> sendStatement();

	/**
	 * Send execute prepared query request to the SQL server.
	 @return Future<LowLevelPreparedStatement>
	 */
	Future<LowLevelResultSet> sendQuery();
    }

    /**
     * Begin request type.
     */
    public interface Begin {
	/**
	 * Changet the new transaction from read-write, default of the request, to read-only
	 */
	void setReadOnly();

	/**
	 * Send begin request to the SQL server.
	 @return Future<ErrorCode>
	 */
	Future<ErrorCode> send();
    }

    /**
     * Commit request type.
     */
    public interface Commit {
	/**
	 * Send commit request to the SQL server.
	 @return Future<ErrorCode>
	 */
	Future<ErrorCode> send();
    }

    /**
     * Rollback request type.
     */
    public interface Rollback {
	/**
	 * Send rollback request to the SQL server.
	 @return Future<ErrorCode>
	 */
	Future<ErrorCode> send();
    }
}
