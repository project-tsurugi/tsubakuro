package com.nautilus.technologies.tsubakuro;

/**
 * Link type.
 */
public interface Link {
    /**
     * InWire type.
     */
    public interface InWire {
	/**
	 * Receive data encoded as a byte array from the SQL server.
	 @return data byte array received from the SQL server
	 */
	byte[] recv();
    }
    /**
     * OutWire type.
     */
    public interface OutWire {
	/**
	 * Send data encoded as a byte array to the SQL server.
	 @param data byte array to be sent to the SQL server
	 */
	void send(byte[] data);
    }
}
