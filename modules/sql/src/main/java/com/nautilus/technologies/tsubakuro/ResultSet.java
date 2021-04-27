package com.nautilus.technologies.tsubakuro;

/**
 * ResultSet type.
 */
public interface ResultSet {
    /**
     * Describes field type
     */
    public enum FieldType {
	NULL("NULL", 0),
	INT4("INT4", 1),
	INT8("INT8", 2),
	FLOAT4("FLOAT4", 3),
	FLOAT8("FLOAT8", 4),
	STRING("STRING", 5);

	private String label;
	private int type;

	private FieldType(String label, int type) {
	    this.label = label;
	    this.type = type;
	}


	public String getLabel() {
	    return label;
	}

	public int getType() {
	    return type;
	}

    }

    /**
     * Provides record metadata holding information about field type and nullability
     */
    public interface RecordMeta {
	/**
	 * Get the field type
	 * @param index field index. Must be equal to, or greater than 0. Must be less than the field count.
	 * @return field type
	 */
	public FieldType at(int index);

	/**
	 * Get the nullability for the field
	 * @param index field index. Must be equal to, or greater than 0. Must be less than the field count.
	 * @return true if the field is nullable
	 */
	public boolean nullable(int index);
	
	/**
	 * Get the number of fields in the record
	 * @return the number of the fields
	 */
	public long fieldCount();
    }

    /**
     * Provides record object in the result set
     */
    public interface Record {
	/**
	 * Get the field values of the record
	 * @param index indicate the field offset originated at 0. This must be smaller than the field count.
	 * @return the value of given type
	 */
	public int setInt4(long index);
	public long setInt8(long index);
	public float setFloat4(long index);
	public double setFloat8(long index);
	public String setCharacter(long index);
    }

    /**
     * Provides result set iterator
     */
    public interface ResultSetIterator {
	/**
	 * Provides whether the next record exists
	 */
	public boolean hasNext();

	/**
	 * Move the iterator to the next record and return accessor to it
	 */
	public Record next();
    }

    /**
     * Get the metadata of the result records
     */
    public RecordMeta meta();

    /**
     * Get the iterator at the beginning of the result records
     */
    public ResultSetIterator iterator();
}
