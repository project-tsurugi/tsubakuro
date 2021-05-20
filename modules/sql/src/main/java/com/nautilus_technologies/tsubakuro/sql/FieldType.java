package com.nautilus_technologies.tsubakuro.sql;

/**
 * Describes field type
 */
public enum FieldType {
    INT4("INT4", 1),
    INT8("INT8", 2),
    FLOAT4("FLOAT4", 3),
    FLOAT8("FLOAT8", 4),
    STRING("STRING", 5);

    private String label;
    private int type;

    FieldType(String label, int type) {
	this.label = label;
	this.type = type;
    }

    String getLabel() {
	return label;
    }

    int getType() {
	return type;
    }
}
