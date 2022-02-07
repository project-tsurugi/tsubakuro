package com.nautilus_technologies.tsubakuro.low.tpch;

public class Profile {
    public long scales;
    public boolean readOnly;
    public boolean queryValidation;
    public long q21;
    public long q22;
    public long q6;
    public long q14;
    public long q19;

    public Profile() {
	this.readOnly = false;
	this.queryValidation = false;
	this.q21 = 0;
	this.q22 = 0;
	this.q6 = 0;
	this.q14 = 0;
	this.q19 = 0;
    }
}
