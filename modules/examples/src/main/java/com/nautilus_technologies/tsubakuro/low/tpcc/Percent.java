package com.nautilus_technologies.tsubakuro.low.tpcc;

public final class Percent {
    private Percent() {
    }

    static final long KXCT_NEWORDER_PERCENT = 45;
    static final long KXCT_PAYMENT_PERCENT = KXCT_NEWORDER_PERCENT + 43;
    static final long KXCT_ORDERSTATUS_PERCENT = KXCT_PAYMENT_PERCENT + 4;
    static final long KXCT_DELIEVERY_PERCENT = KXCT_ORDERSTATUS_PERCENT + 4;
    
    public static long kXctNewOrder() {
	return KXCT_NEWORDER_PERCENT;
    }
    public static long kXctPayment() {
	return KXCT_PAYMENT_PERCENT;
    }
    public static long kkXctOrderStatus() {
	return KXCT_ORDERSTATUS_PERCENT;
    }
    public static long kXctDelievery() {
	return KXCT_DELIEVERY_PERCENT;
    }
    
    public static long kNewOrderRemote() {
	return 10;
    }
}
