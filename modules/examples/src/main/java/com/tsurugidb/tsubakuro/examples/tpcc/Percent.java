package com.tsurugidb.tsubakuro.examples.tpcc;

public final class Percent {
    private Percent() {
    }

    public static final long KXCT_NEWORDER_PERCENT = 45;
    public static final long KXCT_PAYMENT_PERCENT = KXCT_NEWORDER_PERCENT + 43;
    public static final long KXCT_ORDERSTATUS_PERCENT = KXCT_PAYMENT_PERCENT + 4;
    public static final long KXCT_DELIEVERY_PERCENT = KXCT_ORDERSTATUS_PERCENT + 4;

    public static final long K_NEW_ORDER_REMOTE = 10;
}
