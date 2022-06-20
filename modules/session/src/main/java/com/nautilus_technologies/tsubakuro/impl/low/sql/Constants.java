package com.nautilus_technologies.tsubakuro.impl.low.sql;

import com.tsurugidb.jogasaki.proto.SqlRequest;

/**
 * The constant values.
 */
final class Constants {

    /**
     * The service ID of SQL service ({@value}).
     */
    public static final int SERVICE_ID_SQL = 3;

    /**
     * The channel name of metadata of result set.
     */
    public static final String CHANNEL_NAME_RESULT_SET_METADATA = "metadata"; //$NON-NLS-1$

    /**
     * The channel name of metadata of result set.
     */
    public static final String CHANNEL_NAME_RESULT_SET_RELATION = "relation"; //$NON-NLS-1$

    private Constants() {
        throw new AssertionError();
    }
}
