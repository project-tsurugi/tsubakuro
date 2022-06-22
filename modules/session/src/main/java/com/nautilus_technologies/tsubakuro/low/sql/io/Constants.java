package com.nautilus_technologies.tsubakuro.low.sql.io;

/**
 * The constant values for this package.
 */
final class Constants {

    public static final int HEADER_EMBED_POSITIVE_INT = 0x00;

    public static final int HEADER_EMBED_CHARACTER = 0x40;

    public static final int HEADER_EMBED_ROW = 0x80;

    public static final int HEADER_EMBED_ARRAY = 0xa0;

    public static final int HEADER_EMBED_NEGATIVE_INT = 0xc0;

    public static final int HEADER_EMBED_OCTET = 0xd0;

    public static final int HEADER_EMBED_BIT = 0xe0;

    public static final int HEADER_UNKNOWN = 0xe8;

    public static final int HEADER_INT = 0xe9;

    public static final int HEADER_FLOAT4 = 0xea;

    public static final int HEADER_FLOAT8 = 0xeb;

    public static final int HEADER_RESERVED_EC = 0xec;

    public static final int HEADER_DECIMAL4 = 0xed;

    public static final int HEADER_DECIMAL8 = 0xee;

    public static final int HEADER_DECIMAL16 = 0xef;

    public static final int HEADER_CHARACTER = 0xf0;

    public static final int HEADER_OCTET = 0xf1;

    public static final int HEADER_BIT = 0xf2;

    public static final int HEADER_DATE = 0xf3;

    public static final int HEADER_TIME_OF_DAY = 0xf4;

    public static final int HEADER_TIME_POINT = 0xf5;

    public static final int HEADER_DATETIME_INTERVAL = 0xf6;

    public static final int HEADER_RESERVED_F7 = 0xf7;

    public static final int HEADER_ROW = 0xf8;

    public static final int HEADER_ARRAY = 0xf9;

    public static final int HEADER_CLOB = 0xfa;

    public static final int HEADER_BLOB = 0xfb;

    public static final int HEADER_RESERVED_FC = 0xfc;

    public static final int HEADER_RESERVED_FD = 0xfd;

    public static final int HEADER_END_OF_CONTENTS = 0xfe;

    public static final int HEADER_RESERVED_FF = 0xff;


    public static final int MASK_EMBED_POSITIVE_INT = 0x3f;

    public static final int MASK_EMBED_CHARACTER = 0x3f;

    public static final int MASK_EMBED_ROW = 0x1f;

    public static final int MASK_EMBED_ARRAY = 0x1f;

    public static final int MASK_EMBED_NEGATIVE_INT = 0x0f;

    public static final int MASK_EMBED_OCTET = 0x0f;

    public static final int MASK_EMBED_BIT = 0x07;


    public static final int MIN_EMBED_POSITIVE_INT_VALUE = 0x00;

    public static final int MAX_EMBED_POSITIVE_INT_VALUE = MASK_EMBED_POSITIVE_INT + MIN_EMBED_POSITIVE_INT_VALUE;


    public static final int MIN_EMBED_NEGATIVE_INT_VALUE = -(MASK_EMBED_NEGATIVE_INT + 1);

    public static final int MAX_EMBED_NEGATIVE_INT_VALUE = 0;


    public static final int MIN_EMBED_CHARACTER_SIZE = 0x01;

    public static final int MAX_EMBED_CHARACTER_SIZE = MASK_EMBED_CHARACTER + MIN_EMBED_CHARACTER_SIZE;


    public static final int MIN_EMBED_OCTET_SIZE = 0x01;

    public static final int MAX_EMBED_OCTET_SIZE = MASK_EMBED_OCTET + MIN_EMBED_OCTET_SIZE;


    public static final int MIN_EMBED_BIT_SIZE = 0x01;

    public static final int MAX_EMBED_BIT_SIZE = MASK_EMBED_BIT + MIN_EMBED_BIT_SIZE;


    public static final int MIN_EMBED_ROW_SIZE = 0x01;

    public static final int MAX_EMBED_ROW_SIZE = MASK_EMBED_ROW + MIN_EMBED_ROW_SIZE;


    public static final int MIN_EMBED_ARRAY_SIZE = 0x01;

    public static final int MAX_EMBED_ARRAY_SIZE = MASK_EMBED_ARRAY + MIN_EMBED_ARRAY_SIZE;

    private Constants() {
        throw new AssertionError();
    }
}
