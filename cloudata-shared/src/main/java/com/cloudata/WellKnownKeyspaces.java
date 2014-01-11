package com.cloudata;

public class WellKnownKeyspaces {
    public static final int SYSTEM_START = 0x7fff0000;

    public static final int KEYSPACE_DEFINITIONS = SYSTEM_START + 1;
    public static final int KEYSPACE_KEYS = SYSTEM_START + 2;

    protected static final int SYSTEM_INDEXES_START = 0x7fff1000;

    public static final int KEYSPACE_DEFINITIONS_IX_NAME = SYSTEM_INDEXES_START + 1;
    public static final int KEYSPACE_KEYS_IX_KEYSPACE_NAME = SYSTEM_INDEXES_START + 2;

}
