package com.cloudata.clients.structured.cloudata;

import com.cloudata.structured.StructuredProtocol.KeyspaceData;

public class TableInfo {

    private final KeyspaceData keyspaceData;

    public TableInfo(KeyspaceData keyspaceData) {
        this.keyspaceData = keyspaceData;
    }

    public int getKeyspaceId() {
        return keyspaceData.getId();
    }

}
