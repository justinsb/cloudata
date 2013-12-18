package com.cloudata.structured.sql;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URI;
import java.util.List;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Split;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

public class MockSplit implements Split {
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final URI uri;
    private final boolean remotelyAccessible;
    private final ImmutableList<HostAddress> addresses;

    @JsonCreator
    public MockSplit(@JsonProperty("connectorId") String connectorId, @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName, @JsonProperty("uri") URI uri) {
        this.schemaName = checkNotNull(schemaName, "schema name is null");
        this.connectorId = checkNotNull(connectorId, "connector id is null");
        this.tableName = checkNotNull(tableName, "table name is null");
        this.uri = checkNotNull(uri, "uri is null");

        // if ("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme())) {
        remotelyAccessible = true;
        addresses = ImmutableList.of(HostAddress.fromUri(uri));
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    // @JsonProperty
    // public URI getUri()
    // {
    // return uri;
    // }

    @Override
    public boolean isRemotelyAccessible() {
        // only http or https is remotely accessible
        return remotelyAccessible;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return addresses;
    }

    @Override
    public Object getInfo() {
        return this;
    }
}
