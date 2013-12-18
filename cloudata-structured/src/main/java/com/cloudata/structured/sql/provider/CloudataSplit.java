package com.cloudata.structured.sql.provider;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Split;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

public class CloudataSplit implements Split {
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final boolean remotelyAccessible;
    private final ImmutableList<HostAddress> addresses;
    private final HostAddress hostAddress;

    @JsonCreator
    public CloudataSplit(@JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName, @JsonProperty("tableName") String tableName,
            @JsonProperty("hostAddress") HostAddress hostAddress) {
        this.schemaName = checkNotNull(schemaName, "schema name is null");
        this.connectorId = checkNotNull(connectorId, "connector id is null");
        this.tableName = checkNotNull(tableName, "table name is null");
        this.hostAddress = checkNotNull(hostAddress, "hostAddress is null");

        // if ("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme())) {
        remotelyAccessible = false;
        addresses = ImmutableList.of(hostAddress);
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
