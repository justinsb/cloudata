package com.cloudata.clients.structured.cloudata;

import java.io.IOException;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.WellKnownKeyspaces;
import com.cloudata.clients.structured.AlreadyExistsException;
import com.cloudata.clients.structured.StructuredStore;
import com.cloudata.clients.structured.StructuredStoreSchema;
import com.cloudata.clients.structured.VersionMismatchException;
import com.cloudata.structured.StructuredProtocol.KeyspaceData;
import com.cloudata.structured.StructuredProtocol.KeyspaceName;
import com.cloudata.structured.StructuredProtocol.KeyspaceType;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * Manages schemas & tablespaces. For now, this is client side. TODO: Move to server!
 * 
 */
public class CloudataStructuredStoreSchema implements StructuredStoreSchema {

    private static final Logger log = LoggerFactory.getLogger(CloudataStructuredStoreSchema.class);

    final CloudataStructuredStore store;

    static final Random random = new Random();

    public CloudataStructuredStoreSchema(CloudataStructuredStore store) {
        this.store = store;
    }

    @Override
    public TableInfo putTablespace(String name, DescriptorProto proto, FieldDescriptor[] primaryKey)
            throws AlreadyExistsException, IOException {
        Preconditions.checkState(primaryKey != null && primaryKey.length > 0);

        KeyspaceName keyspaceName = buildKeyspaceName(name);
        KeyspaceData keyspaceData;
        {
            KeyspaceData.Builder b = KeyspaceData.newBuilder();
            b.setName(keyspaceName);
            b.setProtobufSchema(proto.toByteString());
            for (FieldDescriptor field : primaryKey) {
                b.addPrimaryKeyFields(field.getNumber());
            }
            keyspaceData = b.build();
        }

        StructuredStore.Entry entry;

        try {
            entry = store.put(WellKnownKeyspaces.KEYSPACE_DEFINITIONS, null, keyspaceData.toByteString(), true, null);
        } catch (VersionMismatchException e) {
            throw new IOException("Protocol error (unexpected version-mismatch response)", e);
        }

        keyspaceData = KeyspaceData.parseFrom(entry.getData());

        if (!keyspaceData.hasId()) {
            throw new IllegalStateException();
        }

        return new TableInfo(keyspaceData);
    }

    private KeyspaceName buildKeyspaceName(String name) {
        KeyspaceName keyspaceName;
        {
            KeyspaceName.Builder b = KeyspaceName.newBuilder();
            b.setType(KeyspaceType.USER_DATA);
            b.setName(ByteString.copyFromUtf8(name));
            keyspaceName = b.build();
        }
        return keyspaceName;
    }

    @Override
    public TableInfo findTablespace(String name) throws IOException {
        KeyspaceName keyspaceName = buildKeyspaceName(name);

        // TODO: Server side dereference / query

        StructuredStore.Entry indexEntry = store.read(WellKnownKeyspaces.KEYSPACE_DEFINITIONS_IX_NAME,
                keyspaceName.toByteString());
        if (indexEntry == null) {
            return null;
        }

        StructuredStore.Entry entry = store.read(WellKnownKeyspaces.KEYSPACE_DEFINITIONS, indexEntry.getData());
        if (entry == null) {
            throw new IllegalStateException();
        }

        KeyspaceData keyspaceData = KeyspaceData.parseFrom(entry.getData());
        return new TableInfo(keyspaceData);
    }

}