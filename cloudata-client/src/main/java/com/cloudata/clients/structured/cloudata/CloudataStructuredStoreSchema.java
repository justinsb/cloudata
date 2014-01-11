package com.cloudata.clients.structured.cloudata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.clients.structured.AlreadyExistsException;
import com.cloudata.clients.structured.StructuredStore;
import com.cloudata.clients.structured.StructuredStoreSchema;
import com.cloudata.structured.StructuredSchema;
import com.cloudata.structured.StructuredSchema.SchemaData;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;

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
    public int putTablespace(String name, DescriptorProto proto, FieldDescriptor[] primaryKey)
            throws AlreadyExistsException, IOException {
        Preconditions.checkState(primaryKey != null && primaryKey.length > 0);

        ByteString key = ByteString.copyFromUtf8(name);

        while (true) {
            int tablespaceId = 1 + random.nextInt(StructuredStore.SYSTEM_TABLESPACE_START - 1);

            Preconditions.checkState(tablespaceId > 0);
            Preconditions.checkState(tablespaceId < StructuredStore.SYSTEM_TABLESPACE_START);

            ByteString schemaData;
            {
                SchemaData.Builder b = SchemaData.newBuilder();
                b.setName(ByteString.copyFromUtf8(name));
                b.setTablespaceId(tablespaceId);
                b.setProtobufSchema(proto.toByteString());
                for (FieldDescriptor field : primaryKey) {
                    b.addPrimaryKeyFields(field.getNumber());
                }
                schemaData = b.build().toByteString();
            }

            ByteString tablespaceIdKey;
            {
                ByteBuffer b = ByteBuffer.allocate(4);
                b.putInt(tablespaceId);
                b.flip();

                tablespaceIdKey = ByteString.copyFrom(b);
            }

            // TODO: Need batch operation here!!
            try {
                store.create(StructuredStore.TABLESPACEID_TABLESPACE_ID_INDEX, tablespaceIdKey, key);
            } catch (AlreadyExistsException e) {
                log.debug("Retrying after tablespace id conflict", e);
                continue;
            }

            boolean rollback = true;
            try {
                store.create(StructuredStore.TABLESPACEID_TABLESPACES, key, schemaData);
                rollback = false;
            } finally {
                if (rollback) {
                    try {
                        store.create(StructuredStore.TABLESPACEID_TABLESPACE_ID_INDEX, tablespaceIdKey, key);
                    } catch (Exception e2) {
                        log.warn("Error while deleting tablespace id entry", e2);
                    }
                }
            }
        }
    }

    @Override
    public Integer findTablespace(String name) throws IOException {
        ByteString key = ByteString.copyFromUtf8(name);

        StructuredStore.Entry entry = store.read(StructuredStore.TABLESPACEID_TABLESPACES, key);
        if (entry == null) {
            return null;
        }
        ByteString data = entry.getData();
        try {
            SchemaData schemaData = StructuredSchema.SchemaData.parseFrom(data);
            return schemaData.getTablespaceId();
        } catch (InvalidProtocolBufferException e) {
            throw new IOException("Schema data is corrupted", e);
        }
    }

}