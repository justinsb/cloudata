package com.cloudata.clients.structured;

import java.io.IOException;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.Descriptors.FieldDescriptor;

public interface StructuredStoreSchema {

    int putTablespace(String name, DescriptorProto proto, FieldDescriptor[] primaryKey) throws AlreadyExistsException,
            IOException;

    Integer findTablespace(String name) throws IOException;

}
