package com.cloudata.clients.structured;

import java.io.IOException;

import com.cloudata.clients.structured.cloudata.TableInfo;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.Descriptors.FieldDescriptor;

public interface StructuredStoreSchema {

    TableInfo putTablespace(String name, DescriptorProto proto, FieldDescriptor[] primaryKey)
            throws AlreadyExistsException, IOException;

    TableInfo findTablespace(String name) throws IOException;

}
