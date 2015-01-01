package com.cloudata.datastore.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

class BytesColumnInfo extends ColumnInfo {
  public BytesColumnInfo(FieldDescriptor[] protobufField, String sqlName) {
    super(protobufField, sqlName);
  }

  @Override
  public void mapFromDb(ResultSet rs, Message.Builder dest) throws SQLException {
    byte[] value = rs.getBytes(this.sqlColumnName);
    if (value != null) {
      setField(dest, ByteString.copyFrom(value));
    }
  }

  @Override
  public SqlColumnSpec buildColumnSpec() {
    return new SqlColumnSpec(sqlColumnName, "bytea");
  }

  @Override
  public void mapToStatement(Message src, PreparedStatement dest, int parameter) throws SQLException {
    ByteString value = (ByteString) getField(src);
    dest.setObject(parameter, value.toByteArray());
  }

}